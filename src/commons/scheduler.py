#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Seagate Technology LLC and/or its Affiliates
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#
#

"""Scheduler module."""

import asyncio
import datetime
import logging
import os

import schedule
from schedule import Job

from src.commons.constants import ROOT
from src.commons.report import log_status

LOGGER = logging.getLogger(ROOT)


async def create_session(funct: list, start_time: float, **kwargs) -> None:
    """
    Execute the test function in sessions.

    :param funct: List of class name and method name to be called.
    :param start_time: Start time for session.
    :param kwargs: session name from each test and test parameters.
    """
    await asyncio.sleep(start_time)
    active_session = kwargs.pop("session", None)
    LOGGER.info("Starting Session %s, PID - %s", active_session, os.getpid())
    LOGGER.info("kwargs : %s", kwargs)
    func = getattr(funct[0](**kwargs), funct[1])
    await func()
    LOGGER.info("Ended Session %s, PID - %s", active_session, os.getpid())


# pylint: disable=too-many-branches
async def schedule_sessions(test_plan: str, test_plan_value: dict, common_params: dict) -> None:
    """
    Create and Schedule specified number of sessions for each test in test_plan.
    :param test_plan: YAML file name for specific S3 operation
    :param test_plan_value: Parsed test_plan values
    :param common_params: Common arguments to be sent to function
    """
    process_name = f"Test [Process {os.getpid()}, test_num {test_plan}]"
    tasks = []
    for _, each in test_plan_value.items():
        test_id = each.pop("TEST_ID")
        start_time = each.pop("start_time")
        params = {"test_id": test_id, "object_size": each["object_size"]}
        if "part_range" in each.keys():
            params["part_range"] = each["part_range"]
        if "range_read" in each.keys():
            params["range_read"] = each["range_read"]
        if "part_copy" in each.keys():
            params["part_copy"] = each["part_copy"]
        if common_params.get("sequential_run"):
            LOGGER.info("Sequential execution is enabled for workload: %s.", test_plan)
            min_runtime = each.get("min_runtime", 0)
            if not min_runtime:
                raise AssertionError(f"Minimum run time is not defined for sequential run. {each}")
            params["duration"] = min_runtime
        params.update(common_params)
        params.update(each)
        if each["tool"] == "s3api":
            if "TestTypeXObjectOps" in str(each.get("operation")[0]):
                params["session"] = f"{test_id}_session_main"
                tasks.append(create_session(funct=each["operation"],
                                            start_time=start_time.total_seconds(),
                                            **params))
            else:
                for i in range(1, int(each["sessions"]) + 1):
                    params["session"] = f"{test_id}_session{i}"
                    tasks.append(create_session(funct=each["operation"],
                                                start_time=start_time.total_seconds(),
                                                **params))
        elif each["tool"] == "s3bench":
            tasks.append(create_session(funct=each["operation"],
                                        start_time=start_time.total_seconds(), **params))
        else:
            raise NotImplementedError(f"Tool is not supported: {each['tool']}")
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    if pending:
        LOGGER.critical("Terminating process: %s & pending tasks: %s", process_name, pending)
        for task in pending:
            task.cancel()
    if done:
        LOGGER.info("Completed tasks: %s", done)
        for task in done:
            task.result()


def schedule_test_plan(test_plan: str, test_plan_values: dict, common_params: dict) -> None:
    """
    Create event loop for each test plan.

    :param test_plan: YAML file name for specific S3 operation.
    :param test_plan_values: Parsed yaml file values.
    :param common_params: Common arguments to be passed to function.
    """
    process_name = f"TestPlan [Process {os.getpid()}, topic {test_plan}]"
    LOGGER.info("%s Started ", process_name)
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(schedule_sessions(test_plan, test_plan_values, common_params))
    except KeyboardInterrupt:
        LOGGER.critical("%s Loop interrupted", process_name)
    finally:
        loop.stop()
        LOGGER.warning("%s terminated", process_name)


def schedule_test_status_update(parsed_input: dict, corio_start_time: datetime,
                                periodic_time: int = 1, sequential_run=False) -> Job:
    """
    Schedule the test status update.

    :param parsed_input: Dict for all the input yaml files.
    :param corio_start_time: Start time for main process.
    :param periodic_time: Duration to update test status.
    :param sequential_run: Execute tests sequentially.
    """
    sched_job = schedule.every(periodic_time).minutes.do(log_status, parsed_input=parsed_input,
                                                         corio_start_time=corio_start_time,
                                                         test_failed=None,
                                                         sequential_run=sequential_run)
    LOGGER.info("Report status update scheduled for every %s minutes", periodic_time)
    sched_job.run()
    return sched_job


def terminate_update_test_status(parsed_input: dict, corio_start_time: datetime,
                                 terminated_tp: str, test_ids: list, sched_job: Job) -> None:
    """
    Terminate the scheduler and update the test status.

    :param parsed_input: Dict for all the input yaml files.
    :param corio_start_time: Start time for main process.
    :param terminated_tp: Reason for failure is any.
    :param test_ids: Terminated tests from workload.
    :param sched_job: scheduled test status update job.
    """
    schedule.cancel_job(sched_job)
    log_status(parsed_input, corio_start_time, test_failed=terminated_tp, terminated_tests=test_ids)
