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
import logging
import os

from src.commons.constants import ROOT

LOGGER = logging.getLogger(ROOT)


async def create_session(funct: list, start_time: float, **kwargs) -> None:
    """
    Execute the test function in sessions.

    :param funct: List of class name and method name to be called.
    :param start_time: Start time for session.
    :param kwargs: session name from each test and test parameters.
    """
    await asyncio.sleep(start_time)
    LOGGER.info("Starting Session %s, PID - %s", kwargs.get("session"), os.getpid())
    LOGGER.info("kwargs : %s", kwargs)
    func = getattr(funct[0](**kwargs), funct[1])
    await func()
    LOGGER.info("Ended Session %s, PID - %s", kwargs.get("session"), os.getpid())


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
        params = {"test_id": each["TEST_ID"], "object_size": each["object_size"]}
        if "part_range" in each.keys():
            params["part_range"] = each["part_range"]
        if "range_read" in each.keys():
            params["range_read"] = each["range_read"]
        if "part_copy" in each.keys():
            params["part_copy"] = each["part_copy"]
        params.update(common_params)
        for i in range(1, int(each["sessions"]) + 1):
            params["session"] = f"{each['TEST_ID']}_session{i}"
            tasks.append(create_session(funct=each["operation"],
                                        start_time=each["start_time"].total_seconds(), **params))
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    LOGGER.critical("Terminating process & pending task: %s", process_name)
    for task in pending:
        task.cancel()
    LOGGER.error(done)
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
        LOGGER.info("%s Loop interrupted", process_name)
        loop.stop()
    LOGGER.info("%s terminating", process_name)


def terminate_processes(process_list):
    """
    Terminate Process on failure.

    :param process_list: Terminate the given list of process
    """
    LOGGER.debug("Process lists to terminate: %s", process_list)
    for process in process_list:
        process.terminate()
        process.join()
