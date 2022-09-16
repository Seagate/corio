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
import multiprocessing
import os
from copy import deepcopy

import munch
import schedule
from schedule import Job

from config import MASTER_CFG, S3_CFG, CORIO_CFG
from src.commons import cluster_health
from src.commons import degrade_cluster
from src.commons import support_bundle
from src.commons.constants import ROOT
from src.commons.exception import DegradedModeError
from src.commons.exception import HealthCheckError
from src.commons.report import log_status
from src.commons.utils._asyncio import (
    run_event_loop_until_complete,
    schedule_tasks,
)
from src.commons.utils.utility import (
    run_local_cmd,
    get_s3_keys,
    set_s3_access_secret_key,
)

LOGGER = logging.getLogger(ROOT)


async def create_session(funct: list, start_time: float, **kwargs: dict) -> tuple:
    """
    Execute the test function in sessions.

    :param funct: List of class name and method name to be called.
    :param start_time: Start time for session.
    :param kwargs: parameters of the tests.
    """
    await asyncio.sleep(start_time)
    session = kwargs.get("session")
    LOGGER.info("Starting Session %s, PID - %s", session, os.getpid())
    LOGGER.info("kwargs : %s", kwargs)
    func = getattr(funct[0](**kwargs), funct[1])
    resp = await func()
    LOGGER.info(resp)
    LOGGER.info("Ended Session %s, PID - %s", session, os.getpid())
    return resp


async def schedule_sessions(test_plan: str, test_plan_value: dict, common_params: dict) -> None:
    """
    Create and Schedule specified number of sessions for each test in test_plan.

    :param test_plan: YAML file name for specific S3 operation
    :param test_plan_value: Parsed test_plan values
    :param common_params: Common arguments to be sent to function
    """
    process_name = f"Test [Process {os.getpid()}, test_num {test_plan}]"
    tasks = []
    if common_params.get("sequential_run", False):
        LOGGER.info("Sequential execution is enabled for workload: %s.", test_plan)
    else:
        LOGGER.info("Incremental execution is enabled for workload: %s.", test_plan)
    access_secret_keys = common_params.pop("access_secret_keys")
    for _, each in test_plan_value.items():
        iter_keys = iter(access_secret_keys.items())
        params = deepcopy(each)
        params["test_id"] = params.pop("TEST_ID")
        test_start_time = params.pop("start_time").total_seconds()
        if common_params.get("sequential_run", False):
            params["duration"] = params.get("min_runtime", 0)
        params.update(common_params)
        if params["tool"] == "s3api":
            operation = str(params.get("operation")[0])
            if "TestTypeX" in operation or "TestType5" in operation:
                params["session"] = f"{params['test_id']}_session_main"
                iter_keys = set_s3_access_secret_key(access_secret_keys, iter_keys, params)
                tasks.append(
                    create_session(funct=params["operation"], start_time=test_start_time, **params)
                )
            else:
                for i in range(1, int(params["sessions"]) + 1):
                    params["session"] = f"{params['test_id']}_session{i}"
                    iter_keys = set_s3_access_secret_key(access_secret_keys, iter_keys, params)
                    tasks.append(
                        create_session(
                            funct=params["operation"],
                            start_time=test_start_time,
                            **params,
                        )
                    )
        elif params["tool"] == "s3bench":
            params["session"] = f"{params['test_id']}_session_s3bench"
            iter_keys = set_s3_access_secret_key(access_secret_keys, iter_keys, params)
            tasks.append(
                create_session(funct=params["operation"], start_time=test_start_time, **params)
            )
        else:
            raise NotImplementedError(f"Tool is not supported: {params['tool']}")
        LOGGER.debug(iter_keys)
    await schedule_tasks(LOGGER, tasks)
    LOGGER.info("Execution completed for process: %s", process_name)


def schedule_test_plan(test_plan: str, test_plan_values: dict, common_params: dict) -> None:
    """
    Create event loop for each test plan.

    :param test_plan: YAML file name for specific S3 operation.
    :param test_plan_values: Parsed yaml file values.
    :param common_params: Common arguments to be passed to function.
    """
    process_name = f"TestPlan: Process {os.getpid()}, topic {test_plan}"
    LOGGER.info("%s Started ", process_name)
    run_event_loop_until_complete(
        LOGGER, schedule_sessions, test_plan, test_plan_values, common_params
    )
    LOGGER.info("%s completed successfully", process_name)


def schedule_test_status_update(
    parsed_input: dict, corio_start_time: datetime, periodic_time: int = 1, **kwargs
) -> Job:
    """
    Schedule the test status update.

    :param parsed_input: Dict for all the input yaml files.
    :param corio_start_time: Start time for main process.
    :param periodic_time: Duration to update test status.
    """
    sched_job = schedule.every(periodic_time).minutes.do(
        log_status,
        parsed_input=parsed_input,
        corio_start_time=corio_start_time,
        **kwargs,
    )
    LOGGER.info("Report status update scheduled for every %s minutes", periodic_time)
    sched_job.run()
    return sched_job


def terminate_update_test_status(
    parsed_input: dict,
    corio_start_time: datetime,
    terminated_tp: str,
    test_ids: list,
    sched_job: Job,
    **kwargs,
) -> None:
    """
    Terminate the scheduler and update the test status.

    :param parsed_input: Dict for all the input yaml files.
    :param corio_start_time: Start time for main process.
    :param terminated_tp: Reason for failure is any.
    :param test_ids: Terminated tests from workload.
    :param sched_job: scheduled test status update job.
    :keyword sequential_run: Execute tests sequentially.
    """
    schedule.cancel_job(sched_job)
    log_status(
        parsed_input,
        corio_start_time,
        test_failed=terminated_tp,
        terminated_tests=test_ids,
        **kwargs,
    )


def monitor_processes(processes: dict, return_dict) -> str or None:
    """Monitor the process."""
    skip_process = []
    for tp_key, process in processes.items():
        if not process.is_alive():
            if tp_key == "support_bundle":
                LOGGER.critical(
                    "Process with PID %s stopped Support bundle collection error.",
                    process.pid,
                )
                skip_process.append(tp_key)
                continue
            if tp_key == "health_check":
                raise HealthCheckError(
                    f"Process with PID {process.pid} stopped. Health Check collection error."
                )
            if os.path.exists(os.getenv("log_path")):
                resp = run_local_cmd(
                    f"grep 'topic {tp_key} completed successfully' {os.getenv('log_path')} "
                )
                if resp[0] and resp[1]:
                    skip_process.append(tp_key)
                    continue
            if tp_key == "degraded_mode":
                if not return_dict["degraded_done"]:
                    LOGGER.critical(
                        "Process '%s' for Cluster Degraded Mode Transition stopped.",
                        process.pid,
                    )
                    raise DegradedModeError(f"Process with PID {process.pid} stopped.")
                LOGGER.info(
                    "Process with PID for Cluster Degraded Mode Transition %s completed.",
                    process.pid,
                )
                skip_process.append(tp_key)
                continue
            LOGGER.critical(
                "Process with PID %s Name %s exited. Stopping other Process.",
                process.pid,
                process.name,
            )
            return tp_key
    for proc in skip_process:
        LOGGER.warning("Process '%s' removed from monitoring...", processes[proc].pid)
        processes.pop(proc)
    return None


def terminate_processes(processes: dict) -> None:
    """
    Terminate Process on failure.

    :param processes: List of process to be terminated.
    """
    LOGGER.debug("Processes to terminate: %s", processes)
    for process in processes.values():
        process.terminate()
        process.join()


def start_processes(processes: dict) -> None:
    """
    Trigger all proces from process list.

    :param processes: List of process to start.
    """
    LOGGER.info("Processes to start: %s", processes)
    for process in processes.values():
        process.start()
        LOGGER.info("Process started: %s", process)


def schedule_execution_plan(parsed_input: dict, options: munch.Munch, return_dict: dict) -> dict:
    """Schedule the execution plan."""
    processes = {}
    commons_params = {
        "access_secret_keys": get_s3_keys(S3_CFG.access_key, S3_CFG.secret_key),
        "endpoint_url": S3_CFG.endpoint,
        "use_ssl": S3_CFG.use_ssl,
        "seed": options.seed,
        "sequential_run": options.sequential_run,
    }
    for test_plan, test_plan_value in parsed_input.items():
        processes[test_plan] = multiprocessing.Process(
            target=schedule_test_plan,
            name=test_plan,
            args=(
                test_plan,
                test_plan_value,
                commons_params,
            ),
        )
    LOGGER.info("scheduled execution plan. Processes: %s", processes)
    if options.support_bundle:
        processes["support_bundle"] = multiprocessing.Process(
            target=support_bundle.support_bundle_process,
            name="support_bundle",
            args=(CORIO_CFG.sb_interval_mins * 60,),
        )
        LOGGER.info(
            "Support bundle collection scheduled for every %s minutes",
            CORIO_CFG.sb_interval_mins,
        )
    if options.health_check:
        processes["health_check"] = multiprocessing.Process(
            target=cluster_health.health_check_process,
            name="health_check",
            args=(CORIO_CFG.hc_interval_mins * 60, return_dict),
        )
        LOGGER.info("Health check scheduled for every %s minutes", CORIO_CFG.hc_interval_mins)

    if options.degraded_mode:
        processes["degraded_mode"] = multiprocessing.Process(
            target=degrade_cluster.activate_degraded_mode_parallel,
            name="degraded_mode",
            args=(
                return_dict,
                MASTER_CFG,
            ),
        )
    return processes
