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
"""Perform parallel S3 operations as per the given test input YAML using Asyncio."""

import logging
import multiprocessing
import os
import time
from collections import Counter
from datetime import datetime
from pprint import pformat

import schedule

from arguments import opts
from config import CORIO_CFG
from config import S3_CFG
from src.commons import cluster_health
from src.commons import constants as const
from src.commons import degrade_cluster
from src.commons import scheduler
from src.commons import support_bundle
from src.commons.exception import DegradedModeError
from src.commons.exception import HealthCheckError
from src.commons.logger import initialize_loghandler
from src.commons.utils import utility
from src.commons.utils.alerts import SendMailNotification
from src.commons.utils.jira import JiraApp
from src.commons.utils.resource import collect_resource_utilization
from src.commons.workload_mapping import SCRIPT_MAPPING
from src.commons.yaml_parser import test_parser

LOGGER = logging.getLogger(const.ROOT)


def pre_requisites(options):
    """Perform health check and start resource monitoring."""
    utility.setup_environment()
    # Check cluster is healthy to start execution.
    if options.health_check:
        cluster_health.check_health()
        # start resource utilization.
        collect_resource_utilization(action="start")
    # Unique id for each run.
    os.environ["run_id"] = const.DT_STRING


def get_parsed_input_details(flist: list, nodes: int) -> dict:
    """Parse workloads and get all details with function mapping."""
    parsed = {}
    for each in flist:
        parsed[each] = test_parser(each, nodes)
    # update function mapping.
    for _, value in parsed.items():
        LOGGER.info("Test Values : %s", value)
        for test_key, test_value in value.items():
            if "operation" in test_value.keys():
                if "partcopy" in test_value["operation"].lower():
                    test_value["part_copy"] = True
                test_value["operation"] = SCRIPT_MAPPING[test_value["operation"]]
                value[test_key] = test_value
    return parsed


def check_report_duplicate_missing_ids(parsed_input, tests_details):
    """Check and report duplicate test ids from workload."""
    test_ids = []
    missing_jira_ids = []
    tests_to_execute = {}
    for _, value in parsed_input.items():
        for _, test_value in value.items():
            test_ids.append(test_value["TEST_ID"])
            if tests_details:
                if test_value["TEST_ID"] in tests_details:
                    tests_to_execute[test_value["TEST_ID"]] = tests_details[test_value["TEST_ID"]]
                    tests_to_execute[test_value["TEST_ID"]]["start_time"] = test_value["start_time"]
                    tests_to_execute[test_value["TEST_ID"]]["min_runtime"] = test_value[
                        "min_runtime"
                    ]
                else:
                    missing_jira_ids.append(test_value["TEST_ID"])
    # Check and report duplicate test ids from workload.
    duplicate_ids = [test_id for test_id, count in Counter(test_ids).items() if count > 1]
    if duplicate_ids:
        raise AssertionError(f"Found duplicate ids in workload files. ids {set(duplicate_ids)}")
    if tests_details:
        # If jira update selected then will report missing workload test ids from jira TP.
        if missing_jira_ids:
            raise AssertionError(
                f"List of workload test ids {missing_jira_ids} which are missing"
                f" from jira tp: {tests_details.key()}"
            )
    if tests_to_execute:
        LOGGER.info("List of tests to be executed with jira update: %s", tests_to_execute)
    return tests_to_execute


def get_test_ids_from_terminated_workload(workload_dict: dict, workload_key: str) -> list:
    """Get all test-id from terminated workload due to failure."""
    test_ids = []
    for test in workload_dict[workload_key].values():
        test_ids.append(test["TEST_ID"])
    return test_ids


# pylint: disable=broad-except
def main(options):
    """
    CORIO main function to trigger workload.

    :param options: Parsed Arguments.
    """
    terminated_tp, test_ids, tests_details = None, [], {}
    jira_obj = JiraApp() if options.test_plan else None
    if jira_obj:
        tests_details = jira_obj.get_all_tests_details_from_tp(options.test_plan, reset_status=True)
    workload_list = utility.get_workload_list(options.test_input)
    LOGGER.info("Test YAML Files to be executed : %s", workload_list)
    parsed_input = get_parsed_input_details(workload_list, options.number_of_nodes)
    tests_to_execute = check_report_duplicate_missing_ids(parsed_input, tests_details)
    corio_start_time = datetime.now()
    LOGGER.info("Parsed files data:\n %s", pformat(parsed_input))
    return_dict = multiprocessing.Manager().dict()
    processes = scheduler.schedule_execution_plan(parsed_input, options, return_dict)
    sched = scheduler.schedule_test_status_update(
        parsed_input,
        corio_start_time,
        periodic_time=CORIO_CFG.report_interval_mins,
        sequential_run=options.sequential_run,
    )
    mobj = SendMailNotification(
        corio_start_time,
        options.test_plan,
        health_check=options.health_check,
        endpoint=S3_CFG.endpoint,
    )
    mobj.email_alert(action="start")
    try:
        if options.degraded_mode:
            degrade_cluster.get_degraded_mode()
        scheduler.start_processes(processes)
        while processes:
            time.sleep(1)
            utility.cpu_memory_details()
            schedule.run_pending()
            if jira_obj:
                jira_obj.update_jira_status(
                    corio_start_time=corio_start_time, tests_details=tests_to_execute
                )
            terminated_tp = scheduler.monitor_processes(processes, return_dict)
            if terminated_tp:
                test_ids = get_test_ids_from_terminated_workload(parsed_input, terminated_tp)
                break
            if tuple(processes.keys()) in const.terminate_process_list:
                break
    except (
        Exception,
        KeyboardInterrupt,
        MemoryError,
        HealthCheckError,
        DegradedModeError,
    ) as err:
        LOGGER.exception(err)
        terminated_tp = type(err).__name__
    finally:
        scheduler.terminate_processes(processes)
        scheduler.terminate_update_test_status(
            parsed_input,
            corio_start_time,
            terminated_tp,
            test_ids,
            sched,
            action="final",
            sequential_run=options.sequential_run,
        )
        if jira_obj:
            jira_obj.update_jira_status(
                corio_start_time=corio_start_time,
                tests_details=tests_to_execute,
                aborted=True,
                terminated_tests=test_ids,
            )
        if options.support_bundle:
            support_bundle.collect_upload_rotate_support_bundles(const.CMN_LOG_DIR)
            utility.store_logs_to_nfs_local_server()
        if options.health_check:
            collect_resource_utilization(action="stop")
        mobj.email_alert(action="stop", tp=terminated_tp, ids=test_ids)



if __name__ == "__main__":
    # backup old execution logs.
    utility.log_cleanup()
    initialize_loghandler(LOGGER, os.path.splitext(os.path.basename(__file__))[0], opts.verbose)
    LOGGER.info("Arguments: %s", opts)
    pre_requisites(opts)
    main(opts)
