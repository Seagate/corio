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

import argparse
import glob
import logging
import multiprocessing
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime
from distutils.util import strtobool
from pprint import pformat

import munch
import schedule

from config import S3_CFG, CORIO_CFG
from src.commons.cluster_health import check_health
from src.commons.cluster_health import health_check_process
from src.commons.constants import CMN_LOG_DIR, LOG_DIR
from src.commons.constants import DT_STRING
from src.commons.constants import ROOT
from src.commons.degrade_cluster import activate_degraded_mode
from src.commons.exception import HealthCheckError
from src.commons.logger import StreamToLogger
from src.commons.report import log_status
from src.commons.scheduler import schedule_test_plan
from src.commons.support_bundle import collect_upload_rotate_support_bundles
from src.commons.support_bundle import support_bundle_process
from src.commons.utils.corio_utils import cpu_memory_details
from src.commons.utils.corio_utils import log_cleanup
from src.commons.utils.corio_utils import setup_environment
from src.commons.utils.corio_utils import store_logs_to_nfs_local_server
from src.commons.utils.jira_utils import JiraApp
from src.commons.utils.resource_util import collect_resource_utilisation
from src.commons.workload_mapping import SCRIPT_MAPPING
from src.commons.yaml_parser import test_parser

LOGGER = logging.getLogger(ROOT)


def initialize_loghandler(opt):
    """Initialize io driver runner logging with stream and file handlers."""
    # If log level provided then it will use DEBUG else will use default INFO.
    dir_path = os.path.join(os.path.join(LOG_DIR, "latest"))
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    name = os.path.splitext(os.path.basename(__file__))[0]
    if opt.verbose:
        level = logging.getLevelName(logging.DEBUG)
        name = os.path.join(dir_path, f"{name}_console_{DT_STRING}.DEBUG")
    else:
        level = logging.getLevelName(logging.INFO)
        name = os.path.join(dir_path, f"{name}_console_{DT_STRING}.INFO")
    os.environ["log_level"] = level
    LOGGER.setLevel(level)
    StreamToLogger(name, LOGGER, stream=True)


def parse_args():
    """Commandline arguments for CORIO Driver."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-ti", "--test_input", type=str,
                        help="Directory path containing test data input yaml files or "
                             "input yaml file path.")
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="log level used verbose(debug), default is info.")
    parser.add_argument("-us", "--use_ssl", type=lambda x: bool(strtobool(str(x))), default=True,
                        help="Use HTTPS/SSL connection for S3 endpoint.")
    parser.add_argument("-sd", "--seed", type=int, help="seed.",
                        default=random.SystemRandom().randint(1, 9999999))
    parser.add_argument("-sk", "--secret_key", type=str, help="s3 secret Key.")
    parser.add_argument("-ak", "--access_key", type=str, help="s3 access Key.")
    parser.add_argument("-ep", "--endpoint", type=str,
                        help="fqdn/ip:port of s3 endpoint for io operations without http/https."
                             "protocol in endpoint is based on use_ssl flag.",
                        default="s3.seagate.com")
    parser.add_argument("-nn", "--number_of_nodes", type=int,
                        help="number of nodes in k8s system", default=1)
    parser.add_argument("-sb", "--support_bundle", type=lambda x: bool(strtobool(str(x))),
                        default=False, help="Capture Support bundle.")
    parser.add_argument("-hc", "--health_check", type=lambda x: bool(strtobool(str(x))),
                        default=False, help="Health Check.")
    parser.add_argument("-tp", "--test_plan", type=str, default=None,
                        help="jira xray test plan id")
    parser.add_argument("-dm", "--degraded_mode", type=lambda x: bool(strtobool(str(x))),
                        default=False,
                        help="Degraded Mode, True/False")
    return parser.parse_args()


def main(options):
    """
    Main function for CORIO.

    :param options: Parsed Arguments.
    """
    LOGGER.info("Setting up environment!!")
    # Check cluster is healthy to start execution.

    if options.degraded_mode:
        activate_degraded_mode(options)
        options.number_of_nodes -= os.environ['DEGRADED_PODS']
    else:
        os.environ["d_bucket"] = False

    if options.health_check:
        check_health()
    setup_environment()
    pre_requisites(options)
    jira_obj = options.test_plan
    tests_details = {}
    if jira_obj:
        jira_obj = JiraApp()
        tests_details = jira_obj.get_all_tests_details_from_tp(options.test_plan, reset_status=True)
    workload_list = get_workload_list(options.test_input)
    LOGGER.info("Test YAML Files to be executed : %s", workload_list)
    parsed_input = get_parsed_input_details(workload_list, options.number_of_nodes)
    tests_to_execute = check_report_duplicate_missing_ids(parsed_input, tests_details)
    corio_start_time = datetime.now()
    LOGGER.info("Parsed files data:\n %s", pformat(parsed_input))
    LOGGER.info("List of tests to be executed with jira update: %s", tests_to_execute)
    processes = schedule_execution_plan(parsed_input, options)
    sched_job = schedule.every(30).minutes.do(log_status, parsed_input=parsed_input,
                                              corio_start_time=corio_start_time, test_failed=None)
    LOGGER.info("Report status update scheduled for every %s minutes", 30)
    terminated_tp, test_ids = None, []
    try:
        start_processes(processes)
        while True:
            cpu_memory_details()
            time.sleep(1)
            schedule.run_pending()
            if jira_obj:
                jira_obj.update_jira_status(
                    corio_start_time=corio_start_time, tests_details=tests_to_execute)
            terminated_tp = monitor_processes(processes)
            if terminated_tp:
                test_ids = get_test_ids_from_terminated_workload(parsed_input, terminated_tp)
                sys.exit()
    except (KeyboardInterrupt, MemoryError, HealthCheckError) as error:
        LOGGER.exception(error)
        terminated_tp = type(error).__name__
        sys.exit()
    finally:
        terminate_processes(processes)
        schedule.cancel_job(sched_job)
        log_status(parsed_input, corio_start_time, terminated_tp, terminated_tests=test_ids)
        if jira_obj:
            jira_obj.update_jira_status(corio_start_time=corio_start_time,
                                        tests_details=tests_to_execute, aborted=True,
                                        terminated_tests=test_ids)
        if options.support_bundle:
            collect_upload_rotate_support_bundles(MOUNT_DIR, os.getenv("sb_identifier"))
        collect_resource_utilisation(action="stop")
        LOGGER.info("Cleaning up TestData")
        if os.path.exists(DATA_DIR_PATH):
            shutil.rmtree(DATA_DIR_PATH)


def pre_requisites(options: munch.Munch):
    """Perform health check and start resource monitoring."""
    setup_environment()
    # Check cluster is healthy to start execution.
    if options.health_check:
        check_health()
    # start resource utilisation.
    collect_resource_utilisation(action="start")
    # Unique id for each run.
    os.environ["run_id"] = DT_STRING


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


def get_workload_list(path: str) -> list:
    """Get all workload filepath list."""
    if os.path.isdir(path):
        file_list = glob.glob(path + "/*")
    elif os.path.isfile(path):
        file_list = [os.path.abspath(path)]
    else:
        raise IOError(f"Incorrect test input: {path}")
    return file_list


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
                        "min_runtime"]
                else:
                    missing_jira_ids.append(test_value["TEST_ID"])
    # Check and report duplicate test ids from workload.
    duplicate_ids = [test_id for test_id, count in Counter(test_ids).items() if count > 1]
    assert (not duplicate_ids), f"Found duplicate ids in workload files. ids {set(duplicate_ids)}"
    if tests_details:
        # If jira update selected then will report missing workload test ids from jira TP.
        assert (not missing_jira_ids), f"List of workload test ids {missing_jira_ids} " \
                                       f"which are missing from jira tp: {tests_details.key()}"
    return tests_to_execute


def schedule_execution_plan(parsed_input: dict, options: munch.Munch):
    """Schedule the execution plan."""
    processes = {}
    commons_params = {"access_key": S3_CFG.access_key,
                      "secret_key": S3_CFG.secret_key,
                      "endpoint_url": S3_CFG.endpoint,
                      "use_ssl": S3_CFG.use_ssl,
                      "seed": options.seed}
    for test_plan, test_plan_value in parsed_input.items():
        processes[test_plan] = multiprocessing.Process(target=schedule_test_plan, name=test_plan,
                                                       args=(test_plan, test_plan_value,
                                                             commons_params))
    LOGGER.info("scheduled execution plan. Processes: %s", processes)
    if options.support_bundle:
        processes["support_bundle"] = multiprocessing.Process(target=support_bundle_process,
                                                              name="support_bundle",
                                                              args=(CORIO_CFG.sb_interval_mins * 60,
                                                                    ))
        LOGGER.info("Support bundle collection scheduled for every %s minutes",
                    CORIO_CFG.sb_interval_mins)
    if options.health_check:
        processes["health_check"] = multiprocessing.Process(target=health_check_process,
                                                            name="health_check",
                                                            args=(CORIO_CFG.hc_interval_mins * 60,))
        LOGGER.info("Health check scheduled for every %s minutes", CORIO_CFG.hc_interval_mins)
    return processes


def get_test_ids_from_terminated_workload(workload_dict: dict, workload_key: str) -> list:
    """Get all test-id from terminated workload due to failure."""
    test_ids = []
    for test in workload_dict[workload_key].values():
        test_ids.append(test["TEST_ID"])
    return test_ids


def monitor_processes(processes: dict) -> str or None:
    """Monitor the process."""
    for tp_key, process in processes.items():
        if not process.is_alive():
            if tp_key == "support_bundle":
                LOGGER.warning("Process with PID %s stopped Support bundle collection"
                               " error.", process.pid)
                continue
            if tp_key == "health_check":
                raise HealthCheckError(f"Process with PID {process.pid} stopped."
                                       f" Health Check collection error.")
            LOGGER.critical("Process with PID %s Name %s exited. Stopping other Process.",
                            process.pid, process.name)
            return tp_key
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


def main(options):
    """
    Main function for CORIO.

    :param options: Parsed Arguments.
    """
    # Pre-requisite to start CORIO run.
    pre_requisites(options)
    jira_obj = options.test_plan
    tests_details = {}
    if jira_obj:
        jira_obj = JiraApp()
        tests_details = jira_obj.get_all_tests_details_from_tp(options.test_plan, reset_status=True)
    workload_list = get_workload_list(options.test_input)
    LOGGER.info("Test YAML Files to be executed : %s", workload_list)
    parsed_input = get_parsed_input_details(workload_list, options.number_of_nodes)
    tests_to_execute = check_report_duplicate_missing_ids(parsed_input, tests_details)
    corio_start_time = datetime.now()
    LOGGER.info("Parsed files data:\n %s", pformat(parsed_input))
    LOGGER.info("List of tests to be executed with jira update: %s", tests_to_execute)
    processes = schedule_execution_plan(parsed_input, options)
    sched_job = schedule.every(30).minutes.do(log_status, parsed_input=parsed_input,
                                              corio_start_time=corio_start_time, test_failed=None)
    LOGGER.info("Report status update scheduled for every %s minutes", 30)
    terminated_tp, test_ids = None, []
    try:
        start_processes(processes)
        while True:
            cpu_memory_details()
            time.sleep(1)
            schedule.run_pending()
            if jira_obj:
                jira_obj.update_jira_status(
                    corio_start_time=corio_start_time, tests_details=tests_to_execute)
            terminated_tp = monitor_processes(processes)
            if terminated_tp:
                test_ids = get_test_ids_from_terminated_workload(parsed_input, terminated_tp)
                sys.exit()
    except (KeyboardInterrupt, MemoryError, HealthCheckError) as error:
        LOGGER.exception(error)
        terminated_tp = type(error).__name__
        sys.exit()
    finally:
        terminate_processes(processes)
        schedule.cancel_job(sched_job)
        log_status(parsed_input, corio_start_time, terminated_tp, terminated_tests=test_ids)
        if jira_obj:
            jira_obj.update_jira_status(corio_start_time=corio_start_time,
                                        tests_details=tests_to_execute, aborted=True,
                                        terminated_tests=test_ids)
        if options.support_bundle:
            collect_upload_rotate_support_bundles(CMN_LOG_DIR)
        collect_resource_utilisation(action="stop")
        store_logs_to_nfs_local_server()


if __name__ == "__main__":
    # backup old execution logs.
    log_cleanup()
    OPTS = parse_args()
    initialize_loghandler(OPTS)
    LOGGER.info("Arguments: %s", OPTS)
    main(OPTS)
