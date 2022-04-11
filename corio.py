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
from datetime import datetime
from distutils.util import strtobool
from pprint import pformat
from collections import Counter

import schedule

from config import S3_CFG, CORIO_CFG, CLUSTER_CFG
from src.commons.utils.corio_utils import log_cleanup
from src.commons.logger import StreamToLogger
from src.commons import yaml_parser
from src.commons.constants import MOUNT_DIR
from src.commons.constants import ROOT
from src.commons.utils.cluster_services import mount_nfs_server
from src.commons.utils.cluster_services import collect_upload_sb_to_nfs_server
from src.commons.utils.jira_utils import JiraApp
from src.commons.utils.corio_utils import cpu_memory_details
from src.commons.exception import HealthCheckError
from src.commons.utils.resource_util import collect_resource_utilisation
from src.commons.report import log_status
from src.commons.workload_mapping import function_mapping
from src.commons.scheduler import schedule_test_plan

LOGGER = logging.getLogger(ROOT)
DT_STRING = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")


def initialize_loghandler(opt):
    """
    Initialize io driver runner logging with stream and file handlers.

    param opt: logging level used in CorIO tool.
    """
    # If  log level provided then it will use DEBUG else will use default INFO.
    if opt.verbose:
        level = logging.getLevelName(logging.DEBUG)
    else:
        level = logging.getLevelName(logging.INFO)
    os.environ['log_level'] = level
    LOGGER.setLevel(level)
    dir_path = os.path.join(os.path.join(os.getcwd(), "log", "latest"))
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    name = os.path.splitext(os.path.basename(__file__))[0]
    name = os.path.join(dir_path, f"{name}_console_{DT_STRING}.log")
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
    return parser.parse_args()


def setup_environment():
    """Tool installations for test execution."""
    ret = mount_nfs_server(CORIO_CFG['nfs_server'], MOUNT_DIR)
    assert ret, "Error while Mounting NFS directory"


def terminate_processes(process_list):
    """
    Terminate Process on failure.

    :param process_list: Terminate the given list of process
    """
    for process in process_list:
        process.terminate()
        process.join()


def support_bundle_process(interval, sb_identifier):
    """Support bundle wrapper.

    :param interval: Interval in Seconds
    :param sb_identifier: Support Bundle Identifier
    """
    while True:
        time.sleep(interval)
        resp = collect_upload_sb_to_nfs_server(MOUNT_DIR, sb_identifier,
                                               max_sb=CORIO_CFG['max_no_of_sb'])
        if not resp[0]:
            return resp


def health_check_process(interval):
    """Health check wrapper.

    :param interval: Interval in Seconds
    """
    while True:
        time.sleep(interval)
        # ToDo: Enable once health check is added
        # resp = check_cluster_services()
        # if not resp[0]:
        #     return resp


# pylint: disable-msg=too-many-branches,too-many-locals, too-many-statements
def main(options):
    """
    Main function for CORIO.

    :param options: Parsed Arguments
    """
    LOGGER.info("Setting up environment!!")
    setup_environment()
    commons_params = {'access_key': S3_CFG.access_key,
                      'secret_key': S3_CFG.secret_key,
                      'endpoint_url': S3_CFG.endpoint,
                      'use_ssl': S3_CFG["use_ssl"],
                      'seed': options.seed}
    tests_details = {}
    tests_to_execute = {}
    jira_obj = None
    jira_flg = options.test_plan
    # start resource util
    collect_resource_utilisation(action="start")
    if jira_flg:
        jira_obj = JiraApp()
        tests_details = jira_obj.get_all_tests_details_from_tp(options.test_plan)
    if os.path.isdir(options.test_input):
        file_list = glob.glob(options.test_input + "/*")
    elif os.path.isfile(options.test_input):
        file_list = [os.path.abspath(options.test_input)]
    else:
        raise IOError(f"Incorrect test input: {options.test_input}")
    LOGGER.info("Test YAML Files to be executed : %s", file_list)
    parsed_input = {}
    for each in file_list:
        parsed_input[each] = yaml_parser.test_parser(each, options.number_of_nodes)
    test_ids, missing_jira_ids = [], []
    for key, value in parsed_input.items():
        LOGGER.info("Test Values : %s", value)
        for test_key, test_value in value.items():
            test_ids.append(test_value["TEST_ID"])
            if 'operation' in test_value.keys():
                test_value['operation'] = function_mapping[
                    test_value['operation']]
                value[test_key] = test_value
            if jira_flg:
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
    if jira_flg:
        # If jira update selected then will report missing workload test ids from jira TP.
        assert (not missing_jira_ids), f"List of workload test ids {missing_jira_ids} " \
                                       f"which are missing from jira tp: {options.test_plan}"
    corio_start_time = datetime.now()
    LOGGER.info("Parsed input files : ")
    LOGGER.info(pformat(parsed_input))
    LOGGER.info("List of tests to be executed with jira update: %s", tests_to_execute)
    processes = {}
    for test_plan, test_plan_value in parsed_input.items():
        process = multiprocessing.Process(target=schedule_test_plan, name=test_plan,
                                          args=(test_plan, test_plan_value, commons_params))
        processes[test_plan] = process
    LOGGER.info(processes)
    sb_identifier = CLUSTER_CFG['nodes'][0]['hostname'] + DT_STRING if options.support_bundle \
        else DT_STRING
    if options.support_bundle:
        process = multiprocessing.Process(target=support_bundle_process, name="support_bundle",
                                          args=(CORIO_CFG['sb_interval_mins'] * 60, sb_identifier))
        processes["support_bundle"] = process
    if options.health_check:
        process = multiprocessing.Process(target=health_check_process, name="health_check",
                                          args=(CORIO_CFG['hc_interval_mins'] * 60,))
        processes["health_check"] = process
    sched_job = schedule.every(30).minutes.do(log_status, parsed_input=parsed_input,
                                              corio_start_time=corio_start_time, test_failed=None)
    try:
        for process in processes.values():
            process.start()
        test_ids = []
        while True:
            terminated_tp = None
            cpu_memory_details()
            time.sleep(1)
            schedule.run_pending()
            if jira_flg:
                jira_obj.update_jira_status(
                    corio_start_time=corio_start_time, tests_details=tests_to_execute)
            for key, process in processes.items():
                if not process.is_alive():
                    if key == "support_bundle":
                        LOGGER.warning("Process with PID %s stopped Support bundle collection"
                                       " error.", process.pid)
                        continue
                    if key == "health_check":
                        raise HealthCheckError(f"Process with PID {process.pid} stopped."
                                               f" Health Check collection error.")
                    LOGGER.critical("Process with PID %s Name %s exited. Stopping other Process.",
                                    process.pid, process.name)
                    terminated_tp = key
                    # Get all test id from terminated workload due to failure.
                    for test in parsed_input[terminated_tp].values():
                        test_ids.append(test["TEST_ID"])
            if terminated_tp:
                terminate_processes(processes.values())
                log_status(parsed_input, corio_start_time, terminated_tp, terminated_tests=test_ids)
                if jira_flg:
                    jira_obj.update_jira_status(corio_start_time=corio_start_time,
                                                tests_details=tests_to_execute, aborted=True,
                                                terminated_tests=test_ids)
                schedule.cancel_job(sched_job)
                if options.support_bundle:
                    resp = collect_upload_sb_to_nfs_server(MOUNT_DIR, sb_identifier,
                                                           max_sb=CORIO_CFG['max_no_of_sb'])
                    LOGGER.info("collect support bundles response: %s", resp)
                collect_resource_utilisation(action="stop")
                sys.exit()
    except (KeyboardInterrupt, MemoryError, HealthCheckError) as error:
        LOGGER.exception(error)
        terminate_processes(processes.values())
        log_status(parsed_input, corio_start_time, 'KeyboardInterrupt')
        if jira_flg:
            jira_obj.update_jira_status(corio_start_time=corio_start_time,
                                        tests_details=tests_to_execute, aborted=True)
        schedule.cancel_job(sched_job)
        if options.support_bundle:
            resp = collect_upload_sb_to_nfs_server(
                MOUNT_DIR, sb_identifier, max_sb=CORIO_CFG['max_no_of_sb'])
            LOGGER.info("collect support bundles response: %s", resp)
        # TODO: cleanup object files created
        # stop resource util
        collect_resource_utilisation(action="stop")
        sys.exit()


if __name__ == "__main__":
    log_cleanup()
    opts = parse_args()
    initialize_loghandler(opts)
    LOGGER.info("Arguments: %s", opts)
    main(opts)
