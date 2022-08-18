# -*- coding: utf-8 -*-
# !/usr/bin/python
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

"""Report module to generate the execution details."""

import logging
from datetime import datetime
from typing import Union

import pandas as pd

from src.commons.constants import ROOT
from src.commons.utils.corio_utils import convert_size
from src.commons.utils.corio_utils import get_report_file_path
from src.commons.utils.corio_utils import monitor_sessions_iterations

LOGGER = logging.getLogger(ROOT)


def log_status(parsed_input: dict, corio_start_time: datetime, **kwargs):
    """
    Log execution status into log file.

    :param parsed_input: Dict for all the input yaml files.
    :param corio_start_time: Start time for main process.
    # :param test_failed: Reason for failure is any.
    # :param terminated_tests: terminated tests from workload.
    """
    test_failed = kwargs.get("test_failed")
    action = kwargs.get("action", "")
    status_fpath = get_report_file_path(corio_start_time)
    execution_status = monitor_sessions_iterations(parsed_input, corio_start_time, **kwargs)
    kwargs["execution_status"] = execution_status
    LOGGER.info("Logging current status to %s", status_fpath)
    with open(status_fpath, 'w', encoding="utf-8") as status_file:
        status_file.write(f"\nLogging Status at {datetime.now()}")
        if test_failed == 'KeyboardInterrupt':
            status_file.write("\nTest execution stopped due to Keyboard interrupt.")
        elif test_failed is None:
            if action == "final":
                status_file.write('\nTest execution completed.')
            else:
                status_file.write('\nTest execution still in progress...')
        else:
            status_file.write(f'\nTest execution terminated due to error in {test_failed}')
        status_file.write(f'\nTotal execution Duration : {datetime.now() - corio_start_time}')
        status_file.write("\n\nTestWise Execution Details:")
        for key, value in parsed_input.items():
            dataframe = pd.DataFrame()
            pd.set_option('display.colheader_justify', 'center')
            for key1, value1 in value.items():
                input_dict = {"TEST_NO": key1,
                              "TEST_ID": value1['TEST_ID'],
                              "SESSIONS": int(value1['sessions'])}
                convert_object_size(input_dict, value1)
                update_tests_status(input_dict, corio_start_time, value1, **kwargs)
                dataframe = dataframe.append(input_dict, ignore_index=True)
            # Convert sessions into integer.
            dataframe = dataframe.astype({"SESSIONS": 'int'})
            status_file.write(f"\n\nTEST YAML FILE : {key}\n")
            dataframe.to_string(status_file)


def convert_object_size(input_dict: dict, value: Union[dict, list]) -> None:
    """
    Convert object size(str, list, tuple or dict) for reporting.

    :param input_dict: Dict for all the input yaml files.
    :param value: Dict/List/int for object size.
    """
    if isinstance(value['object_size'], (list, tuple)):
        input_dict["OBJECT_SIZE"] = [convert_size(x) for x in value['object_size']]
    elif isinstance(value['object_size'], dict):
        if 'start' in value['object_size']:
            input_dict["OBJECT_SIZE"] = {"START": convert_size(value['object_size']['start']),
                                         "END": convert_size(value['object_size']['end'])}
        else:
            object_dict = {}
            for key, _value in value['object_size'].items():
                object_dict.update({convert_size(key): _value})
            if object_dict:
                input_dict["OBJECT_SIZE"] = object_dict
    else:
        input_dict["OBJECT_SIZE"] = convert_size(value['object_size'])


def update_tests_status(input_dict: dict, corio_start_time: datetime, value: dict, **kwargs):
    """
    Update tests status in report.

    :param input_dict: Dict for all the input yaml files.
    :param corio_start_time: start time of workload execution.
    :param value: test details from workload execution.
    """
    # List of the test cases from terminated tp.
    terminated_tests = kwargs.get("terminated_tests", [])
    # Reason of the test execution failure.
    execution_status = kwargs.get("execution_status")
    test_failed = kwargs.get("test_failed", '')
    test_start_time = corio_start_time + value['start_time']
    if datetime.now() > test_start_time:
        input_dict["START_TIME"] = f"Started at {test_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        input_dict["RESULT_UPDATE"] = "In Progress"
        if datetime.now() > (test_start_time + value['min_runtime']):
            execution_time = execution_status[input_dict["TEST_ID"]]["execution_time"]
            if execution_time:
                input_dict["RESULT_UPDATE"] = f"Passed at {execution_time}"
                if kwargs.get("sequential_run"):
                    total_execution_time = execution_time - test_start_time
                else:
                    total_execution_time = datetime.now() - test_start_time
                input_dict["TOTAL_TEST_EXECUTION"] = total_execution_time
        if "Passed" not in input_dict["RESULT_UPDATE"]:
            # Report In Progress, Fail, Aborted and update status.
            if input_dict["TEST_ID"] in terminated_tests:
                LOGGER.error("Test execution terminated due to error in %s.", input_dict["TEST_ID"])
                input_dict["RESULT_UPDATE"] = "Fail"
            elif test_failed:
                input_dict["RESULT_UPDATE"] = "Aborted"
            else:
                input_dict["RESULT_UPDATE"] = "In Progress"
            total_execution_time = datetime.now() - test_start_time
            input_dict["TOTAL_TEST_EXECUTION"] = total_execution_time
    else:
        input_dict["START_TIME"] = f"Scheduled at {test_start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        input_dict["RESULT_UPDATE"] = "Not Triggered"
        input_dict["TOTAL_TEST_EXECUTION"] = "NA"
