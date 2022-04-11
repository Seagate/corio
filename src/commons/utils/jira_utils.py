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

"""library having jira related operations."""

import getpass
import json
import logging
import os
import sys
import time
from datetime import datetime
from http import HTTPStatus

import requests
from jira import JIRAError
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException
from requests.packages.urllib3.util.retry import Retry

LOGGER = logging.getLogger(__name__)


class JiraApp:
    """JiraApp class for jira operations."""

    def __init__(self):
        """Initialization for JiraApp."""
        self.auth = (self.get_jira_credential())
        self.headers = {'content-type': "application/json", 'accept': "application/json"}
        self.retry_strategy = Retry(
            total=10, backoff_factor=2, status_forcelist=[
                429, 500, 502, 503, 504, 400, 404, 408], method_whitelist=[
                "HEAD", "GET", "OPTIONS"])
        self.adapter = HTTPAdapter(max_retries=self.retry_strategy)
        self.http = requests.Session()
        self.http.mount("https://", self.adapter)
        self.http.mount("http://", self.adapter)
        self.jira_url = "https://jts.seagate.com/"
        self.jira_base_url = "https://jts.seagate.com/rest/raven/1.0/"

    def get_te_details_from_test_plan(self, test_plan_id: str) -> dict:
        """
        Get test executions details from test plan.

        :param test_plan_id: Test plan number in JIRA.
        :returns: List of dictionaries of test executions from test plan.
            Each dict will have id, key, summary, self, testEnvironments
            [{"id": 311993, "key": "TEST-16653", "summary": "TE:Auto-Stability-Release 515",
             "self": "https://jts.seagate.com/rest/api/2/issue/311993",
             "testEnvironments": ["515_full"]},]
        """
        tp_response = requests.get(f'{self.jira_base_url}api/testplan/{test_plan_id}/testexecution',
                                   auth=self.auth)
        if tp_response.status_code != HTTPStatus.OK:
            LOGGER.exception("Failed to get TE details: %s", test_plan_id)
            sys.exit(1)

        return tp_response.json()

    def get_tests_details_from_te(self, test_exe_id: str) -> list:
        """
        Get details of the test cases in a test execution ticket.

        :param test_exe_id: ID(key) of test execution ticket.
        :return: List of dictionaries of tests from test execution ticket.
            Each dict will have id, key, status, rank
            [{'id': 123270, 'status': 'PASS', 'key': 'TEST-19537', 'rank': 1},
            {'id': 184244, 'status': 'TODO', 'key': 'TEST-19526', 'rank': 2},]
        """
        tests_info = []
        try:
            te_response = requests.get(f"{self.jira_base_url}api/testexec/{test_exe_id}/test",
                                       auth=self.auth)
            if te_response is not None:
                if te_response.status_code != HTTPStatus.OK:
                    page_cnt = 1
                    timeout_start, timeout_sec = time.time(), 180
                    while page_cnt and (time.time() < timeout_start + timeout_sec):
                        try:
                            te_response = requests.get(f"{self.jira_base_url}api/testexec/"
                                                       f"{test_exe_id}/test?page={page_cnt}",
                                                       auth=self.auth)
                            data = te_response.json()
                            tests_info.extend(data)
                        except JIRAError as err:
                            LOGGER.error('Exception in GET tests details from te, error: %s', err)
                        else:
                            page_cnt = 0 if len(data) == 0 else page_cnt + 1
                else:
                    tests_info.extend(te_response.json())
        except (RequestException, ValueError, JIRAError) as err:
            LOGGER.error('Request exception in get_test_details %s', err)

        return tests_info

    def update_test_jira_status(self, test_exe_id, test_id, test_status):
        """
        Update test jira status in xray jira.

        :param test_exe_id: ID(key) of test execution ticket.
        :param test_id: ID(key) of test.
        :param test_status: Test jira status to be updated.
            TODO, PASS, EXECUTING, FAIL, ABORTED, BLOCKED, CONDITIONALPASS
        :return: Dictionary of response.
        {"testExecIssue":{"id":"328794","key":"TEST-19714",
        "self":"https://jts.seagate.com/rest/api/2/issue/328794"},
        "testIssues":{"success": [{"id":"328135","key":"TEST-19537",
        "self":"https://jts.seagate.com/rest/api/2/issue/328135"}]},"infoMessages":[]}
        """
        state = {}
        status = dict()
        state["testExecutionKey"] = test_exe_id
        status["testKey"] = test_id
        if test_status.upper() == 'EXECUTING':
            status["start"] = datetime.now().astimezone().isoformat(timespec='seconds')
        if test_status.upper() == 'PASS':
            status["finish"] = datetime.now().astimezone().isoformat(timespec='seconds')
        status["status"] = test_status
        state["tests"] = []
        state['tests'].append(status)
        data = json.dumps(state)
        status_response = requests.request("POST", f"{self.jira_base_url}import/execution",
                                           data=data, auth=self.auth, headers=self.headers,
                                           params=None)
        if status_response.status_code == HTTPStatus.OK:
            LOGGER.info("Test '%s' status updated successfully to %s", test_id, test_status)
            return status_response.json()
        test_issue = status_response.json().get("testIssues", None)
        if test_issue:
            if not test_issue.get("success", None):
                LOGGER.error("Failed to update test '%s' status.", test_id)

        return status_response.text

    def update_execution_details(self, test_run_id: str, test_id: str, comment: str) -> [dict]:
        """
        Add comment to the mentioned jira id.

        :param test_run_id: ID(id) of the test.
        :param test_id: ID(key) of the test.
        :param comment: Comment of the test execution.
        :return: Update execution details to test.
        """
        try:
            comment_response = requests.request(
                "PUT", f"{self.jira_base_url}testrun/{test_run_id}/comment", data=comment,
                auth=self.auth, headers=self.headers, params=None)
            LOGGER.info("Response code: %s", comment_response.status_code)
            if comment_response.status_code == HTTPStatus.OK:
                LOGGER.info("Updated execution details successfully for test id %s", test_id)
                return comment_response.json()

            return comment_response.text
        except JIRAError as err:
            LOGGER.exception("Error code: %s, error test: %s", err.status_code, err.text)
            return err

    def get_all_tests_details_from_tp(self, tp_id: str) -> dict:
        """
        Get all tests execution details from test plan.

        :param tp_id: ID of the test plan.
        :return: Dictionary of tests from test execution ticket.

        """
        tests_dict = {}
        te_details = self.get_te_details_from_test_plan(tp_id)
        for texe in te_details:
            tests_details = self.get_tests_details_from_te(texe['key'])
            for test in tests_details:
                if test['key'] in tests_dict:
                    raise Exception(f"Duplicate test id '{test['key']}' found in TP '{tp_id}',"
                                    f" TE '{texe['key']}'")
                tests_dict[test['key']] = test
                tests_dict[test['key']]["te"] = texe
        LOGGER.info(tests_dict)

        return tests_dict

    def update_jira_status(self, corio_start_time, tests_details, aborted=False,
                           terminated_tests=None):
        """
        Update execution status in jira.

        :param corio_start_time: Start time for main process.
        :param tests_details: Tests details from test plan.
        :param aborted: Aborted execution due to some issue.
        :param terminated_tests: Terminated tests from yaml file path.
        """
        for test_id, test_data in tests_details.items():
            test_start_time = corio_start_time + test_data['start_time']
            if datetime.now() >= test_start_time:
                if datetime.now() >= (test_start_time + test_data['min_runtime']):
                    if test_data['status'] == "EXECUTING":
                        if aborted:
                            aborted_tests = terminated_tests if terminated_tests else []
                            test_status = "FAILED" if test_id in aborted_tests else "ABORTED"
                            resp = self.update_test_jira_status(
                                test_data['te']['key'], test_id, test_status)
                            tests_details[test_id]['status'] = test_status
                            LOGGER.info(resp)
                        else:
                            resp = self.update_test_jira_status(
                                test_data['te']['key'], test_id, "PASS")
                            LOGGER.info(resp)
                            resp = self.update_execution_details(
                                test_data['id'], test_id,
                                f"Execution completed after {test_data['min_runtime']}")
                            tests_details[test_id]['status'] = "PASS"
                            LOGGER.info(resp)
                elif test_data['status'] == "TODO":
                    resp = self.update_test_jira_status(
                        test_data['te']['key'], test_id, "EXECUTING")
                    tests_details[test_id]['status'] = "EXECUTING"
                    LOGGER.info(resp)
                else:
                    if aborted:
                        aborted_tests = terminated_tests if terminated_tests else []
                        test_status = "FAIL" if test_id in aborted_tests else "ABORTED"
                        if test_data['status'] == "EXECUTING":
                            resp = self.update_test_jira_status(
                                test_data['te']['key'], test_id, test_status)
                            tests_details[test_id]['status'] = test_status
                            LOGGER.info(resp)

    @staticmethod
    def get_jira_credential() -> tuple:
        """
        Function to get Jira Credentials.

        :return: Credentials Tuple.
        """
        try:
            jira_user = os.environ['JIRA_USER']
            jira_pd = os.environ['JIRA_PASSWORD']
        except KeyError as error:
            LOGGER.error(error)
            jira_user = input("JIRA credentials not found in environment.\nJIRA username: ")
            jira_pd = getpass.getpass("JIRA password: ")
            os.environ['JIRA_USER'] = jira_user
            os.environ['JIRA_PASSWORD'] = jira_pd

        return jira_user, jira_pd
