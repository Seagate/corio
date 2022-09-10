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

"""Library for s3 IAM operations using rest api."""

import json
import logging

import requests

from config import CLUSTER_CFG


# pylint: disable=too-few-public-methods, import-error, import-outside-toplevel, maybe-no-member
class DisableWarning:
    """Disable the insecure request warning."""

    from requests.packages.urllib3.exceptions import InsecureRequestWarning

    log = None
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class _Base(DisableWarning):
    """Base class for rest calls."""

    def __init__(self, **kwargs):
        """Initialize resp api base class."""
        self.log = self.log if self.log else logging.getLogger(__name__)
        self.log.info(CLUSTER_CFG["restapi"])
        self.host = kwargs.get("ip", CLUSTER_CFG["restapi"]["ip"])
        self.port = kwargs.get("port", int(CLUSTER_CFG["restapi"]["port"]))
        self.user_name = kwargs.get("username", CLUSTER_CFG["restapi"]["username"])
        self.user_password = kwargs.get("password", CLUSTER_CFG["restapi"]["password"])
        self.headers = None
        self._request = {
            "get": requests.get,
            "post": requests.post,
            "patch": requests.patch,
            "delete": requests.delete,
            "put": requests.put,
        }

    def rest_call(self, request_type: requests.api, endpoint: str, headers: dict, data: dict):
        """
        Function will request REST methods like GET, POST ,PUT etc.

        :param request_type: get/post/delete/update etc
        :param endpoint: endpoint url.
        :param data: data required for REST call.
        :param headers: headers required for REST call.
        :return: response of the request.
        """
        request_url = self._base_url() + endpoint if endpoint else self._base_url()
        data = json.dumps(data) if isinstance(data, dict) else data
        request_details = {
            "Request URL": request_url,
            "Request type": request_type.upper(),
            "Header": headers,
            "Data": data,
        }
        self.log.debug(request_details)

        # Request a REST call
        response = self._request[request_type](
            request_url, headers=headers, data=data, verify=False
        )
        self.log.info("Response Object: %s", response)
        if response.ok is not True:
            self.log.exception(response.json()["message"])
            raise response.raise_for_status() from requests.exceptions.HTTPError

        return response

    def _base_url(self):
        """Create rest api base url."""
        return f"https://{self.host}:{self.port}"


class Authentication:
    """Authenticate user for rest request."""

    @staticmethod
    def login(func):
        """Login the session and get the Authorization token for any rest calls."""

        def _wrapper(self, *args, **kwargs):
            """
            wrapper for login function.

            :param self: reference of class object.
            :param args: arguments of the executable function.
            :param kwargs: keyword arguments of the executable function.
            :return: function executables.
            """
            response = self.rest_call(
                "post",
                endpoint="/api/v2/login",
                headers={"Content-Type": "application/json"},
                data=json.dumps({"username": self.user_name, "password": self.user_password}),
            )
            self.headers = {
                "Authorization": response.headers["Authorization"],
                "Content-Type": "application/json",
            }
            response = func(self, *args, **kwargs)
            return response

        return _wrapper

    @staticmethod
    def logout(func):
        """Rest call for execute rest call and log out the session."""

        def _wrapper(self, *args, **kwargs):
            """
            Wrapper for logout function.

            :param self: reference of class object.
            :param args: arguments of the executable function.
            :param kwargs: keyword arguments of the executable function.
            :return: function executables.
            """
            # Execute prior functions.
            response = func(self, *args, **kwargs)
            # logout session.
            self.rest_call("post", endpoint="/api/v2/logout", headers=self.headers, data={})
            return response

        return _wrapper


class IAMClient(_Base):
    """This is the class for IAM user rest calls."""

    def __int__(self, **kwargs):
        """Initialize IAM Client."""
        super().__init__(**kwargs)

    @Authentication.login
    @Authentication.logout
    def create_user(self, user_name, display_name=None):
        """Create IAM user."""
        payload = {
            "uid": user_name,
            "display_name": display_name if display_name else user_name,
            "max_buckets": 0,
        }
        response = self.rest_call(
            request_type="post", endpoint="/api/v2/iam/users", headers=self.headers, data=payload
        ).json()
        response = {
            "AccessKey": {
                "UserName": response["keys"][0]["user"],
                "AccessKeyId": response["keys"][0]["access_key"],
                "Status": "Active",
                "SecretAccessKey": response["keys"][0]["secret_key"],
            }
        }
        self.log.info(response)
        return response

    @Authentication.login
    @Authentication.logout
    def list_users(self):
        """List IAM users."""
        response = self.rest_call(
            request_type="get", endpoint="/api/v2/iam/users", headers=self.headers, data={}
        )
        self.log.info(response.json())
        return response.json()

    @Authentication.login
    @Authentication.logout
    def delete_user(self, user_name):
        """Delete IAM users."""
        response = self.rest_call(
            request_type="delete",
            endpoint=f"/api/v2/iam/users/{user_name}",
            headers=self.headers,
            data={},
        )
        self.log.info(response.json())
        return response.json()
