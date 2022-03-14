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

"""RestAPI library using aiobotocore module."""

import logging

import boto3
import urllib3
from aiobotocore.config import AioConfig
from aiobotocore.session import get_session
from botocore.config import Config

from config import S3_CFG
from src.commons.logger import get_logger

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# pylint: disable=too-many-instance-attributes
class S3RestApi:
    """Basic Class for Creating Boto3 REST API Objects."""

    def __init__(self, access_key: str, secret_key: str, **kwargs):
        """
        Initialize members of S3Lib.

        Different instances need to be created as per different parameter values like access_key,
        secret_key etc.
        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint url.
        :param s3_cert_path: s3 certificate path.
        :param region: region.
        :param aws_session_token: aws_session_token.
        :param debug: debug mode.
        """
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = kwargs.get("region", S3_CFG.region)
        self.aws_session_token = kwargs.get("aws_session_token", None)
        self.use_ssl = kwargs.get("use_ssl", S3_CFG.use_ssl)
        self.endpoint_url = kwargs.get("endpoint_url", S3_CFG.endpoint)
        self.log = get_logger(os.getenv("log_level") or logging.INFO, kwargs.get("test_id"))
        self.log_path = next(iter([handler.baseFilename for handler in self.log.handlers if
                                   isinstance(handler, logging.FileHandler)]))

    def get_client(self):
        """Create s3 client session for asyncio operations."""
        session = get_session()
        return session.create_client(service_name="s3",
                                     use_ssl=self.use_ssl,
                                     verify=False,
                                     aws_access_key_id=self.access_key,
                                     aws_secret_access_key=self.secret_key,
                                     endpoint_url=self.endpoint_url,
                                     region_name=self.region,
                                     aws_session_token=self.aws_session_token,
                                     config=AioConfig(connect_timeout=S3_CFG.connect_timeout,
                                                      read_timeout=S3_CFG.read_timeout,
                                                      retries={"max_attempts":
                                                               S3_CFG.s3api_retry}))

    def get_boto3_client(self):
        """Create s3 client for without asyncio operations."""
        return boto3.client("s3",
                            use_ssl=self.use_ssl,
                            verify=False,
                            aws_access_key_id=self.access_key,
                            aws_secret_access_key=self.secret_key,
                            endpoint_url=self.endpoint_url,
                            region_name=self.region,
                            aws_session_token=self.aws_session_token,
                            config=Config(connect_timeout=S3_CFG.connect_timeout,
                                          read_timeout=S3_CFG.read_timeout,
                                          retries={'max_attempts': S3_CFG.s3api_retry}))

    def get_boto3_resource(self):
        """Create s3 resource for without asyncio operations."""
        return boto3.resource("s3",
                              use_ssl=self.use_ssl,
                              verify=False,
                              aws_access_key_id=self.access_key,
                              aws_secret_access_key=self.secret_key,
                              endpoint_url=self.endpoint_url,
                              region_name=self.region,
                              aws_session_token=self.aws_session_token,
                              config=Config(connect_timeout=S3_CFG.connect_timeout,
                                            read_timeout=S3_CFG.read_timeout,
                                            retries={'max_attempts': S3_CFG.s3api_retry}))

    def __str__(self):
        """Representation of an S3API object."""
        return "S3RestApi for asyncio operations using aiobotocore."
