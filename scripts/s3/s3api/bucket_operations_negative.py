#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2020 Seagate Technology LLC and/or its Affiliates
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
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#
"""S3 bucket operation workload for io stability."""
import random
from time import perf_counter_ns
from datetime import datetime

import botocore.exceptions as bcexceptions
import boto3.exceptions as b3exceptions

from src.commons.constants import MIN_DURATION
from scripts.s3.s3api.bucket_operations import TestBucketOps


class TestBucketOpsNegative(TestBucketOps):
    """S3 Bucket Operations class for handling negative scenarios"""

    def __init__(
            self,
            access_key: str,
            secret_key: str,
            endpoint_url: str,
            test_id: str,
            **kwargs) -> None:
        super().__init__(access_key, secret_key, endpoint_url, test_id, **kwargs)

    # pylint: disable=broad-except
    async def execute_bucket_workload(self):
        """ Negative bucket operations"""
        iteration = 1
        object_size = self.kwargs.get("object_size")
        while True:
            try:
                self.log.info("Iteration %s is started for %s", iteration, self.session_id)
                file_size = random.randint(0, object_size)
                bucket_name = f"bucket-op-{self.test_id}-iter{iteration}-{perf_counter_ns()}"

                try:
                    self.log.info("Create bucket %s", bucket_name)
                    resp = await self.create_bucket(bucket_name)
                    assert False, f"Able to create bucket with invalid bucket name. {resp}"
                except (bcexceptions.InvalidBucketName, bcexceptions.ParamValidationError) as err:
                    self.log.info("Invalid bucket url: {%s}\nException: {%s}", self.s3_url, err)
                    self.log.info('Error Code: {}'.format(err.response['Error']['Code']))
                    self.log.info('Error Message: {}'.format(err.response['Error']['Message']))

                try:
                    self.log.info("Perform head bucket to check if bucket exists")
                    resp = await self.head_bucket(bucket_name)
                    assert False, f"Able to see bucket with invalid bucket name. {resp}"
                except bcexceptions.ClientError as err:
                    # If a client error is thrown, then check that it was a 404 error.
                    # If it was a 404 error, then the bucket does not exist.
                    self.log.info('Error Code: {}'.format(err.response['Error']['Code']))
                    self.log.info('Error Message: {}'.format(err.response['Error']['Message']))
                    error_code = int(err.response['Error']['Code'])
                    if error_code == 403:
                        self.log.info("Private Bucket. Forbidden Access!")
                    elif error_code == 404:
                        self.log.info("Bucket Does Not Exist!")

                try:
                    self.log.info("upload_n_number_objects")
                    resp = await self.upload_n_number_objects(bucket_name, file_size)
                    assert False, f"Able to upload objects with invalid bucket. {resp}"
                except (b3exceptions.S3UploadFailedError,
                        bcexceptions.InvalidBucketName,
                        bcexceptions.ParamValidationError) as err:
                    self.log.info("Invalid bucket url: {%s}\nException: {%s}", self.s3_url, err)
                    self.log.info('Error Code: {}'.format(err.response['Error']['Code']))
                    self.log.info('Error Message: {}'.format(err.response['Error']['Message']))

                try:
                    self.log.info("List Objects in buckets")
                    resp = await self.list_objects(bucket_name)
                    assert False, f"Able to list objects with invalid bucket. {resp}"
                except (bcexceptions.InvalidBucketName, bcexceptions.ParamValidationError) as err:
                    self.log.info("Invalid bucket url: {%s}\nException: {%s}", self.s3_url, err)
                    self.log.info('Error Code: {}'.format(err.response['Error']['Code']))
                    self.log.info('Error Message: {}'.format(err.response['Error']['Message']))
                try:
                    self.log.info("Delete bucket %s with all objects in it.", bucket_name)
                    resp = await self.delete_bucket(bucket_name, True)
                    assert False, f"Able to list objects with invalid bucket. {resp}"
                except (b3exceptions.ResourceNotExistsError, bcexceptions.ParamValidationError) as err:
                    self.log.info("Invalid bucket url: {%s}\nException: {%s}", self.s3_url, err)
                    self.log.info('Error Code: {}'.format(err.response['Error']['Code']))
                    self.log.info('Error Message: {}'.format(err.response['Error']['Message']))

                self.log.info("Iteration %s is completed of %s", iteration, self.session_id)
            except Exception as err:
                self.log.exception("bucket url: {%s}\nException: {%s}", self.s3_url, err)
                assert False, f"bucket url: {self.s3_url}\nException: {err}"

            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                return True, "Bucket operation execution completed successfully."
            iteration += 1
