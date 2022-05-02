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

import os
import random
from datetime import datetime, timedelta
from time import perf_counter_ns

from botocore.exceptions import ClientError

from src.commons.constants import MIN_DURATION
from src.commons.utils.corio_utils import convert_size
from src.commons.utils.corio_utils import create_file
from src.libs.s3api.s3_bucket_ops import S3Bucket
from src.libs.s3api.s3_object_ops import S3Object


class TestBucketOps(S3Object, S3Bucket):
    """S3 Bucket Operations class for executing given io stability workload."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, test_id: str,
                 **kwargs) -> None:
        """
        s3 bucket operations init class.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :param test_id: Test ID string.
        :param use_ssl: To use secure connection.
        :param object_size: Object size to be used for bucket operation
        :param seed: Seed to be used for random data generator
        :param session: session name.
        :param duration: Duration timedelta object, if not given will run for 100 days.
        """
        super().__init__(access_key, secret_key, endpoint_url=endpoint_url, use_ssl=kwargs.get(
            "use_ssl"), test_id=test_id)
        random.seed(kwargs.get("seed"))
        self.object_per_iter = 500
        self.object_size = kwargs.get("object_size")
        self.test_id = test_id
        self.session_id = kwargs.get("session")
        self.iteration = 1
        if kwargs.get("duration"):
            self.finish_time = datetime.now() + kwargs.get("duration")
        else:
            self.finish_time = datetime.now() + timedelta(hours=int(100 * 24))

    async def execute_bucket_workload(self):
        """Execute bucket operations workload for specific duration."""
        while True:
            self.log.info("Iteration %s is started for %s...", self.iteration, self.session_id)
            try:
                if isinstance(self.object_size, dict):
                    file_size = random.randrange(self.object_size["start"], self.object_size["end"])
                else:
                    file_size = self.object_size
                bucket_name = f'bucket-op-{self.test_id}-{perf_counter_ns()}'.lower()
                self.log.info("Create bucket %s", bucket_name)
                await self.create_bucket(bucket_name)
                await self.upload_n_number_objects(bucket_name, file_size)
                self.log.info("List all buckets")
                await self.list_buckets()
                self.log.info("List objects of created %s bucket", bucket_name)
                await self.list_objects(bucket_name)
                self.log.info("Perform Head bucket")
                await self.head_bucket(bucket_name)
                self.log.info("Delete bucket %s with all objects in it.", bucket_name)
                await self.delete_bucket(bucket_name, True)
            except (ClientError, IOError, AssertionError) as err:
                self.log.exception(err)
                raise err
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                return True, "Bucket operation execution completed successfully."
            self.log.info("Iteration %s is completed of %s...", self.iteration, self.session_id)
            self.iteration += 1

    async def upload_n_number_objects(self, bucket_name, file_size):
        """Upload n number of objects."""
        self.log.info("Upload %s number of objects to bucket %s", self.object_per_iter, bucket_name)
        for i in range(0, self.object_per_iter):
            file_name = f'object-{i}-{perf_counter_ns()}'
            self.log.info("Object '%s', object size %s", file_name, convert_size(file_size))
            file_path = create_file(file_name, file_size)
            await self.upload_object(bucket_name, file_name, file_path=file_path)
            self.log.info("'s3://%s/%s' uploaded successfully.", bucket_name, file_name)
            self.log.info("Delete generated file")
            if os.path.exists(file_path):
                os.remove(file_path)
