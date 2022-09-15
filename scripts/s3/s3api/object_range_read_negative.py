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
"""Script type5 s3 object operation negative scenario workload for io stability."""

import os
import random
from datetime import timedelta, datetime
from time import perf_counter_ns

from botocore.exceptions import ClientError

from src.commons.constants import MIN_DURATION
from src.libs.s3api import S3Api


class TestType5ObjectReadNegative(S3Api):
    """S3 objects type5 operations negative scenario class."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, test_id: str, **kwargs) -> None:
        """
        s3 objects operations negative scenario init class.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :keyword test_id: Test ID string.
        :keyword use_ssl: To use secure connection.
        :keyword seed: Seed to be used for random data generator
        :keyword session: session name.
        :keyword sessions: Number of sessions used for IO.
        :keyword duration: Duration timedelta object, if not given will run for 100 days.
        """
        super().__init__(
            access_key,
            secret_key,
            endpoint_url=endpoint_url,
            use_ssl=kwargs.get("use_ssl"),
            test_id=f"{kwargs.get('test_id')}_object_negative",
        )
        random.seed(kwargs.get("seed"))
        self.test_id = test_id
        self.kwargs = kwargs
        self.session_id = kwargs.get("session")
        self.finish_time = datetime.now() + kwargs.get("duration", timedelta(hours=int(100 * 24)))

    async def execute_object_read_negative_workload(self):
        """Execute object range read with invalid size workload for specific duration."""
        iteration = 1
        object_size = self.kwargs.get("object_size")
        while True:
            try:
                self.log.info("Iteration %s is started for %s...", iteration, self.session_id)
                bucket_name = f"object-op-{self.test_id}-{perf_counter_ns()}".lower()
                self.log.info("Create bucket %s", bucket_name)
                resp = await self.create_bucket(bucket_name)
                self.log.info("Bucket created %s", resp)
                if isinstance(object_size, dict):
                    file_size = random.randrange(object_size["start"], object_size["end"])  # nosec
                else:
                    file_size = object_size
                await self.upload_n_number_objects(bucket_name, file_size)
                self.log.info("List objects of created %s bucket", bucket_name)
                resp = await self.list_objects(bucket_name)
                key_name = random.choice(resp)
                resp = await self.head_object(bucket_name, key_name)
                random_range = random.randrange(1,resp['ContentLength'])
                byte_range = f"bytes={resp['ContentLength']-random_range}-{resp['ContentLength']+random_range}"
                self.log.info("Range read for range %s", byte_range)
                try:
                    resp = await self.get_object(bucket=bucket_name, key=key_name, ranges=byte_range)
                    assert False, f"Expected failure in range read for invalid range:{key_name}, resp: {resp}"
                except ClientError as err:
                    self.log.info("Get Object range read exception for invalid range %s", err)
                await self.delete_bucket(bucket_name, True)
            except Exception as err:
                self.log.exception("bucket url: {%s}\nException: {%s}", self.s3_url, err)
                assert False, f"bucket url: {self.s3_url}\nException: {err}"
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                return True, "Object operation negative execution completed successfully."
            iteration += 1

    async def upload_n_number_objects(self, bucket_name, file_size):
        """Upload n number of objects."""
        number_of_objects = self.kwargs.get("number_of_objects", 500)
        self.log.info("Upload %s number of objects to bucket %s", number_of_objects, bucket_name)
        for i in range(1, number_of_objects + 1):
            file_name = f"object-{i}-{perf_counter_ns()}"
            self.log.info("Object '%s', object size %s bytes", file_name, file_size)
            file_path = corio_utils.create_file(file_name, file_size)
            await self.upload_object(bucket_name, file_name, file_path=file_path)
            self.log.info("'%s' uploaded successfully.", self.s3_url)
            self.log.info("Delete generated file")
            if os.path.exists(file_path):
                os.remove(file_path)
