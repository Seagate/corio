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
#
"""s3 Copy Object workload for io stability."""

import os
import random
from datetime import datetime
from datetime import timedelta
from time import perf_counter_ns

from src.commons.constants import MIN_DURATION
from src.commons.utils.corio_utils import convert_size
from src.commons.utils.corio_utils import create_file
from src.libs.s3api.s3_bucket_ops import S3Bucket
from src.libs.s3api.s3_object_ops import S3Object


class TestS3CopyObjects(S3Object, S3Bucket):
    """S3 Copy Object class for executing given io stability workload."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, test_id: str,
                 **kwargs) -> None:
        """s3 Copy Object init class.

        :param access_key: access key
        :param secret_key: secret key
        :param endpoint_url: endpoint with http or https
        :param test_id: Test ID string
        :param use_ssl: To use secure connection
        :param object_size: Object size
        :param seed: Seed to be used for random data generator
        :param session: session name.
        :param duration: Duration timedelta object, if not given will run for 100 days
        """
        super().__init__(access_key, secret_key, endpoint_url=endpoint_url, use_ssl=kwargs.get(
            "use_ssl"), test_id=f"{test_id}_copy_object_operations")
        random.seed(kwargs.get("seed"))
        self.object_size = kwargs.get("object_size")
        self.iteration = 1
        self.session_id = kwargs.get("session")
        self.test_id = test_id.lower()
        self.range_read = kwargs.get("range_read")
        if kwargs.get("duration"):
            self.finish_time = datetime.now() + kwargs.get("duration")
        else:
            self.finish_time = datetime.now() + timedelta(hours=int(100 * 24))

    @classmethod
    def initialize_variables(cls, test_id):
        """Initialize variables for copy object operations."""
        cls.bucket_name1 = f"copy-obj-bucket1-{test_id}-{perf_counter_ns()}"
        cls.bucket_name2 = f"copy-obj-bucket2-{test_id}-{perf_counter_ns()}"
        cls.object_name1 = f"copy-object1-{test_id}-{perf_counter_ns()}"
        cls.object_name2 = f"copy-object2-{test_id}-{perf_counter_ns()}"

    # pylint: disable=broad-except
    async def execute_copy_object_workload(self):
        """Execute copy object workload for specific duration."""
        self.initialize_variables(self.test_id)
        await self.create_bucket(self.bucket_name1)
        self.log.info("Created bucket %s", self.bucket_name1)
        await self.create_bucket(self.bucket_name2)
        self.log.info("Created bucket %s", self.bucket_name2)
        while True:
            self.log.info("Iteration %s is started for %s...", self.iteration, self.session_id)
            try:
                # Put object in self.bucket_name1
                file_size = await self.get_workload_size()
                file_path = create_file(self.object_name1, file_size)
                self.log.info("Object1 '%s', object size %s", self.object_name1, convert_size(
                    file_size))
                await self.upload_object(self.bucket_name1, self.object_name1, file_path=file_path)
                self.log.info("Objects '%s' uploaded successfully.", self.s3_url)
                ret1 = await self.head_object(self.bucket_name1, self.object_name1)
                await self.copy_object(self.bucket_name1, self.object_name1, self.bucket_name2,
                                       self.object_name2)
                self.log.info("Copied object '%s in same account successfully.", self.s3_url)
                ret2 = await self.head_object(self.bucket_name2, self.object_name2)
                assert ret1["ETag"] == ret2["ETag"], \
                    f"etag of original object ({ret1['ETag']})\netag of copied object " \
                    f"({ret2['ETag']}) are not matching"
                if self.range_read:
                    if isinstance(self.range_read, dict):
                        range_read = random.randrange(
                            self.range_read["start"], self.range_read["end"])
                    else:
                        range_read = self.range_read
                    self.log.info("Get object with range read '%s' bytes.", range_read)
                    offset = random.randrange(file_size - range_read)
                    await self.data_integrity(byte_range=f'bytes={offset}-{range_read + offset}')
                else:
                    self.log.info("Download source and destination object and compare checksum.")
                    await self.data_integrity()
                self.log.info("Delete source object from bucket-1.")
                await self.delete_object(self.bucket_name1, self.object_name1)
                self.log.info("List destination object from bucket-2.")
                await self.head_object(self.bucket_name2, self.object_name2)
                self.log.info("Delete destination object from bucket-2.")
                await self.delete_object(self.bucket_name2, self.object_name2)
                os.remove(file_path)
            except Exception as err:
                self.log.exception("bucket url: {%s}\nException: {%s}", self.s3_url, err)
                assert False, f"bucket url: {self.s3_url}\nException: {err}"
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                for bucket in [self.bucket_name1, self.bucket_name2]:
                    self.log.info("Delete bucket %s with all objects in it.", bucket)
                    await self.delete_bucket(bucket, True)
                return True, "Copy Object execution completed successfully."
            self.log.info("Iteration %s is completed of %s...", self.iteration, self.session_id)
            self.iteration += 1

    async def get_workload_size(self) -> int:
        """Read the workload size in bytes."""
        if isinstance(self.object_size, list):
            file_size = self.object_size[random.randrange(0, len(self.object_size))]
        elif isinstance(self.object_size, dict):
            file_size = random.randrange(self.object_size["start"], self.object_size["end"])
        else:
            file_size = self.object_size
        return file_size

    async def data_integrity(self, byte_range: str = None) -> None:
        """Download/read source and destination object and compare checksum."""
        if byte_range:
            self.log.info("Reading chunk %s", byte_range)
            checksum1 = await self.get_s3object_checksum(
                bucket=self.bucket_name1, key=self.object_name1, ranges=byte_range)
            checksum2 = await self.get_s3object_checksum(
                bucket=self.bucket_name2, key=self.object_name2, ranges=byte_range)
        else:
            checksum1 = await self.get_s3object_checksum(self.bucket_name1, self.object_name1)
            checksum2 = await self.get_s3object_checksum(self.bucket_name2, self.object_name2)
        assert checksum1 == checksum2, f"SHA256 ({checksum1}) of original object " \
                                       f"({self.object_name1}) and SHA256 ({checksum2}) of copied" \
                                       f" object ({self.object_name2}) are not matching."
