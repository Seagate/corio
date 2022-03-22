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
"""s3 Copy Object workload for io stability."""

import logging
import os
import random
import time
from datetime import datetime, timedelta
from typing import Union

from botocore.exceptions import ClientError

from src.libs.s3api.s3_bucket_ops import S3Bucket
from src.libs.s3api.s3_object_ops import S3Object

LOGGER = logging.getLogger(__name__)


# pylint: disable=too-few-public-methods, too-many-statements
class TestS3CopyObjects(S3Object, S3Bucket):
    """S3 Copy Object class for executing given io stability workload"""

    # pylint: disable=too-many-arguments
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, test_id: str,
                 use_ssl: bool, object_size: Union[int, dict], seed: int,
                 range_read: Union[int, dict] = None, duration: timedelta = None) -> None:
        """s3 Copy Object init class.

        :param access_key: access key
        :param secret_key: secret key
        :param endpoint_url: endpoint with http or https
        :param test_id: Test ID string
        :param use_ssl: To use secure connection
        :param object_size: Object size
        :param seed: Seed to be used for random data generator
        :param duration: Duration timedelta object, if not given will run for 100 days
        """
        super().__init__(access_key, secret_key, endpoint_url=endpoint_url, use_ssl=use_ssl)
        random.seed(seed)
        self.object_size = object_size
        self.test_id = test_id
        self.iteration = 1
        self.range_read = range_read
        self.min_duration = 10  # In seconds
        if duration:
            self.finish_time = datetime.now() + duration
        else:
            self.finish_time = datetime.now() + timedelta(hours=int(100 * 24))

    # pylint: disable=too-many-locals
    async def execute_copy_object_workload(self):
        """Execute copy object workload for specific duration."""
        bucket1 = f"bucket-1-{self.test_id}-{time.perf_counter_ns()}".lower()
        bucket2 = f"bucket-2-{self.test_id}-{time.perf_counter_ns()}".lower()
        object1 = f"object-1-{self.test_id}-{time.perf_counter_ns()}".lower()
        object2 = f"object-2-{self.test_id}-{time.perf_counter_ns()}".lower()
        await self.create_bucket(bucket1)
        LOGGER.info("Created bucket %s", bucket1)
        await self.create_bucket(bucket2)
        LOGGER.info("Created bucket %s", bucket2)
        while True:
            LOGGER.info("Iteration %s is started for %s...", self.iteration, self.test_id)
            try:
                # Put object in bucket1
                if isinstance(self.object_size, list):
                    file_size = self.object_size[self.iteration % len(self.object_size)]
                elif isinstance(self.object_size, int):
                    file_size = self.object_size
                else:
                    file_size = random.randrange(self.object_size["start"], self.object_size["end"])
                else:
                    file_size = self.object_size
                with open(object1, 'wb') as f_out:
                    f_out.write(os.urandom(file_size))
                LOGGER.info("Object1 '%s', object size %s Kib", object1, file_size / 1024)
                await self.upload_object(bucket1, object1, file_path=object1)
                LOGGER.info("Objects 's3://%s/%s' uploaded successfully.", object1, bucket1)
                ret1 = await self.head_object(bucket1, object1)
                await self.copy_object(bucket1, object1, bucket2, object2)
                LOGGER.info(
                    "Copied object 's3://%s/%s' to s3://%s/%s in same account successfully.",
                    bucket1, object1, bucket1, bucket2)
                ret2 = await self.head_object(bucket2, object2)
                assert ret1["ETag"] == ret2["ETag"], \
                    f"etag of original object ({ret1['ETag']})\netag of copied object " \
                    f"({ret2['ETag']}) are not matching"
                if self.range_read:
                    if not isinstance(self.range_read, dict):
                        range_read = self.range_read
                    else:
                        range_read = random.randrange(
                            self.range_read["start"], self.range_read["end"])
                    LOGGER.info("Get object using suggested range read '%s'.", range_read)
                    offset = random.randrange(file_size - range_read)
                    LOGGER.info("Reading chunk bytes=%s-%s", offset, range_read + offset)
                    checksum1 = await self.get_s3object_checksum(
                        bucket=bucket1, key=object1, ranges=f'bytes={offset}-{range_read + offset}')
                    checksum2 = await self.get_s3object_checksum(
                        bucket=bucket2, key=object2, ranges=f'bytes={offset}-{range_read + offset}')
                    assert checksum1 == checksum2, \
                        f"SHA256 of original range ({checksum1})\nSHA256 of copied range " \
                        f"({checksum2}) are not matching"
                else:
                    LOGGER.info("Download source and destination object and compare checksum.")
                    checksum1 = await self.get_s3object_checksum(bucket1, object1)
                    checksum2 = await self.get_s3object_checksum(bucket2, object2)
                    assert checksum1 == checksum2, \
                        f"SHA256 of original range ({checksum1})\nSHA256 of copied range " \
                        f"({checksum2}) are not matching"
                LOGGER.info("Delete source object from bucket-1.")
                await self.delete_object(bucket1, object1)
                LOGGER.info("List destination object from bucket-2.")
                await self.head_object(bucket2, object2)
                LOGGER.info("Delete destination object from bucket-2.")
                await self.delete_object(bucket2, object2)
                os.remove(object1)
            except (ClientError, IOError, AssertionError) as err:
                LOGGER.exception(err)
                raise err
            timedelta_v = (self.finish_time - datetime.now())
            timedelta_sec = timedelta_v.total_seconds()
            if timedelta_sec < self.min_duration:
                await self.delete_bucket(bucket1, True)
                await self.delete_bucket(bucket2, True)
                return True, "Copy Object execution completed successfully."
            LOGGER.info("Iteration %s is completed of %s...", self.iteration, self.test_id)
            self.iteration += 1
