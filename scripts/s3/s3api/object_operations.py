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
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#

"""Python Library to perform bucket operations using boto3 module."""

import os
import random
import time
from datetime import timedelta, datetime
from typing import Union

from src.commons.utils.corio_utils import create_file
from src.libs.s3api.s3_bucket_ops import S3Bucket
from src.libs.s3api.s3_object_ops import S3Object
from src.commons.constants import MIN_DURATION


# pylint: disable=too-few-public-methods
class TestS3Object(S3Bucket, S3Object):
    """Class for bucket operations."""

    # pylint: disable=too-many-arguments
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, test_id: str,
                 use_ssl: bool, object_size: Union[int, dict], seed: int, session: str,
                 range_read: Union[int, dict] = None, duration: timedelta = None):
        """S3 Object operation init.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :param test_id: Test ID string.
        :param use_ssl: To use secure connection.
        :param object_size: Size of the object in bytes.
        :param seed: Seed for random number generator.
        :param session: session name.
        :param range_read: Range read size
        :param duration: Duration timedelta object, if not given will run for 100 days.
        """
        super().__init__(access_key, secret_key, endpoint_url=endpoint_url,
                         use_ssl=use_ssl, test_id=test_id)
        random.seed(seed)
        self.object_size = object_size
        self.test_id = test_id
        self.session_id = session
        self.iteration = 1
        self.range_read = range_read
        self.parts = 3
        if duration:
            self.finish_time = datetime.now() + duration
        else:
            self.finish_time = datetime.now() + timedelta(hours=int(100 * 24))

    # pylint: disable=too-many-locals
    async def execute_object_workload(self):
        """Execute object workload with given parameters."""
        bucket = f'object-op-{self.test_id}-{time.perf_counter_ns()}'.lower()
        self.log.info("Create bucket %s", bucket)
        await self.create_bucket(bucket)
        while True:
            self.log.info("Iteration %s is started for %s...", self.iteration, self.session_id)
            try:
                if isinstance(self.object_size, list):
                    file_size = self.object_size[random.randrange(0, len(self.object_size))]
                elif isinstance(self.object_size, dict):
                    file_size = random.randrange(self.object_size["start"], self.object_size["end"])
                else:
                    file_size = self.object_size
                if isinstance(self.range_read, dict):
                    range_read = random.randrange(self.range_read["start"], self.range_read["end"])
                else:
                    range_read = self.range_read
                file_name = f'object-bucket-op-{time.perf_counter_ns()}'
                file_path = create_file(file_name, file_size)
                checksum_in = self.checksum_file(file_path)
                self.log.debug("Checksum IN = %s", checksum_in)
                await self.upload_object(bucket, file_name, file_path=file_path)
                self.log.info("s3://%s/%s uploaded successfully.", bucket, file_name)
                if range_read:
                    part = int(file_size / self.parts)
                    part_ranges = [(0, part), (part + 1, part * 2),
                                   (part * 2 + 1, file_size - range_read)]
                    for start, end in part_ranges:
                        loc = random.randrange(start, end)
                        byte_range = f"{loc}-{loc + range_read - 1}"
                        checksum_out = await self.get_s3object_checksum(
                            bucket, file_name, ranges=f'bytes={byte_range}')
                        checksum_in = self.checksum_part_file(file_path, loc, range_read)
                        assert checksum_out == checksum_in, \
                            f"Checksum of downloaded part for range ({byte_range}) does not " \
                            f"match for s3://{bucket}/{file_name}. Uploaded Checksum = " \
                            f"{checksum_out} Downloaded Checksum = {checksum_in}"
                else:
                    self.log.info("Perform Head bucket.")
                    await self.head_object(bucket, file_name)
                    self.log.info("Get Object")
                    checksum_out = await self.get_s3object_checksum(bucket, file_name)
                    assert checksum_in == checksum_out, "Checksum are not equal"
                    self.log.debug("Checksum Out = %s", checksum_out)
                    self.log.info("Delete object")
                    await self.delete_object(bucket, file_name)
                    os.remove(file_path)
            except Exception as err:
                self.log.exception(err)
                raise err
            timedelta_v = (self.finish_time - datetime.now())
            timedelta_sec = timedelta_v.total_seconds()
            if timedelta_sec < MIN_DURATION:
                self.log.info("Delete bucket %s with all objects in it.", bucket)
                await self.delete_bucket(bucket, True)
                return True, "Multipart execution completed successfully."
            self.log.info("Iteration %s is completed of %s...", self.iteration, self.session_id)
            self.iteration += 1
