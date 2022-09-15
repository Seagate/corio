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
from src.commons.utils import utility
from src.libs.s3api import S3Api


class TestType5ObjectRRNegative(S3Api):
    """S3 objects type5 operations negative scenario class."""

    def __init__(
        self, access_key: str, secret_key: str, endpoint_url: str, test_id: str, **kwargs
    ) -> None:
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

    # pylint: disable=broad-except
    async def execute_multipart_abort_workload(self):
        """Execute multipart abort workload for specific duration."""
        iteration = 1
        object_size = self.kwargs.get("object_size")
        number_of_parts = self.kwargs.get("parts", 20)
        while True:
            try:
                self.log.info("Iteration %s is started for %s...", iteration, self.session_id)
                mpart_bucket = f"s3mpart-bkt-{self.test_id}-{perf_counter_ns()}"
                await self.create_bucket(mpart_bucket)
                s3mpart_object = f"s3mpart-obj-{self.test_id}-{perf_counter_ns()}"
                self.log.info("Object name: %s", s3mpart_object)
                if isinstance(object_size, dict):
                    file_size = random.randrange(object_size["start"], object_size["end"])  # nosec
                else:
                    file_size = object_size
                single_part_size = round(file_size / number_of_parts)
                self.log.info("single part size: %s", utility.convert_size(single_part_size))
                response = await self.create_multipart_upload(mpart_bucket, s3mpart_object)
                mpu_id = response["UploadId"]
                for i in range(1, number_of_parts + 1):
                    byte_s = os.urandom(round(single_part_size))
                    await self.upload_part(
                        byte_s, mpart_bucket, s3mpart_object, upload_id=mpu_id, part_number=i
                    )
                parts = await self.list_parts(mpart_bucket, s3mpart_object, mpu_id)
                while parts:
                    await self.abort_multipart_upload(mpart_bucket, s3mpart_object, mpu_id)
                    parts = await self.list_parts(mpart_bucket, s3mpart_object, mpu_id)
                try:
                    resp = await self.get_object(bucket=mpart_bucket, key=s3mpart_object)
                    assert (
                        False
                    ), f"Expected failure in GetObject API for {s3mpart_object}, resp: {resp}"
                except ClientError as err:
                    self.log.info("Get Object exception for non existing object %s", err)
                self.log.info(
                    "Iteration %s is completed of %s...",
                    iteration,
                    self.session_id,
                )
                await self.delete_object(mpart_bucket, s3mpart_object)
            except Exception as err:
                self.log.exception("bucket url: {%s} \nException: {%s}", self.s3_url, err)
                assert False, f"bucket url: {self.s3_url} \n Exception: {err}"
            await self.delete_bucket(mpart_bucket, True)
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                return True, "Object operation negative execution completed successfully."
            iteration += 1
