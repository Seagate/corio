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
"""File contains s3 multipart test script for io stability."""

import logging
import os
import random
import hashlib
from datetime import datetime, timedelta
from time import perf_counter_ns
from typing import Union
from src.libs.s3api.s3_multipart_ops import S3MultiParts
from src.libs.s3api.s3_object_ops import S3Object
from src.libs.s3api.s3_bucket_ops import S3Bucket

LOGGER = logging.getLogger(__name__)


class TestMultiParts(S3MultiParts, S3Object, S3Bucket):
    """Multipart class for executing given io stability workload."""

    # pylint: disable=too-many-arguments, too-many-locals, too-many-instance-attributes
    # pylint: disable=too-few-public-methods, too-many-statements

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, use_ssl: bool,
                 object_size: Union[dict, bytes], part_range: dict, seed: int, test_id: str = None,
                 range_read: Union[dict, bytes] = None, part_copy: bool = False,
                 duration: timedelta = None) -> None:
        """s3 multipart init for multipart, part copy operations with different workload.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :param test_id: Test ID string.
        :param use_ssl: To use secure connection.
        :param object_size: Size of the object in bytes.
        :param part_range: Number of parts to be uploaded from given range.
        :param part_copy: Perform part copy if True else normal part upload.
        :param duration: Duration timedelta object, if not given will run for 100 days.
        """
        super().__init__(access_key, secret_key, endpoint_url=endpoint_url, use_ssl=use_ssl)
        random.seed(seed)
        self.object_size = object_size
        self.part_range = part_range
        self.part_copy = part_copy
        self.range_read = range_read
        self.iteration = 1
        self.min_duration = 10  # In seconds
        if test_id:
            self.test_id = test_id.lower()
        else:  # If test_id missing then generate random number for execution.
            self.test_id = str(random.randrange(24, 240))
        if duration:
            self.finish_time = datetime.now() + duration
        else:  # If duration not given then test will run for 100 Day
            self.finish_time = datetime.now() + timedelta(hours=int(100 * 24))

    async def execute_multipart_workload(self):
        """Execute multipart workload for specific duration."""
        mpart_bucket = "s3mpart-bkt-{}-{}".format(self.test_id, perf_counter_ns())
        await self.create_bucket(mpart_bucket)
        while True:
            LOGGER.info("Iteration %s is started for %s...", self.iteration, self.test_id)
            try:
                LOGGER.info("Bucket name: %s", mpart_bucket)
                s3mpart_object = "s3mpart-obj-{}-{}".format(self.test_id, perf_counter_ns())
                s3_object = "s3-obj-{}-{}".format(self.test_id, perf_counter_ns())
                LOGGER.info("Object name: %s", s3mpart_object)
                if isinstance(self.object_size, dict):
                    file_size = random.randrange(self.object_size["start"], self.object_size["end"])
                else:
                    file_size = self.object_size
                LOGGER.info("File size: %s GiB", (file_size / (1024 ** 3)))
                number_of_parts = random.randrange(self.part_range["start"], self.part_range["end"])
                LOGGER.info("Number of parts: %s", number_of_parts)
                assert number_of_parts <= 10000, "Number of parts should be equal/less than 10k"
                single_part_size = round(file_size / number_of_parts)
                LOGGER.info("single part size: %s MiB", single_part_size / (1024 ** 2))
                response = await self.create_multipart_upload(mpart_bucket, s3mpart_object)
                assert response["UploadId"], f"Failed to initiate multipart upload: {response}"
                mpu_id = response["UploadId"]
                random_part = random.randrange(1, number_of_parts + 1)
                parts = list()
                file_hash = hashlib.sha256()
                for i in range(1, number_of_parts + 1):
                    byte_s = os.urandom(single_part_size)
                    if self.part_copy and i == random_part:
                        resp = await self.upload_object(body=byte_s, bucket=mpart_bucket,
                                                        key=s3_object)
                        assert resp["ETag"] is not None, f"Failed upload object: {resp}"
                        resp = await self.upload_part_copy(f"{mpart_bucket}/{s3_object}",
                                                           mpart_bucket, s3_object, part_number=i,
                                                           upload_id=mpu_id)
                        parts.append({"PartNumber": i, "ETag": resp[1]["CopyPartResult"]["ETag"]})
                        object_list = await self.list_objects(mpart_bucket)
                        assert s3_object in object_list, f"Failed to upload object {s3_object}"
                    else:
                        response = await self.upload_part(byte_s, mpart_bucket,
                                                          s3mpart_object, upload_id=mpu_id,
                                                          part_number=i)
                        assert response["ETag"] is not None, f"Failed upload part: {response}"
                        parts.append({"PartNumber": i, "ETag": response["ETag"]})
                    file_hash.update(byte_s)
                upload_obj_checksum = file_hash.hexdigest()
                LOGGER.info("Checksum of uploaded object: %s", upload_obj_checksum)
                response = await self.list_parts(mpu_id, mpart_bucket, s3mpart_object)
                assert response, f"Failed to list parts: {response}"
                response = await self.list_multipart_uploads(mpart_bucket)
                assert response, f"Failed to list multipart uploads: {response}"
                response = await self.complete_multipart_upload(
                    mpu_id, parts, mpart_bucket, s3mpart_object)
                LOGGER.info("'s3://%s/%s' uploaded successfully.", mpart_bucket, s3mpart_object)
                all_object = await self.list_objects(mpart_bucket)
                assert s3mpart_object in all_object, f"Failed to upload object {s3mpart_object}"
                assert response, f"Failed to completed multi parts: {response}"
                response = await self.head_object(mpart_bucket, s3mpart_object)
                assert response, f"Failed to do head object on {s3mpart_object}"
                download_obj_checksum = await self.get_s3object_checksum(
                    mpart_bucket, s3mpart_object, single_part_size)
                LOGGER.info("Checksum of s3 object: %s", download_obj_checksum)
                assert upload_obj_checksum == download_obj_checksum,\
                    f"Failed to match checksum: {upload_obj_checksum}, {download_obj_checksum}"
                if self.range_read:
                    self.range_read = {
                        "start": random.randrange(
                            1, self.range_read), "end": self.range_read} if isinstance(
                        self.range_read, int) else self.range_read
                    LOGGER.info("Get object using suggested range read '%s'.", self.range_read)
                    resp = await self.get_object(bucket=mpart_bucket,
                                                 key=s3mpart_object,
                                                 ranges=f"bytes={self.range_read['start']}"
                                                        f"-{self.range_read['end']}")
                    assert resp['Body'].read(), f"Failed to read bytes {self.range_read} from " \
                        f"s3://{mpart_bucket}/{s3mpart_object}"
                if self.part_copy:
                    await self.delete_object(mpart_bucket, s3_object)
                await self.delete_object(mpart_bucket, s3mpart_object)
            except Exception as err:
                LOGGER.exception(err)
                raise err
            timedelta_sec = (self.finish_time - datetime.now()).total_seconds()
            if timedelta_sec < self.min_duration:
                LOGGER.info("Delete bucket %s with all objects in it.", mpart_bucket)
                await self.delete_bucket(mpart_bucket, force=True)
                return True, "Multipart execution completed successfully."
            LOGGER.info("Iteration %s is completed of %s...", self.iteration, self.test_id)
            self.iteration += 1
