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

import hashlib
import os
import random
from datetime import datetime, timedelta
from time import perf_counter_ns

from src.commons.constants import MIN_DURATION
from src.commons.utils import corio_utils
from src.libs.s3api import S3Api


# pylint: disable=too-many-instance-attributes
class TestMultiParts(S3Api):
    """Multipart class for executing given io stability workload."""

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        endpoint_url: str,
        test_id: str,
        **kwargs,
    ) -> None:
        """s3 multipart init for multipart, part copy operations with different workload.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :param test_id: Test ID string.
        :param use_ssl: To use secure connection.
        :param object_size: Size of the object in bytes.
        :param session: session name.
        :param part_range: Number of parts to be uploaded from given range.
        :param part_copy: Perform part copy if True else normal part upload.
        :param duration: Duration timedelta object, if not given will run for 100 days.
        """
        self.part_copy = kwargs.get("part_copy", False)
        super().__init__(
            access_key,
            secret_key,
            endpoint_url=endpoint_url,
            use_ssl=kwargs.get("use_ssl"),
            test_id=(
                f"{test_id}_multipart_partcopy_operations"
                if self.part_copy
                else f"{test_id}_multipart_operations"
            ),
        )
        random.seed(kwargs.get("seed"))
        self.object_size = kwargs.get("object_size")
        self.part_range = kwargs.get("part_range")
        self.range_read = kwargs.get("range_read")
        self.session_id = kwargs.get("session")
        self.iteration = 1
        self.test_id = test_id.lower()
        if kwargs.get("duration"):
            self.finish_time = datetime.now() + kwargs.get("duration")
        else:
            # If duration not given then test will run for 100 Day
            self.finish_time = datetime.now() + timedelta(hours=int(100 * 24))

    # pylint: disable=broad-except
    async def execute_multipart_workload(self):
        """Execute multipart workload for specific duration."""
        mpart_bucket = f"s3mpart-bkt-{self.test_id}-{perf_counter_ns()}"
        await self.create_bucket(mpart_bucket)
        while True:
            try:
                self.log.info("Iteration %s is started for %s...", self.iteration, self.session_id)
                self.log.info("Bucket name: %s", mpart_bucket)
                s3mpart_object = f"s3mpart-obj-{self.test_id}-{perf_counter_ns()}"
                s3_object = f"s3-obj-{self.test_id}-{perf_counter_ns()}"
                self.log.info("Object name: %s", s3mpart_object)
                number_of_parts = await self.get_random_number_of_parts()
                file_size = await self.get_workload_size()
                single_part_size = round(file_size / number_of_parts)
                self.log.info("single part size: %s", corio_utils.convert_size(single_part_size))
                upload_obj_checksum = await self.create_upload_list_completed_mpart(
                    number_of_parts, mpart_bucket, s3mpart_object, s3_object
                )
                all_object = await self.list_objects(mpart_bucket)
                assert s3mpart_object in all_object, f"Failed to upload object {s3mpart_object}"
                await self.head_object(mpart_bucket, s3mpart_object)
                download_obj_checksum = await self.get_s3object_checksum(
                    mpart_bucket, s3mpart_object, single_part_size
                )
                self.log.info("Checksum of s3 object: %s", download_obj_checksum)
                assert (
                    upload_obj_checksum == download_obj_checksum
                ), f"Failed to match checksum: {upload_obj_checksum}, {download_obj_checksum}"
                if self.range_read:
                    await self.range_read_mpart_object(mpart_bucket, s3mpart_object)
                if self.part_copy:
                    await self.delete_object(mpart_bucket, s3_object)
                await self.delete_object(mpart_bucket, s3mpart_object)
                self.log.info(
                    "Iteration %s is completed of %s...",
                    self.iteration,
                    self.session_id,
                )
            except Exception as err:
                self.log.exception("bucket url: {%s} \nException: {%s}", self.s3_url, err)
                assert False, f"bucket url: {self.s3_url} \n Exception: {err}"
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                await self.delete_bucket(mpart_bucket, force=True)
                self.log.info("Deleted bucket %s with all objects in it.", mpart_bucket)
                return True, "Multipart execution completed successfully."
            self.iteration += 1

    async def get_workload_size(self):
        """Get the workload size."""
        if isinstance(self.object_size, dict):
            file_size = random.randrange(self.object_size["start"], self.object_size["end"])
        else:
            file_size = self.object_size
        self.log.info("File size: %s", corio_utils.convert_size(file_size))
        return file_size

    async def get_random_number_of_parts(self):
        """Get the random number of parts."""
        number_of_parts = random.randrange(self.part_range["start"], self.part_range["end"])
        self.log.info("Number of parts: %s", number_of_parts)
        assert number_of_parts <= 10000, "Number of parts should be equal/less than 10k"
        return number_of_parts

    async def range_read_mpart_object(self, s3bucket, s3object):
        """Range read on uploaded multipart object."""
        self.range_read = (
            {"start": random.randrange(1, self.range_read), "end": self.range_read}
            if isinstance(self.range_read, int)
            else self.range_read
        )
        self.log.info("Get object using suggested range read '%s'.", self.range_read)
        resp = await self.get_object(
            bucket=s3bucket,
            key=s3object,
            ranges=f"bytes={self.range_read['start']}" f"-{self.range_read['end']}",
        )
        assert resp, f"Failed to read bytes {self.range_read} from s3://{s3bucket}/{s3object}"

    async def create_upload_list_completed_mpart(
        self, number_of_parts, mpart_bucket, s3mpart_object, s3_object
    ) -> str:
        """Upload, list and complete multipart operations."""
        response = await self.create_multipart_upload(mpart_bucket, s3mpart_object)
        random_part = random.randrange(1, number_of_parts + 1)
        parts = []
        file_hash = hashlib.sha256()
        for i in range(1, number_of_parts + 1):
            byte_s = os.urandom(round(await self.get_workload_size() / number_of_parts))
            if self.part_copy and i == random_part:
                await self.upload_object(body=byte_s, bucket=mpart_bucket, key=s3_object)
                assert s3_object in await self.list_objects(mpart_bucket), (
                    f"Failed to upload " f"object {s3_object}"
                )
                upload_resp = await self.upload_part_copy(
                    f"{mpart_bucket}/{s3_object}",
                    mpart_bucket,
                    s3_object,
                    part_number=i,
                    upload_id=response["UploadId"],
                )
            else:
                upload_resp = await self.upload_part(
                    byte_s,
                    mpart_bucket,
                    s3mpart_object,
                    upload_id=response["UploadId"],
                    part_number=i,
                )
            etag = upload_resp["CopyPartResult"]["ETag"] if self.part_copy else upload_resp["ETag"]
            assert etag is not None, f"Failed upload part: {upload_resp}"
            parts.append({"PartNumber": i, "ETag": etag})
            file_hash.update(byte_s)
        upload_obj_checksum = file_hash.hexdigest()
        self.log.info("Checksum of uploaded object: %s", upload_obj_checksum)
        await self.list_parts(response["UploadId"], mpart_bucket, s3mpart_object)
        await self.list_multipart_uploads(mpart_bucket)
        await self.complete_multipart_upload(
            response["UploadId"], parts, mpart_bucket, s3mpart_object
        )
        self.log.info("'s3://%s/%s' uploaded successfully.", mpart_bucket, s3mpart_object)
        return upload_obj_checksum
