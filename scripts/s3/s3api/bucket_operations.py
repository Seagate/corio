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

from src.commons.constants import MIN_DURATION
from src.libs import IAMClient
from src.libs.s3api import S3Api


# pylint: disable=too-many-ancestors
class TestBucketOps(S3Api):
    """S3 Bucket Operations class for executing given io stability workload."""

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        endpoint_url: str,
        test_id: str,
        **kwargs,
    ) -> None:
        """
        s3 bucket operations init class.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :param test_id: Test ID string.
        :keyword use_ssl: To use secure connection.
        :keyword object_size: Object size to be used for bucket operation
        :keyword seed: Seed to be used for random data generator
        :keyword session: session name.
        :keyword duration: Duration timedelta object, if not given will run for 100 days.
        # Type 5:
        :keyword number_of_buckets: Number of buckets to be created.
        # Type 2:
        :keyword number_of_objects: Number of objects per iterations.
        """
        super().__init__(
            access_key,
            secret_key,
            endpoint_url=endpoint_url,
            use_ssl=kwargs.get("use_ssl"),
            test_id=f"{test_id}_bucket_operations",
        )
        random.seed(kwargs.get("seed"))
        self.test_id = test_id
        self.session_id = kwargs.get("session")
        kwargs["endpoint_url"] = endpoint_url
        self.object_size = kwargs.get("object_size")
        self.kwargs = kwargs
        self.finish_time = datetime.now() + kwargs.get("duration", timedelta(hours=int(100 * 24)))

    # pylint: disable=broad-except, too-many-locals
    async def execute_bucket_workload(self):
        """Execute bucket operations workload for specific duration."""
        try:
            number_of_buckets = self.kwargs.get("number_of_buckets")
            iteration, buckets = 1, []
            bops_obj, user_name = None, None
            if number_of_buckets:
                user_name = f"iam-user-{self.test_id.lower()}-{perf_counter_ns()}"
                response = await self.create_s3iam_user(user_name)
                bops_obj = S3Api(
                    access_key=response["AccessKey"]["AccessKeyId"],
                    secret_key=response["AccessKey"]["SecretAccessKey"],
                    endpoint_url=self.kwargs.get("endpoint_url"),
                    use_ssl=self.kwargs.get("use_ssl"),
                    test_id=f"{self.test_id}_bucket_operations",
                )
                buckets = await self.create_number_of_buckets(bops_obj, number_of_buckets)
            while True:
                self.log.info("Iteration %s is started for %s", iteration, self.session_id)
                if isinstance(self.object_size, dict):
                    file_size = random.randrange(
                        self.object_size["start"], self.object_size["end"]
                    )  # nosec
                else:
                    file_size = self.object_size
                if number_of_buckets:
                    bucket_name = random.choice(buckets)  # nosec
                    file_name = f"object-{self.test_id.lower()}-{perf_counter_ns()}"
                    file_path = corio_utils.create_file(file_name, file_size)
                    sha256_in = bops_obj.checksum_file(file_path)
                    self.s3_url = bops_obj.s3_url
                    await bops_obj.upload_object(bucket_name, file_name, file_path=file_path)
                    os.remove(file_path)
                    await bops_obj.download_object(bucket_name, file_name, file_path)
                    sha256_out = bops_obj.checksum_file(file_path)
                    if sha256_in != sha256_out:
                        raise AssertionError(
                            f"Failed to match checksum for {bops_obj.s3_url}. "
                            f"Input file checksum: {sha256_in}"
                            f"Output file checksum: {sha256_out}"
                        )
                else:
                    bucket_name = (
                        f"bucket-op-{self.test_id.lower()}-iter{iteration}-{perf_counter_ns()}"
                    )
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
                self.log.info("Iteration %s is completed of %s", iteration, self.session_id)
                if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                    if bops_obj:
                        for bucket in await bops_obj.list_buckets():
                            await bops_obj.delete_bucket(bucket, force=True)
                    if user_name:
                        await self.delete_s3iam_user(user_name)
                    return True, "Bucket operation execution completed successfully."
                iteration += 1
        except Exception as err:
            self.log.exception("bucket url: {%s}\nException: {%s}", self.s3_url, err)
            assert False, f"bucket url: {self.s3_url}\nException: {err}"

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

    async def create_number_of_buckets(self, bkt_ops_obj, number_of_buckets):
        """Create s3 buckets as per number_of_buckets."""
        bucket_list = []
        for _ in range(number_of_buckets):
            bucket_name = bkt_ops_obj.get_bucket_name(bucket_list)
            self.log.info(bucket_name)
            self.s3_url = f"s3://{bucket_name}"
            await bkt_ops_obj.create_bucket(bucket_name)
            bucket_list.append(bucket_name)
        return bucket_list

    async def create_s3iam_user(self, user_name):
        """Create s3 iam user using rest or boto api."""
        try:
            response = await self.create_iam_user(user_name)
        except Exception as err:
            self.log.warning(err)
            response = IAMClient().create_user(user_name)
        self.log.info(response)
        return response

    async def delete_s3iam_user(self, user_name):
        """Delete s3 iam user using rest or boto api."""
        try:
            response = await self.delete_iam_user(user_name)
        except Exception as err:
            self.log.warning(err)
            response = IAMClient().delete_user(user_name)
        self.log.info(response)
        return response
