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

"""Python Library to perform bucket operations using aiobotocore module."""
import random
import string
import time

from src.commons.utils.corio_utils import retries
from src.libs.s3api.s3_restapi import S3RestApi


class S3Bucket(S3RestApi):
    """Class for bucket operations."""

    def __init__(self, *args, **kwargs):
        """Initialize for S3Bucket operations."""
        super().__init__(*args, **kwargs)
        self.s3_url = None

    @retries()
    async def create_bucket(self, bucket_name: str) -> dict:
        """
        Create Bucket.

        :param bucket_name: Name of the bucket.
        :return: Response of create bucket.
        """
        async with self.get_client() as client:
            self.s3_url = f"s3://{bucket_name}"
            response = await client.create_bucket(Bucket=bucket_name)
            self.log.info("create_bucket:%s, Response: %s", bucket_name, response)

        return response

    @retries()
    async def list_buckets(self) -> list:
        """
        List all the buckets.

        :return: Response of bucket list.
        """
        async with self.get_client() as client:
            buckets = await client.list_buckets()
            response = [bucket["Name"] for bucket in buckets["Buckets"]]
            self.log.info("list_buckets: Response: %s", response)

        return response

    @retries()
    async def head_bucket(self, bucket_name: str) -> dict:
        """
        To determine if a bucket exists and have a permission to access it.

        :param bucket_name: Name of the bucket.
        :return: Response of head bucket.
        """
        async with self.get_client() as client:
            self.s3_url = f"s3://{bucket_name}"
            response = await client.head_bucket(Bucket=bucket_name)
            self.log.info("head_bucket: %s, Response: %s", bucket_name, response)

        return response

    @retries()
    async def get_bucket_location(self, bucket_name: str) -> dict:
        """
        Get Bucket Location.

        :param bucket_name: Name of the bucket.
        :return: Response of bucket location.
        """
        async with self.get_client() as client:
            self.s3_url = f"s3://{bucket_name}"
            response = await client.get_bucket_location(Bucket=bucket_name)
            self.log.info(
                "get_bucket_location: %s, Response: %s", bucket_name, response
                )

        return response

    @retries()
    async def delete_bucket(self, bucket_name: str, force: bool = False) -> dict:
        """
        Delete the empty bucket or deleting the buckets along with objects stored in it.

        :param bucket_name: Name of the bucket.
        :param force: Value for delete bucket with object or without object.
        :return: Response of delete bucket.
        """
        async with self.get_client() as client:
            if force:
                self.log.info(
                    "This might cause data loss as you have opted for bucket deletion"
                    " with objects in it"
                    )
                # list s3 objects using paginator
                paginator = client.get_paginator("list_objects")
                async for result in paginator.paginate(Bucket=bucket_name):
                    for content in result.get("Contents", []):
                        self.s3_url = f"s3://{bucket_name}/{content['Key']}"
                        resp = await client.delete_object(
                            Bucket=bucket_name, Key=content["Key"]
                            )
                        self.log.debug(resp)
                self.log.info("All objects deleted successfully.")
            self.s3_url = f"s3://{bucket_name}"
            response = await client.delete_bucket(Bucket=bucket_name)
            self.log.info(
                "Bucket '%s' deleted successfully. Response: %s", bucket_name, response
                )

        return response

    @retries()
    async def create_n_buckets(self, bucket_prefix: str, bucket_count: int) -> list:
        """
        Create N number of s3 Bucket as per bucket count.

        :param bucket_prefix: Prefix of the bucket.
        :param bucket_count: Number of buckets to create.
        :return: List of buckets.
        """
        bucket_list = []
        for i in range(bucket_count):
            bucket_name = f"{bucket_prefix}-{i}-{time.perf_counter_ns()}"
            await self.create_bucket(bucket_name)
            bucket_list.append(bucket_name)
        return bucket_list

    @retries(asyncio=False)
    def create_s3_bucket(self, bucket_name: str = None) -> dict:
        """
        Create s3 bucket.

        :param bucket_name: Name of the bucket.
        :return: response.
        """
        response = self.get_boto3_client().create_bucket(Bucket=bucket_name)
        self.log.info("Bucket: %s, Response: %s", bucket_name, response)

        return response

    @retries(asyncio=False)
    def list_s3_buckets(self) -> list:
        """
        List all s3 buckets.

        :return: response.
        """
        response = self.get_boto3_client().list_buckets()
        self.log.debug("Response: %s", response)
        bucket_list = [bucket["Name"] for bucket in response["Buckets"]]
        self.log.info("List s3 buckets: %s", bucket_list)
        return bucket_list

    @retries(asyncio=False)
    def delete_s3_bucket(self, bucket_name: str = None, force: bool = False) -> dict:
        """
        Delete the empty bucket or delete the bucket along with objects stored in it.

        :param bucket_name: Name of the bucket.
        :param force: Value for delete bucket with object or without object.
        :return: response.
        """
        self.s3_url = f"s3://{bucket_name}"
        bucket = self.get_boto3_resource().Bucket(bucket_name)
        if force:
            self.log.info(
                "This might cause data loss as you have opted for bucket deletion with "
                "objects in it"
                )
            response = bucket.objects.all().delete()
            self.log.debug(
                "Objects deleted successfully from bucket %s, response: %s",
                bucket_name,
                response,
                )
        response = bucket.delete()
        self.log.debug(
            "Bucket '%s' deleted successfully. Response: %s", bucket_name, response
            )

        return response

    @staticmethod
    def get_bucket_name():
        """
        Generate a valid bucket name string.

        first letter should be a number or lowercase letter, rest letters can include number,
         lowercase, hyphens and dots. bucket length can vary from 3 to 63.
        """
        return "".join(
            random.SystemRandom().choice(string.ascii_lowercase + string.digits)
            + "".join(
                random.SystemRandom().choice(
                    string.ascii_lowercase + string.digits + "." + "-"
                    )
                for _ in range(random.randint(2, 63))
                )
            )
