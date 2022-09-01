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
"""S3api IO utility."""

import os
import random
from collections import Counter
from itertools import cycle
from random import shuffle
from time import perf_counter_ns

from src.commons.utils import corio_utils
from src.commons.utils.asyncio_utils import run_event_loop_until_complete
from src.commons.utils.asyncio_utils import schedule_tasks
from src.libs.s3api import S3Api


class S3ApiIOUtils(S3Api):
    """Utils for s3api."""

    def __int__(self, *args, **kwargs):
        """Init for s3 utils."""
        super().__init__(*args, **kwargs)
        seed = kwargs.get("seed")
        if seed:
            random.seed(kwargs.get("seed"))

    def distribution_of_buckets_objects_per_session(
        self, bucket_list: list, object_count: int, sessions: int
    ) -> dict:
        """
        Get the objects per bucket and buckets per session distribution dictionary.

        I/P: bucket_list=["abc", "bcd", "cde"], object_count=500, sessions=5
        O/P: {'session1': [{'bucket_name': 'cde', 'object_count': 500}],
             'session2': [{'bucket_name': 'abc', 'object_count': 250}],
             'session3': [{'bucket_name': 'bcd', 'object_count': 250}],
             'session4': [{'bucket_name': 'abc', 'object_count': 250}],
             'session5': [{'bucket_name': 'bcd', 'object_count': 250}]}
        :param bucket_list: List of s3 buckets.
        :param object_count: Number of s3 object count.
        :param sessions: Number of sessions.
        :return: Distribution dict.
        """
        distribution, buckets = {}, {}
        bkt_itr, sess_itr = iter(cycle(bucket_list)), iter(cycle(range(sessions)))
        if sessions > len(bucket_list):
            ctr_itr = Counter([next(bkt_itr) for _ in range(sessions)])
            for i in range(1, sessions + 1):
                bucket_name = next(bkt_itr)
                while bucket_name in buckets and buckets[bucket_name] == ctr_itr[bucket_name]:
                    bucket_name = next(bkt_itr)
                distribution[f"session{i}"] = [
                    {
                        "bucket_name": bucket_name,
                        "object_count": round(object_count / ctr_itr[bucket_name]),
                    }
                ]
                buckets[bucket_name] = (
                    1 if bucket_name not in buckets else buckets.get(bucket_name, 1) + 1
                )
        else:
            ctr_itr = Counter([next(bkt_itr) for _ in range(len(bucket_list))])
            for _ in range(len(ctr_itr)):
                bucket_name = next(bkt_itr)
                while bucket_name in buckets and ctr_itr[bucket_name] in [1, buckets[bucket_name]]:
                    bucket_name = next(bkt_itr)
                session = f"session{next(sess_itr)}"
                if session in distribution:
                    distribution[session].append(
                        {
                            "bucket_name": bucket_name,
                            "object_count": round(object_count / ctr_itr[bucket_name]),
                        }
                    )
                else:
                    distribution[session] = [
                        {
                            "bucket_name": bucket_name,
                            "object_count": round(object_count / ctr_itr[bucket_name]),
                        }
                    ]
                buckets[bucket_name] = (
                    1 if bucket_name not in buckets else buckets.get(bucket_name, 0) + 1
                )
        del buckets, bkt_itr, ctr_itr, sess_itr
        self.log.info("Distribution of objects per buckets per sessions: %s", distribution)
        return distribution

    def generate_objects_distribution(
        self,
        distribution: dict,
        delete_obj_percent: int = 0,
        put_object_percent: int = 0,
        overwrite_object_percent: int = 0,
        **kwargs,
    ) -> None:
        """
        Modify/generate objects distribution as per put, delete, overwrite object percentage.

        I/P: distribution= {1: [{'bucket_name': 'cde', 'object_count': 500}],
                             2: [{'bucket_name': 'abc', 'object_count': 250}],
                             3: [{'bucket_name': 'bcd', 'object_count': 250}],
                             4: [{'bucket_name': 'abc', 'object_count': 250}],
                             5: [{'bucket_name': 'bcd', 'object_count': 250}]},
            delete_obj_percent=10, put_object_percent=10, overwrite_object_percent=10,
            read_percentage_per_bucket=10
        O/P:
            {"session1": [{'bucket_name': 'cde',
                  'delete_object_count': 50,
                  'object_count': 500,
                  'put_object_count': 50,
                  'overwrite_object_count':50,
                  'read_object_count':50}],
             2: [{'bucket_name': 'abc',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25,
                  'overwrite_object_count':25,
                  'read_object_count':25}],
             3: [{'bucket_name': 'bcd',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25,
                  'overwrite_object_count':25,
                  'read_object_count':25}],
             4: [{'bucket_name': 'abc',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25,
                  'overwrite_object_count':25,
                  'read_object_count':25}],
             5: [{'bucket_name': 'bcd',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25,
                  'overwrite_object_count':25,
                  'read_object_count':25}]}
        :param distribution: Distribution of buckets, objects per sessions.
        :param delete_obj_percent: Delete object percentage per bucket of total objects.
        :param put_object_percent: Put object percentage per bucket of total objects.
        :param overwrite_object_percent : Overwrite object percentage per bucket of total objects.
        :keyword read_percentage_per_bucket: Read object percentage per bucket of total objects.
        """
        read_percentage_per_bucket = kwargs.get("read_percentage_per_bucket", 0)
        for _, value in distribution.items():
            for ele in value:
                if delete_obj_percent:
                    ele["delete_object_count"] = int(ele["object_count"] * delete_obj_percent / 100)
                if put_object_percent:
                    ele["put_object_count"] = int(ele["object_count"] * put_object_percent / 100)
                if overwrite_object_percent:
                    ele["overwrite_object_count"] = int(
                        ele["object_count"] * overwrite_object_percent / 100
                    )
                if read_percentage_per_bucket:
                    ele["read_object_count"] = int(
                        ele["object_count"] * read_percentage_per_bucket / 100
                    )
        self.log.info(
            "Distribution of percentage(read/write/delete) of objects per buckets per sessions: %s",
            distribution,
        )

    def starts_sessions(self, func, *args, **kwargs):
        """Start workload execution on s3 bucket as per distribution data."""
        self.log.info("Execution started for %s", func.__name__)
        run_event_loop_until_complete(self.log, func, *args, **kwargs)
        self.log.info("Execution completed for %s", func.__name__)

    def get_object_size(self, object_size) -> int:
        """Get the object size in bytes."""
        if isinstance(object_size, list):
            file_size = object_size[random.randrange(0, len(object_size))]  # nosec
        elif isinstance(object_size, dict):
            file_size = random.randrange(object_size["start"], object_size["end"])  # nosec
        else:
            file_size = object_size
        self.log.debug(file_size)
        return file_size

    async def write_data(self, distribution, object_size):
        """Write given percentage of object distribution data to s3 bucket."""
        tasks = []

        async def put_data(data, bucket_name, object_count, objsize):
            """Upload n number of objects to s3 bucket."""
            data["files"] = {}
            for _ in range(object_count):
                file_size = self.get_object_size(objsize)
                file_name = f"s3object-{file_size}bytes-{perf_counter_ns()}"
                file_path = corio_utils.create_file(file_name, file_size)
                checksum_in = self.checksum_file(file_path)
                self.s3_url = f"s3://{bucket_name}/{file_name}"
                response = await self.upload_object(bucket_name, key=file_name, file_path=file_path)
                data["files"][file_name] = {
                    "s3url": self.s3_url,
                    "key_size": file_size,
                    "key_checksum": checksum_in,
                    "bucket": bucket_name,
                    "key": file_name,
                    "etag": response["ETag"],
                }
                self.remove_file(file_path)

        for _, values in distribution.items():
            for value in values:
                tasks.append(
                    put_data(value, value["bucket_name"], value["object_count"], object_size)
                )
        await schedule_tasks(self.log, tasks)

    async def read_data(self, distribution, validate=True):
        """Read & validate given percentage of object distribution data from s3 bucket."""
        tasks = []

        async def read_data(data):
            """Read n number of objects from s3 bucket."""
            for file_name in data["files"]:
                response = await self.get_object(data["bucket_name"], file_name)
                if validate:
                    assert (
                        data["files"][file_name]["etag"] == response["ETag"]
                    ), f"Failed to match ETag for {file_name}"

    for _, values in distribution.items():
            for value in values:
                tasks.append(read_data(value))
        await schedule_tasks(self.log, tasks)

    async def read_distribution_data(self, distribution):
        """Read object distribution per s3 bucket in parallel."""
        tasks = []

        async def read_data(data):
            """Read n number of objects randomly from s3 bucket."""
            file_list = data.get("files", [])
            shuffle(file_list)
            file_iter = iter(cycle(file_list))
            for _ in range(data["read_object_count"]):
                file_name = next(file_iter)
                await self.get_object(data["bucket_name"], file_name)

        for _, values in distribution.items():
            for value in values:
                tasks.append(read_data(value))
        await schedule_tasks(self.log, tasks)

    async def delete_distribution_data(self, distribution):
        """Delete given percentage of object distribution data randomly from s3 bucket."""
        tasks = []

        async def delete_data(data):
            """Delete n number of objects randomly from s3 bucket."""
            file_list = data["files"].keys()
            shuffle(file_list)
            file_iter = iter(cycle(file_list))
            for _ in range(data["delete_object_count"]):
                file_name = next(file_iter, "")
                if file_name and file_name in data["files"]:
                    await self.delete_object(data["bucket_name"], file_name)
                    data["files"].pop(file_name, "")
                else:
                    self.log.warning("File '%s' does not exists.", file_name)

        for _, values in distribution.items():
            for value in values:
                tasks.append(delete_data(value))
        await schedule_tasks(self.log, tasks)

    async def write_distribution_data(self, distribution, object_size):
        """Write given percentage of object distribution to a s3 bucket."""
        tasks = []

        async def put_data(data, bucket_name, object_count, objsize):
            """Upload n number of objects to s3 bucket."""
            for _ in range(object_count):
                file_size = self.get_object_size(objsize)
                file_name = f"s3object-{file_size}bytes-{perf_counter_ns()}"
                file_path = corio_utils.create_file(file_name, file_size)
                checksum_in = self.checksum_file(file_path)
                self.s3_url = f"s3://{bucket_name}/{file_name}"
                response = await self.upload_object(bucket_name, key=file_name, file_path=file_path)
                self.remove_file(file_path)
                data["files"][file_name] = {
                    "s3url": self.s3_url,
                    "key_size": file_size,
                    "key_checksum": checksum_in,
                    "bucket": bucket_name,
                    "key": file_name,
                    "etag": response["ETag"],
                }

        for _, values in distribution.items():
            for value in values:
                tasks.append(
                    put_data(value, value["bucket_name"], value["put_object_count"], object_size)
                )
        await schedule_tasks(self.log, tasks)

    async def cleanup_data(self, distribution: dict) -> None:
        """Delete given percentage of objects from s3 buckets in parallel forcefully by default."""
        tasks = []

        async def delete_buckets(buckets):
            """Delete s3 buckets along with objects."""
            for bucket in buckets:
                await self.delete_bucket(bucket, force=True)

        for _, values in distribution.items():
            bucket_list = []
            for value in values:
                bucket_list.append(value["bucket_name"])
            tasks.append(delete_buckets(bucket_list))
        await schedule_tasks(self.log, tasks)

    @staticmethod
    def get_random_sleep_time(delay) -> int:
        """Get the random delay time from dict/list/tuple/int in seconds."""
        if isinstance(delay, dict):
            sleep_time = random.randrange(delay["start"], delay["end"])  # nosec
        elif isinstance(delay, (list, tuple)):
            sleep_time = random.choice(delay)  # nosec
        else:
            sleep_time = delay
        return sleep_time

    async def overwrite_distribution_data(self, distribution, object_size, validate=True) -> None:
        """Overwrite given percentage of total objects in a given s3 bucket."""
        tasks = []

        async def overwrite_read_data(data, bucket_name, object_count, objsize):
            """Overwrite and read same number of objects from s3 bucket."""
            for _ in range(object_count):
                file_name = random.choice(data["files"])  # nosec
                file_size = self.get_object_size(objsize)
                file_path = corio_utils.create_file(file_name, file_size)
                checksum_in = self.checksum_file(file_path)
                self.s3_url = f"s3://{bucket_name}/{file_name}"
                response = await self.upload_object(bucket_name, key=file_name, file_path=file_path)
                self.remove_file(file_path)
                data["files"][file_name] = {
                    "s3url": self.s3_url,
                    "key_size": file_size,
                    "key_checksum": checksum_in,
                    "bucket": bucket_name,
                    "key": file_name,
                    "etag": response["ETag"],
                }
                response = await self.get_object(bucket_name, file_name)
                if validate:
                    if validate:
                        assert data["files"][file_name]["etag"] == response["ETag"], (
                            f"Failed to match ETag for {file_name}: data: "
                            f"{data['files'][file_name]}, response: {response}"
                        )

        for _, values in distribution.items():
            for value in values:
                tasks.append(
                    overwrite_read_data(
                        value, value["bucket_name"], value["overwrite_object_count"], object_size
                    )
                )
        await schedule_tasks(self.log, tasks)

    def remove_file(self, file_path: str) -> None:
        """Remove the existing file."""
        if os.path.exists(file_path):
            os.remove(file_path)
        else:
            self.log.warning("File '%s' does not exists.", file_path)
