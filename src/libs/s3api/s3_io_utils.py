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

import asyncio
import os
import random
from collections import Counter
from itertools import cycle
from random import shuffle
from time import perf_counter_ns

from src.commons.utils import corio_utils
from src.libs.s3api import S3Api


class S3ApiIOUtils(S3Api):
    """Utils for s3api."""

    def __int__(self, *args, **kwargs):
        """Constructor for s3 utils."""

        super().__init__(*args, **kwargs)
        seed = kwargs.get("seed")
        if seed:
            random.seed(kwargs.get("seed"))

    def distribution_of_buckets_objects_per_session(self, bucket_list: list, object_count: int,
                                                    sessions: int) -> dict:
        """
        Get the objects per bucket and buckets per session distribution dictionary.

        I/P: bucket_list=["abc", "bcd", "cde"], object_count=500, sessions=5
        O/P: {1: [{'bucket_name': 'cde', 'object_count': 500}],
             2: [{'bucket_name': 'abc', 'object_count': 250}],
             3: [{'bucket_name': 'bcd', 'object_count': 250}],
             4: [{'bucket_name': 'abc', 'object_count': 250}],
             5: [{'bucket_name': 'bcd', 'object_count': 250}]}
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
                distribution[i] = [{"bucket_name": bucket_name, "object_count": round(
                    object_count / ctr_itr[bucket_name])}]
                buckets[bucket_name] = 1 if bucket_name not in buckets else buckets.get(
                    bucket_name, 0) + 1
        else:
            ctr_itr = Counter([next(bkt_itr) for _ in range(len(bucket_list))])
            for _ in range(len(ctr_itr)):
                bucket_name = next(bkt_itr)
                while bucket_name in buckets and ctr_itr[bucket_name] in [1, buckets[bucket_name]]:
                    bucket_name = next(bkt_itr)
                session = next(sess_itr)
                if session in distribution:
                    distribution[session].append({"bucket_name": bucket_name, "object_count": round(
                        object_count / ctr_itr[bucket_name])})
                else:
                    distribution[session] = [{"bucket_name": bucket_name, "object_count": round(
                        object_count / ctr_itr[bucket_name])}]
                buckets[bucket_name] = 1 if bucket_name not in buckets else buckets.get(
                    bucket_name, 0) + 1
        del buckets, bkt_itr, ctr_itr, sess_itr
        self.log.debug(distribution)
        return distribution

    def put_delete_distribution(self, distribution: dict, delete_obj_percent: int,
                                put_object_percent: int) -> None:
        """
        Modify distribution dict with delete object percentage and put object percentage.

        I/P: distribution= {1: [{'bucket_name': 'cde', 'object_count': 500}],
                             2: [{'bucket_name': 'abc', 'object_count': 250}],
                             3: [{'bucket_name': 'bcd', 'object_count': 250}],
                             4: [{'bucket_name': 'abc', 'object_count': 250}],
                             5: [{'bucket_name': 'bcd', 'object_count': 250}]},
            delete_obj_percent=10, put_object_percent=10
        O/P:
            {1: [{'bucket_name': 'cde',
                  'delete_object_count': 50,
                  'object_count': 500,
                  'put_object_count': 50}],
             2: [{'bucket_name': 'abc',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25}],
             3: [{'bucket_name': 'bcd',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25}],
             4: [{'bucket_name': 'abc',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25}],
             5: [{'bucket_name': 'bcd',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25}]}
        :param distribution: Distribution of buckets, objects per sessions.
        :param delete_obj_percent: Delete percentage of total objects.
        :param put_object_percent: Put object percentage of total objects.
        """
        for _, value in distribution.items():
            for ele in value:
                ele["delete_object_count"] = int(ele["object_count"] * delete_obj_percent / 100)
                ele["put_object_count"] = int(ele["object_count"] * put_object_percent / 100)
        self.log.debug(distribution)

    async def schedule_sessions(self, tasks):
        """Schedule session for function as per sessions."""
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
        if pending:
            self.log.critical("Terminating pending task: %s", pending)
            for task in pending:
                task.cancel()
        self.log.info(done)
        for task in done:
            task.result()

    # pylint: disable=broad-except
    def create_sessions(self, func, *args, **kwargs):
        """
        Start workload execution.

        :param func: Name of the function.
        """
        self.log.info("Execution started for %s", func.__name__)
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        try:
            new_loop.run_until_complete(func(*args, **kwargs))
        except Exception as error:
            self.log.exception(error)
            if new_loop.is_running():
                new_loop.stop()
            raise error from Exception
        finally:
            if not new_loop.is_closed():
                new_loop.close()
        self.log.info("Execution completed for %s", func.__name__)

    def get_object_size(self, object_size) -> int:
        """get the object size in bytes."""
        if isinstance(object_size, list):
            file_size = object_size[random.randrange(0, len(object_size))]
        elif isinstance(object_size, dict):
            file_size = random.randrange(object_size["start"], object_size["end"])
        else:
            file_size = object_size
        self.log.debug(file_size)
        return file_size

    async def write_data(self, distribution, object_size):
        """Write distribution data to s3."""
        tasks = []

        async def put_data(data, bucket_name, object_count, objsize):
            """Upload n number of objects to s3 bucket."""
            data["files"] = []
            for cnt in range(1, object_count + 1):
                file_size = self.get_object_size(objsize)
                file_name = f"obj-{bucket_name}-{cnt}-{file_size}-{perf_counter_ns()}"
                file_path = corio_utils.create_file(file_name, file_size)
                await self.upload_object(bucket_name, key=file_name, file_path=file_path)
                os.remove(file_path)
                data["files"].append(file_path)

        for _, values in distribution.items():
            for value in values:
                tasks.append(
                    put_data(value, value["bucket_name"], value["object_count"], object_size))
        await self.schedule_sessions(tasks)

    async def read_data(self, distribution):
        """Read distribution data from s3."""
        tasks = []

        async def read_data(data):
            """Upload n number of objects to s3 bucket."""
            for file_name in data["files"]:
                await self.get_object(data["bucket_name"], file_name)

        for _, values in distribution.items():
            for value in values:
                tasks.append(read_data(value))
        await self.schedule_sessions(tasks)

    async def delete_distribution_data(self, distribution):
        """Read distribution data from s3."""
        tasks = []

        async def delete_data(data):
            """Upload n number of objects to s3 bucket."""
            file_list = data.get("files", [])
            shuffle(file_list)
            file_iter = iter(cycle(file_list))
            for _ in range(1, data["delete_object_count"]+1):
                file_name = next(file_iter, "")
                if file_name:
                    await self.delete_object(data["bucket_name"], file_name)
                    if file_name in data["files"]:
                        data["files"].remove(file_name)
                else:
                    break

        for _, values in distribution.items():
            for value in values:
                tasks.append(delete_data(value))
        await self.schedule_sessions(tasks)

    async def write_distribution_data(self, distribution, object_size):
        """Write distribution data to s3."""
        tasks = []

        async def put_data(data, bucket_name, object_count, objsize):
            """Upload n number of objects to s3 bucket."""
            for cnt in range(1, object_count + 1):
                file_size = self.get_object_size(objsize)
                file_name = f"obj-{bucket_name}-{cnt}-{file_size}-{perf_counter_ns()}"
                file_path = corio_utils.create_file(file_name, file_size)
                await self.upload_object(bucket_name, key=file_name, file_path=file_path)
                os.remove(file_path)
                data["files"].append(file_path)

        for _, values in distribution.items():
            for value in values:
                tasks.append(
                    put_data(value, value["bucket_name"], value["put_object_count"], object_size))
        await self.schedule_sessions(tasks)

    async def cleanup_data(self, distribution: dict) -> None:
        """Delete s3 buckets in parallel forcefully by default."""

        async def delete_buckets(buckets):
            """Delete s3 buckets."""
            for bucket in buckets:
                await self.delete_bucket(bucket, force=True)

        tasks = []
        for _, values in distribution.items():
            bucket_list = []
            for value in values:
                bucket_list.append(value["bucket_name"])
            tasks.append(delete_buckets(bucket_list))
        await self.schedule_sessions(tasks)
