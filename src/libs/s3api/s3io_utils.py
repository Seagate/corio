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
from typing import Union

from src.commons.utils.async_io import run_event_loop_until_complete
from src.commons.utils.async_io import schedule_tasks
from src.libs.s3api import S3Api


class S3ApiIOUtils(S3Api):
    """Utils for s3api."""

    def __int__(self, *args, **kwargs):
        """Init for s3 utils."""
        super().__init__(*args, **kwargs)
        seed = kwargs.get("seed")
        if seed:
            random.seed(seed)

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
                    1 if bucket_name not in buckets else buckets.get(bucket_name, 0) + 1
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

        I/P: distribution= {"session1": [{'bucket_name': 'cde', 'object_count': 500}],
                             "session2": [{'bucket_name': 'abc', 'object_count': 250}],
                             "session3": [{'bucket_name': 'bcd', 'object_count': 250}],
                             "session4": [{'bucket_name': 'abc', 'object_count': 250}],
                             "session5": [{'bucket_name': 'bcd', 'object_count': 250}]},
            delete_obj_percent=10, put_object_percent=10, overwrite_object_percent=10,
            read_percentage_per_bucket=10
        O/P:
            {"session1": [{'bucket_name': 'cde',
                  'delete_object_count': 50,
                  'object_count': 500,
                  'put_object_count': 50,
                  'overwrite_object_count':50,
                  'read_object_count':50}],
             "session2": [{'bucket_name': 'abc',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25,
                  'overwrite_object_count':25,
                  'read_object_count':25}],
             "session3": [{'bucket_name': 'bcd',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25,
                  'overwrite_object_count':25,
                  'read_object_count':25}],
             "session4": [{'bucket_name': 'abc',
                  'delete_object_count': 25,
                  'object_count': 250,
                  'put_object_count': 25,
                  'overwrite_object_count':25,
                  'read_object_count':25}],
             "session5": [{'bucket_name': 'bcd',
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
                    ele["delete_object_count"] = round(
                        ele["object_count"] * delete_obj_percent / 100
                    )
                if put_object_percent:
                    ele["put_object_count"] = round(ele["object_count"] * put_object_percent / 100)
                if overwrite_object_percent:
                    ele["overwrite_object_count"] = round(
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

    def starts_sessions(self, func, *args, **kwargs) -> None:
        """Start workload execution on s3 bucket as per distribution data."""
        self.log.info("Execution started for %s", func.__name__)
        run_event_loop_until_complete(self.log, func, *args, **kwargs)
        self.log.info("Execution completed for %s", func.__name__)

    def get_object_size(self, object_size: Union[list, dict, int]) -> int:
        """Get the object size in bytes."""
        if isinstance(object_size, list):
            file_size = object_size[random.randrange(0, len(object_size))]  # nosec
        elif isinstance(object_size, dict):
            file_size = random.randrange(object_size["start"], object_size["end"])  # nosec
        else:
            file_size = object_size
        self.log.debug(file_size)
        return file_size

    async def write_data(self, distribution: dict, object_size: Union[list, dict, int]) -> None:
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

    async def read_all_data(self, distribution: dict, validate=True) -> None:
        """Read & validate given percentage of object distribution data from s3 bucket."""
        tasks = []

        async def read_data(data: dict) -> None:
            """Read n number of objects from s3 bucket."""
            for file_name in data["files"]:
                response = await self.get_object(data["bucket_name"], file_name)
                if validate:
                    if data["files"][file_name]["etag"] != response["ETag"]:
                        raise AssertionError(
                            f"Failed to match ETag for {file_name}: response: " f"{response}"
                        )
                    self.log.info(f"Matched ETag for {file_name}")

        for _, values in distribution.items():
            for value in values:
                tasks.append(read_data(value))
        await schedule_tasks(self.log, tasks)

    async def read_distribution_data(self, distribution, validate=True):
        """Read object distribution per s3 bucket in parallel."""
        tasks = []

        async def read_data(data):
            """Read n number of objects randomly from s3 bucket."""
            file_list = data.get("files", [])
            shuffle(file_list)
            file_iter = iter(cycle(file_list))
            for _ in range(data["read_object_count"]):
                file_name = next(file_iter)
                response = await self.get_object(data["bucket_name"], file_name)
                if validate:
                    if data["files"][file_name]["etag"] != response["ETag"]:
                        raise AssertionError(
                            f"Failed to match ETag for {file_name}: response: " f"{response}"
                        )
                    self.log.info(f"Matched ETag for {file_name}")

        for _, values in distribution.items():
            for value in values:
                tasks.append(read_data(value))
        await schedule_tasks(self.log, tasks)

    async def delete_distribution_data(self, distribution: dict) -> None:
        """Delete given percentage of object distribution data randomly from s3 bucket."""
        tasks = []

        async def delete_data(data: dict) -> None:
            """Delete n number of objects randomly from s3 bucket."""
            file_list = list(data["files"].keys())
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

    async def write_distribution_data(
        self, distribution: dict, object_size: Union[list, dict, int]
    ) -> None:
        """Write given percentage of object distribution to a s3 bucket."""
        tasks = []

        async def put_data(
            data: dict, bucket_name: str, object_count: int, objsize: Union[list, dict, int]
        ) -> None:
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

    async def cleanup_data(self, buckets: list) -> None:
        """Delete all buckets from s3 in parallel forcefully by default."""
        tasks = []

        async def delete_buckets(bucket_name: str) -> None:
            """Delete s3 buckets along with objects."""
            await self.delete_bucket(bucket_name, force=True)

        for bucket in buckets:
            tasks.append(delete_buckets(bucket))
        await schedule_tasks(self.log, tasks)

    @staticmethod
    def get_random_sleep_time(delay: Union[list, dict, int]) -> int:
        """Get the random delay time from dict/list/tuple/int in seconds."""
        if isinstance(delay, dict):
            sleep_time = random.randrange(delay["start"], delay["end"])  # nosec
        elif isinstance(delay, (list, tuple)):
            sleep_time = random.choice(delay)  # nosec
        else:
            sleep_time = delay
        return round(sleep_time)

    async def overwrite_distribution_data(
        self, distribution: dict, object_size: Union[list, dict, int], validate=True
    ) -> None:
        """Overwrite given percentage of total objects in a given s3 bucket."""
        tasks = []

        async def overwrite_read_data(
            data: dict, bucket_name: str, object_count: int, objsize: Union[list, dict, int]
        ) -> None:
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
                        if data["files"][file_name]["etag"] != response["ETag"]:
                            raise AssertionError(
                                f"Failed to match ETag for {file_name}: response: " f"{response}"
                            )
                        self.log.info(f"Matched ETag for {file_name}")

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


# pylint: disable=too-many-arguments
class S3ApiParallelIO(S3Api):
    """S3 object operations class for executing given type-1, 3 and 4 io stability workload."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, **kwargs) -> None:
        """
        S3 object operations init.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :param use_ssl: To use secure connection.
        """
        super().__init__(access_key, secret_key, endpoint_url=endpoint_url, **kwargs)
        self.io_ops_dict = {}
        self.read_files = {}
        self.validated_files = {}
        self.deleted_files = {}

    async def read_data(
        self,
        bucket_name: str,
        object_size: int,
        sessions: int,
        object_prefix: str,
        validate=False,
    ) -> None:
        """
        Read data from s3 bucket as per object size.

        Object prefix and number of samples in parallel as per sessions.

        :param bucket_name: Name of the s3 bucket.
        :param object_size: Object size per sample.
        :param sessions: total number of sessions used to read samples.
        :param object_prefix: Object prefix used for read.
        :param validate: Validate the io's.
        """
        self.log.info("Reading data...")
        self.log.info("Object size: %s, Number of samples: %s", object_size, sessions)
        if bucket_name not in self.read_files:
            self.read_files[bucket_name] = {}
            if "keys" not in self.read_files[bucket_name]:
                self.read_files[bucket_name]["keys"] = []
            if "total_count" not in self.read_files[bucket_name]:
                self.read_files[bucket_name]["total_count"] = 0
        if len(self.read_files[bucket_name]["keys"]) + sessions > len(
            self.io_ops_dict[bucket_name]
        ):
            self.read_files[bucket_name]["keys"] = []
        rkey_cntr = len(self.read_files[bucket_name]["keys"])

        async def read_s3object(**kwargs):
            """Read s3 object."""
            self.log.info("Get Object and check data integrity.")
            key = list(self.io_ops_dict[bucket_name].keys())[kwargs.get("cntr")]
            self.log.info("Reading s3 object %s", key)
            if self.io_ops_dict[bucket_name][key]["key_size"] == object_size and key.startswith(
                object_prefix
            ):
                checksum_in = self.io_ops_dict[bucket_name][key]["key_checksum"]
                if validate:
                    if checksum_in != await self.get_s3object_checksum(bucket_name, key):
                        raise AssertionError("Checksum are not equal.")
                else:
                    await self.get_object(bucket_name, key)
                if key not in self.read_files[bucket_name]["keys"]:
                    self.read_files[bucket_name]["keys"].append(key)

        await self.schedule_api_sessions(sessions, read_s3object, cntr=rkey_cntr)
        self.read_files[bucket_name]["total_count"] += sessions
        self.log.info("Reading completed...")

    async def delete_data(
        self, bucket_name: str, object_size: int, sessions: int, object_prefix: str
    ) -> None:
        """
        Delete data from s3 bucket as per object size.

        Object prefix and number of samples in parallel as per sessions.

        :param bucket_name: Name of the bucket.
        :param object_size: Object size per sample.
        :param sessions: total number of sessions used to upload samples.
        :param object_prefix: object prefix used to delete.
        """
        self.log.info("Deleting data...")
        self.log.info("Single object size: %s, Number of samples: %s", object_size, sessions)
        if bucket_name not in self.deleted_files:
            self.deleted_files[bucket_name] = {}
            if "keys" not in self.deleted_files[bucket_name]:
                self.deleted_files[bucket_name]["keys"] = []
            if "total_count" not in self.deleted_files[bucket_name]:
                self.deleted_files[bucket_name]["total_count"] = 0
        dkey_cntr = 0
        if dkey_cntr + sessions > len(self.io_ops_dict[bucket_name]):
            raise AssertionError(
                f"Deletion keys count '{dkey_cntr + sessions}' is greater"
                f" than actual keys '{self.io_ops_dict[bucket_name].keys()}'"
            )

        async def delete_s3object(**kwargs):
            """Delete s3 object."""
            key = list(self.io_ops_dict[bucket_name])[kwargs.get("cntr")]
            if (
                key.startswith(object_prefix)
                and self.io_ops_dict[bucket_name][key]["key_size"] == object_size
            ):
                await self.delete_object(bucket_name, key)
                self.deleted_files[bucket_name]["keys"].append(key)

        await self.schedule_api_sessions(sessions, delete_s3object, cntr=dkey_cntr)
        self.deleted_files[bucket_name]["total_count"] += sessions
        self.log.info("Deletion completed...")

    async def validate_data(
        self, bucket_name: str, object_size: int, sessions: int, object_prefix: str
    ) -> None:
        """
        Validate data from s3 bucket as per object size.

        Object prefix and number of samples in parallel as per sessions.

        :param bucket_name: Name of the s3 bucket.
        :param object_size: Object size per sample.
        :param sessions: total number objects to validate.
        :param object_prefix: object prefix used to validate specific object.
        """
        self.log.info("Validating data...")
        self.log.info(
            "Single object size: %s, Object prefix: %s,  Number of samples: %s",
            object_size,
            object_prefix,
            sessions,
        )
        if bucket_name not in self.validated_files:
            self.validated_files[bucket_name] = {}
            if "keys" not in self.validated_files[bucket_name]:
                self.validated_files[bucket_name]["keys"] = []
            if "total_count" not in self.validated_files[bucket_name]:
                self.validated_files[bucket_name]["total_count"] = 0
        if len(self.validated_files[bucket_name]["keys"]) + sessions > len(
            self.io_ops_dict[bucket_name]
        ):
            self.validated_files[bucket_name]["keys"] = []
        vkey_cntr = len(self.validated_files[bucket_name]["keys"])

        async def validate_s3object(**kwargs):
            """Validate object."""
            key = list(self.io_ops_dict[bucket_name].keys())[kwargs.get("cntr")]
            if (
                key.startswith(object_prefix)
                and self.io_ops_dict[bucket_name][key]["key_size"] == object_size
            ):
                checksum_in = self.io_ops_dict[bucket_name][key]["key_checksum"]
                checksum_dwn = await self.get_s3object_checksum(bucket_name, key)
                assert checksum_in == checksum_dwn, (
                    f"Checksum are not equal for {key}: checksum_in: {checksum_in}, "
                    f"checksum_down: {checksum_dwn}."
                )
                self.log.info("Check sum matched for object %s", key)
                if key not in self.validated_files[bucket_name]["keys"]:
                    self.validated_files[bucket_name]["keys"].append(key)

        await self.schedule_api_sessions(sessions, validate_s3object, cntr=vkey_cntr)
        self.validated_files[bucket_name]["total_count"] += sessions
        self.log.info("Validation completed...")

    async def cleanup_data(self, sessions: int) -> None:
        """
        Delete s3 buckets along with all s3 objects in parallel as per sessions.

        :param sessions: Number of parallel session.
        """
        deleted_buckets = []
        buckets = list(self.io_ops_dict)
        self.log.info("Bucket list: %s", buckets)

        async def delete_bucket(**kwargs):
            """Delete s3 bucket."""
            i = kwargs.get("cntr")
            if len(buckets) >= i:
                bucket_name = buckets[i]
                await self.delete_bucket(bucket_name, force=True)
                deleted_buckets.append(bucket_name)
                del self.io_ops_dict[bucket_name]

        await self.schedule_api_sessions(sessions, delete_bucket)
        self.log.info("Deleted buckets: %s", deleted_buckets)

    async def write_data(
        self, bucket_name: str, object_size: int, object_prefix: str, sessions: int
    ) -> None:
        """
        Write data to s3 bucket as per object size.

        Object prefix and number of samples in parallel as per sessions.

        :param object_prefix: Object name prefix used while creating unique object.
        :param bucket_name: Name of the s3 bucket.
        :param object_size: Object size per sample.
        :param sessions: total number of sessions(samples) used to upload samples.
        """
        self.log.info("Writing data...")
        file_name = f"{object_prefix}-{perf_counter_ns()}"
        file_path = corio_utils.create_file(file_name, object_size)
        self.log.info(
            "Object: '%s', object size: %s, Number of samples: %s",
            object_prefix,
            corio_utils.convert_size(object_size),
            sessions,
        )
        checksum_in = self.checksum_file(file_path)
        self.log.debug("Checksum of '%s' = %s", file_name, checksum_in)
        kcnt = (len(self.io_ops_dict[bucket_name]) if bucket_name in self.io_ops_dict else 0) + 1

        async def upload_s3object(**kwargs):
            """Upload s3 object."""
            key = f"{object_prefix}-{perf_counter_ns()}-{checksum_in}-{kwargs.get('cntr')}"
            self.s3_url = s3_url = f"s3://{bucket_name}/{key}"
            response = await self.upload_object(bucket_name, key, file_path=file_path)
            self.log.info("Uploading s3 object: url: %s", s3_url)
            if bucket_name not in self.io_ops_dict:
                self.io_ops_dict[bucket_name] = {
                    key: {
                        "s3url": s3_url,
                        "key_size": object_size,
                        "key_checksum": checksum_in,
                        "bucket": bucket_name,
                        "key": key,
                        "etag": response["ETag"],
                    }
                }
            else:
                self.io_ops_dict[bucket_name][key] = {
                    "s3url": s3_url,
                    "key_size": object_size,
                    "key_checksum": checksum_in,
                    "bucket": bucket_name,
                    "key": key,
                    "etag": response["ETag"],
                }
            self.log.info("s3://%s/%s uploaded successfully.", bucket_name, key)

        self.log.info(
            "Scheduling to upload file %s, size %s, for samples %s ",
            object_prefix,
            file_path,
            sessions,
        )
        await self.schedule_api_sessions(sessions, upload_s3object, cntr=kcnt)
        os.remove(file_path)

    @staticmethod
    def get_session_distributions(samples, sessions):
        """Get session distributions as per samples."""
        if samples < sessions:
            sessions_distributions = [samples]
        else:
            sessions_distributions = [sessions for _ in range(int(samples / sessions))]
            if samples % sessions:
                sessions_distributions.extend([samples % sessions])
        return sessions_distributions

    async def schedule_api_sessions(self, sessions, func, *args, **kwargs):
        """Schedule session for function as per sessions."""
        tasks = []
        kwargs["cntr"] = kwargs.get("cntr", 0)
        for _ in range(1, sessions + 1):
            tasks.append(func(*args, **kwargs))
            kwargs["cntr"] += 1
        if tasks:
            self.log.info("Scheduling tasks: %s.", tasks)
            await schedule_tasks(self.log, tasks)
            self.log.info("completed tasks: %s.", tasks)

    def create_sessions(self, func, *args, **kwargs):
        """
        Start workload execution.

        :param func: Name of the function.
        """
        self.log.info("Execution started for %s", func.__name__)
        run_event_loop_until_complete(self.log, func, *args, **kwargs)
        self.log.info("Execution completed for %s", func.__name__)

    def get_s3bucket(self, operations: str, bucket_name: str, obj_size: int):
        """Get/Create the s3 io bucket."""
        buckets = [
            bkt
            for bkt in self.list_s3_buckets()
            if (bucket_name == bkt or bkt.startswith(f"iobkt-size{obj_size}-samples"))
        ]
        if operations == "write" and not buckets:
            self.create_s3_bucket(bucket_name)
        else:
            if not buckets:
                raise AssertionError(f"Bucket does not exists: {bucket_name}")
            bucket_name = buckets[-1]
        return bucket_name

    # pylint: disable=too-many-branches, too-many-nested-blocks
    def execute_workload(self, operations, sessions=1, **kwargs):
        """
        Execute s3 workload distribution.

        :param operations: Supported operations are 'write', 'read', 'delete', 'validate' and
        'cleanup' in parallel as per sessions and distribution.
        :param sessions: Number of sessions.
        :keyword distribution: Distribution of object size and number of samples.
            ex: {1024: 115, 2048: 100, 4096: 225}
        :keyword validate: Optional and used in case of read operations.
        :keyword bucket_name: Name of s3 bucket.
            format: "iobkt-size{obj_size}-samples{num_sample}".
        :keyword object_prefix: Object prefix of the s3 object. format: "object-{obj_size}".
        """
        distribution = kwargs.get("distribution")
        if distribution:
            for obj_size, num_sample in distribution.items():
                bucket_name = kwargs.get("bucket_name", f"iobkt-size{obj_size}-samples{num_sample}")
                object_prefix = kwargs.get("object_prefix", f"object-{obj_size}")
                if operations == "write":
                    bucket_name = self.get_s3bucket(operations, bucket_name, obj_size)
                    for clients in self.get_session_distributions(num_sample, sessions):
                        self.create_sessions(
                            self.write_data,
                            bucket_name=bucket_name,
                            object_size=obj_size,
                            object_prefix=object_prefix,
                            sessions=clients,
                        )
                if operations == "read":
                    validate = kwargs.get("validate", False)
                    bucket_name = self.get_s3bucket(operations, bucket_name, obj_size)
                    for clients in self.get_session_distributions(num_sample, sessions):
                        self.create_sessions(
                            self.read_data,
                            bucket_name=bucket_name,
                            object_size=obj_size,
                            object_prefix=object_prefix,
                            sessions=clients,
                            validate=validate,
                        )
                if operations == "validate":
                    bucket_name = self.get_s3bucket(operations, bucket_name, obj_size)
                    for clients in self.get_session_distributions(num_sample, sessions):
                        self.create_sessions(
                            self.validate_data,
                            bucket_name=bucket_name,
                            object_size=obj_size,
                            object_prefix=object_prefix,
                            sessions=clients,
                        )
                if operations == "delete":
                    bucket_name = self.get_s3bucket(operations, bucket_name, obj_size)
                    for clients in self.get_session_distributions(num_sample, sessions):
                        self.create_sessions(
                            self.delete_data,
                            bucket_name=bucket_name,
                            object_size=obj_size,
                            object_prefix=object_prefix,
                            sessions=clients,
                        )
                        for key in self.deleted_files[bucket_name]["keys"]:
                            if key in self.io_ops_dict[bucket_name]:
                                self.io_ops_dict[bucket_name].pop(key)
                    self.log.info(
                        "Bucket: %s, Deleted keys: %s",
                        bucket_name,
                        self.deleted_files[bucket_name]["keys"],
                    )
        if operations == "cleanup":
            for clients in self.get_session_distributions(len(self.io_ops_dict), sessions):
                self.create_sessions(self.cleanup_data, sessions=clients)
