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
"""object crud operations in parallel for io stability."""

import asyncio
import os

from time import perf_counter_ns

from src.commons.utils import corio_utils
from src.libs.s3api import S3Api


class S3ApiParallelIO(S3Api):
    """S3 mix object operations class for executing given io stability workload."""

    # pylint: disable=too-many-arguments

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, **kwargs) -> None:
        """
        s3 Mix object operations init class.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :param use_ssl: To use secure connection.
        """
        super().__init__(access_key, secret_key, endpoint_url=endpoint_url, **kwargs)
        self.io_ops_dict = {}
        self.read_files = []
        self.validated_files = []
        self.delete_keys = {}
        self.s3_buckets = []

    async def read_data(self, bucket_name: str, object_size: int, sessions: int,
                        object_prefix: str, validate=False) -> None:
        """
        Read data from s3 bucket.

        :param bucket_name: Name of the s3 bucket.
        :param object_size: Object size per sample.
        :param sessions: total number of sessions used to read samples.
        :param object_prefix: Object prefix used for read.
        :param validate: Validate the io's.
        """
        self.log.info("Reading data...")
        self.log.info("Object size: %s, Number of samples: %s", object_size, sessions)

        async def read_object(**kwargs):
            """Read s3 object."""
            i = kwargs.get("cntr")
            self.log.info("Get Object and check data integrity.")
            for file_name in list(self.io_ops_dict[bucket_name].keys())[i:]:
                self.log.info("Getting Values for file %s", file_name)
                if self.io_ops_dict[bucket_name][file_name]["key_size"] == object_size and \
                        file_name not in self.read_files and file_name.startswith(object_prefix):
                    checksum_in = self.io_ops_dict[bucket_name][file_name]["key_checksum"]
                    if validate:
                        if checksum_in != await self.get_s3object_checksum(bucket_name, file_name):
                            raise AssertionError("Checksum are not equal.")
                    else:
                        await self.get_object(bucket_name, file_name)
                    self.read_files.append(file_name)
                    break

        await self.schedule_api_sessions(sessions, read_object)
        self.log.info("Reading completed...")

    async def delete_data(self, bucket_name: str, object_size: int, sessions: int,
                          object_prefix: str) -> None:
        """
        delete data from s3.

        :param bucket_name: Name of the bucket.
        :param object_size: Object size per sample.
        :param sessions: total number of sessions used to upload samples.
        :param object_prefix: object prefix used to delete.
        """
        self.log.info("Deleting data...")
        self.log.info("Single object size: %s, Number of samples: %s", object_size, sessions)
        self.delete_keys[bucket_name] = []
        del_key_cnt = len(self.delete_keys[bucket_name])
        if del_key_cnt + sessions > len(self.io_ops_dict[bucket_name]):
            raise AssertionError(f"Opted deletion keys count '{del_key_cnt + sessions}' is greater"
                                 f" than actual keys '{self.io_ops_dict[bucket_name].keys()}'")

        async def delete_s3object(**kwargs):
            """Delete s3 object."""
            i = kwargs.get("cntr")
            file_name = list(self.io_ops_dict[bucket_name])[i]
            if file_name.startswith(object_prefix) and self.io_ops_dict[bucket_name][file_name][
                    "key_size"] == object_size:
                await self.delete_object(bucket_name, file_name)
                self.delete_keys[bucket_name].append(file_name)

        await self.schedule_api_sessions(sessions, delete_s3object, cntr=del_key_cnt)
        self.log.info("Deletion completed...")

    async def validate_data(self, bucket_name: str, object_size: int, sessions: int,
                            object_prefix: str) -> None:
        """
        Validate data from s3.

        :param bucket_name: Name of the s3 bucket.
        :param object_size: Object size per sample.
        :param sessions: total number objects to validate.
        :param object_prefix: object prefix used to validate specific object.
        """
        self.log.info("Validating data...")
        self.log.info("Single object size: %s, Object prefix: %s,  Number of samples: %s",
                      object_size, object_prefix, sessions)

        async def validate_object(**kwargs):
            """Validate object."""
            i = kwargs.get("cntr")
            for file_name in list(self.io_ops_dict[bucket_name].keys())[i:]:
                if file_name.startswith(object_prefix) and self.io_ops_dict[bucket_name][file_name][
                        "key_size"] == object_size and file_name not in self.validated_files:
                    checksum_in = self.io_ops_dict[bucket_name][file_name]["key_checksum"]
                    checksum_dwn = await self.get_s3object_checksum(bucket_name, file_name)
                    assert checksum_in == checksum_dwn, (f"Checksum are not equal for {file_name}:"
                                                         f"checksum_in: {checksum_in},"
                                                         f" checksum_down: {checksum_dwn}.")
                    self.log.info("Check sum matched for object %s", file_name)
                    break
        await self.schedule_api_sessions(sessions, validate_object)
        self.log.info("Validation completed...")

    async def cleanup_data(self, sessions: int) -> None:
        """
        delete s3 bucket with all objects in it.

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

    async def write_data(self, bucket_name: str, object_size: int, object_prefix: str,
                         sessions: int) -> None:
        """
        Write data to s3.

        :param object_prefix: Object name prefix used while creating unique object.
        :param bucket_name: Name of the s3 bucket.
        :param object_size: Object size per sample.
        :param sessions: total number of sessions(samples) used to upload samples.
        """
        self.log.info("Writing data...")
        file_name = f"{object_prefix}-{perf_counter_ns()}"
        self.s3_buckets.append(bucket_name)
        self.log.info("Single object size: %s, Number of samples: %s", object_size, sessions)
        file_path = corio_utils.create_file(file_name, object_size)
        self.log.info("Object '%s', object size %s", object_prefix, corio_utils.convert_size(
            object_size))
        checksum_in = self.checksum_file(file_path)
        self.log.debug("Checksum of '%s' = %s", file_name, checksum_in)
        kcnt = (len(self.io_ops_dict[bucket_name]) if bucket_name in self.io_ops_dict else 0) + 1

        async def upload_object(**kwargs):
            """upload s3 object."""
            key = f"{object_prefix}-{perf_counter_ns()}-{checksum_in}-{kwargs.get('cntr')}"
            s3_url = f"s3://{bucket_name}/{key}"
            response = await self.upload_object(bucket_name, key, file_path=file_path)
            self.log.info("Uploaded s3 object: url: %s", s3_url)
            if bucket_name not in self.io_ops_dict:
                self.io_ops_dict[bucket_name] = {key: {"s3url": s3_url, "key_size": object_size,
                                                       "key_checksum": checksum_in, "bucket":
                                                       bucket_name, "key": key,
                                                       "etag": response['ETag']}}
            else:
                self.io_ops_dict[bucket_name][key] = {"s3url": s3_url, "key_size": object_size,
                                                      "key_checksum": checksum_in,
                                                      "bucket": bucket_name, "key": key,
                                                      "etag": response['ETag']}
            self.log.info("s3://%s/%s uploaded successfully.", bucket_name, key)

        self.log.info("Scheduling to upload file %s, size %s, for samples %s ",
                      object_prefix, file_path, sessions)
        await self.schedule_api_sessions(sessions, upload_object, cntr=kcnt)
        os.remove(file_path)

    @staticmethod
    def get_session_distributions(samples, sessions):
        """get session distributions as per samples."""
        if samples < sessions:
            distribution_list = [samples]
        else:
            distribution_list = [sessions for _ in range(int(samples / sessions))]
            if samples % sessions:
                distribution_list.extend([samples % sessions])
        return distribution_list

    async def schedule_api_sessions(self, sessions, func, **kwargs):
        """schedule api sessions."""
        tasks = []
        kwargs["cntr"] = kwargs.get("cntr", 0)
        for _ in range(1, sessions + 1):
            tasks.append(func(**kwargs))
            kwargs["cntr"] += 1
        if tasks:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            if pending:
                self.log.critical("Terminating pending task: %s", func)
                for task in pending:
                    task.cancel()
            self.log.info(done)
            for task in done:
                task.result()

    # pylint: disable=broad-except
    def create_sessions(self, func, **kwargs):
        """start workload execution."""
        self.log.info("Execution started for %s", func)
        loop = asyncio.get_event_loop()
        try:
            loop.run_until_complete(func(**kwargs))
        except Exception as error:
            self.log.exception(error)
            if loop.is_running():
                loop.stop()
            raise error from Exception
        finally:
            if loop.is_closed():
                loop.close()
        self.log.info("Execution completed: %s", not loop.is_running())

    # pylint: disable=too-many-branches
    def execute_workload(self, operations, sessions=1, **kwargs):
        """Execute s3 workload."""
        distribution = kwargs.get("distribution")
        if distribution:
            for obj_size, num_sample in distribution.items():
                bucket_name = kwargs.get("bucket_name", f"iobkt-size{obj_size}-samples{num_sample}")
                object_prefix = kwargs.get("object_prefix", f'object-{obj_size}')
                if operations == "write":
                    if bucket_name not in self.list_s3_buckets():
                        self.create_s3_bucket(bucket_name)
                    for clients in self.get_session_distributions(num_sample, sessions):
                        self.create_sessions(self.write_data, bucket_name=bucket_name,
                                             object_size=obj_size, object_prefix=object_prefix,
                                             sessions=clients)
                if operations == "read":
                    validate = kwargs.get("validate", False)
                    for clients in self.get_session_distributions(num_sample, sessions):
                        self.create_sessions(self.read_data, bucket_name=bucket_name,
                                             object_size=obj_size, object_prefix=object_prefix,
                                             sessions=clients, validate=validate)
                if operations == "validate":
                    for clients in self.get_session_distributions(num_sample, sessions):
                        self.create_sessions(self.validate_data, bucket_name=bucket_name,
                                             object_size=obj_size, object_prefix=object_prefix,
                                             sessions=clients)
                if operations == "delete":
                    bucket_name = [bkt for bkt in self.list_s3_buckets()
                                   if (bucket_name == bkt or
                                       bkt.startswith(f"iobkt-size{obj_size}"))][-1]
                    if not bucket_name:
                        raise AssertionError(f"Bucket does not exists: {bucket_name}")
                    for clients in self.get_session_distributions(num_sample, sessions):
                        self.create_sessions(self.delete_data, bucket_name=bucket_name,
                                             object_size=obj_size, object_prefix=object_prefix,
                                             sessions=clients)
                        for key in self.delete_keys[bucket_name]:
                            self.io_ops_dict[bucket_name].pop(key)
                        self.log.info("Bucket: %s, Deleted keys: %s", bucket_name, self.delete_keys)
        if operations == "cleanup":
            for clients in self.get_session_distributions(len(self.io_ops_dict), sessions):
                self.create_sessions(self.cleanup_data, sessions=clients)
