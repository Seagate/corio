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
"""Mix(Write, Read, Validate, Delete in percentage) object operation workload for io stability."""

import asyncio
import random
from datetime import datetime, timedelta
from typing import Union

from math import modf
from time import perf_counter_ns

from src.commons.constants import MIN_DURATION
from src.commons.utils.cluster_utils import ClusterServices
from src.commons.utils.corio_utils import get_master_details
from src.libs.s3api.s3_parallel_io_ops import S3ApiParallelIO


# pylint: disable=too-many-instance-attributes
class TestTypeXObjectOps(S3ApiParallelIO):
    """S3 object crud operations class for executing given type-x io stability workload."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, **kwargs) -> None:
        """
        s3 crud object operations init class.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :param test_id: Test ID string.
        :param use_ssl: To use secure connection.
        :param object_size: Object size to be used for bucket operation
        :param seed: Seed to be used for random data generator
        :param session: session name.
        :param duration: Duration timedelta object, if not given will run for 100 days.
        """
        super().__init__(access_key, secret_key, endpoint_url=endpoint_url, use_ssl=kwargs.get(
            "use_ssl"), test_id=f"{kwargs.get('test_id')}_mix_s3io_operations")
        random.seed(kwargs.get("seed"))
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.iteration = 0
        self.sessions = kwargs.get("sessions")
        if kwargs.get("duration"):
            self.finish_time = datetime.now() + kwargs.get("duration")
        else:
            self.finish_time = datetime.now() + timedelta(hours=int(100 * 24))
        if "total_storage_size" in kwargs:
            self.initialize_variables(**kwargs)
        else:
            self.distribution = kwargs.get("object_size")

    @classmethod
    def initialize_variables(cls, **kwargs):
        """
        Initialize the variables.

        # :param write_percentage: Percentage of data to fill storage.
        # :param read_percentage: Percentage of data to read from storage.
        # :param delete_percentage: percentage of data to delete from storage.
        # :param cleanup_percentage: Once write reached to percentage then delete all data.
        # :param total_storage_size: Total storage on cloud.
        """
        cls.object_prefix = "s3mix_object_ops_iter"
        cls.bucket_name = f"s3mix-bucket-{perf_counter_ns()}"
        cls.write_percentage = kwargs.get("write_percentage")
        cls.read_percentage = kwargs.get("read_percentage")
        cls.delete_percentage = kwargs.get("delete_percentage")
        cls.cleanup_percentage = kwargs.get("cleanup_percentage")
        cls.object_size = kwargs.get("object_size")
        cls.cluster_storage = cls.get_cluster_capacity(**kwargs)
        cls.total_written_data = 0

    @staticmethod
    def get_cluster_capacity(**kwargs):
        """Get cluster storage from kwargs if passed else from cluster."""
        cluster_storage_size = kwargs.get("total_storage_size")
        if not cluster_storage_size:
            host, user, password = get_master_details()
            cluster_obj = ClusterServices(host, user, password)
            status, cluster_storage_size = cluster_obj.get_user_quota_in_bytes()
            assert status, f"Failed to get user quota: {cluster_storage_size}"
        return cluster_storage_size

    @staticmethod
    def get_total_size_from_distribution(distribution: dict) -> int:
        """Get total storage utilized from storage distribution."""
        total_size = 0
        for size in [size * samples for size, samples in distribution.items()]:
            total_size += size
        return total_size

    async def display_storage_consumed(self, operation="write"):
        """Display storage consumed after specified operation."""
        storage_consumed = int(self.total_written_data / self.cluster_storage * 100)
        if operation:
            self.log.info("Storage consumed %s%% after %s operations.", storage_consumed, operation)
        else:
            self.log.info("Storage consumed %s%% after iteration %s.", storage_consumed,
                          self.iteration)
        await asyncio.sleep(0)

    # pylint: disable=too-many-branches
    async def get_object_distribution(self, file_size: Union[int, list], operation: str) -> dict:
        """
        Get samples for write, read, delete operation to be used in IO.

        :param file_size: Single object size used to calculate the number of sample.
        :param operation: Distribution for write, read, validate, delete operations.
        """
        per_limit = "%s percentage should be less than or equal to 100%"
        err = f"Unable to generate distribution due to unsupported file size: {file_size}"
        # Logic behind adding extra 1 sample is to cover fractional part.
        assert self.write_percentage <= 100, per_limit.format("Write")
        assert self.delete_percentage <= 100, per_limit.format("Delete")
        if operation == "write":
            write_size = int((self.cluster_storage - self.total_written_data) *
                             self.write_percentage / 100)
            if isinstance(file_size, (int, list)):
                object_distribution = self.get_distribution_samples(write_size, file_size)
            else:
                raise AssertionError(err)
        elif operation in "read":
            read_size = int(self.total_written_data * self.read_percentage / 100)
            if isinstance(file_size, (int, list)):
                object_distribution = self.get_distribution_samples(read_size, file_size)
            else:
                raise AssertionError(err)
        elif operation in "delete":
            delete_size = int(self.total_written_data * self.delete_percentage / 100)
            if isinstance(file_size, (int, list)):
                object_distribution = self.get_distribution_samples(delete_size, file_size)
            else:
                raise AssertionError(err)
        else:
            raise AssertionError(f"Unsupported operation: {operation}.")
        await asyncio.sleep(0)
        self.log.info("Operation: %s, Object distribution: %s", operation, object_distribution)
        return object_distribution

    @staticmethod
    def get_distribution_samples(total_size: int, file_size: Union[list, int]) -> dict:
        """Get distribution dict of size and number of samples per size."""
        fsize_list = file_size if isinstance(file_size, list) else [file_size]
        object_distribution = {}
        single_workload_byte = int(total_size / len(fsize_list))
        for fsize in fsize_list:
            w_chunks = modf(single_workload_byte / fsize)
            samples = int(w_chunks[1]) + 1 if w_chunks[0] >= 0.67 else int(w_chunks[1])
            if samples:
                object_distribution[fsize] = samples
        return object_distribution

    # pylint: disable=broad-except, too-many-branches
    async def execute_mix_object_workload(self):
        """Execute mix object operations workload for specific duration."""
        size_to_cleanup_all_data = int(self.cluster_storage / 100 * self.cleanup_percentage)
        while True:
            self.log.info("iteration %s is started...", self.iteration)
            try:
                self.log.info("Object size in bytes: %s", self.object_size)
                written_percentage = int(self.total_written_data / self.cluster_storage * 100)
                if isinstance(self.object_size, dict):
                    object_size = random.randrange(
                        self.object_size["start"], self.object_size["end"])
                elif isinstance(self.object_size, list):
                    object_size = self.object_size
                else:
                    raise AssertionError(f"Unsupported object size type: {type(self.object_size)}")
                # Write data to fill storage as per write percentage. 3% delta added as fractions
                # of bytes might be missed during calculation of sample distribution.
                if written_percentage + 3 < self.write_percentage:
                    write_object_distribution = await self.get_object_distribution(
                        object_size, operation="write")
                    self.execute_workload(operations="write",
                                          distribution=write_object_distribution,
                                          sessions=self.sessions)
                    self.total_written_data += self.get_total_size_from_distribution(
                        write_object_distribution)
                    await self.display_storage_consumed()
                # Read data as per read percentage.
                if self.read_percentage:
                    read_object_distribution = await self.get_object_distribution(
                        object_size, operation="read")
                    self.execute_workload(operations="read",
                                          distribution=read_object_distribution,
                                          sessions=self.sessions, validate=True)
                    read_percentage = int(self.get_total_size_from_distribution(
                        read_object_distribution) / self.total_written_data * 100)
                    self.log.info("Able to read %s%% of data from cluster.", read_percentage)
                # Delete data as per delete percentage.
                if self.delete_percentage:
                    delete_object_distribution = await self.get_object_distribution(
                        object_size, operation="delete")
                    self.execute_workload(operations="delete",
                                          distribution=delete_object_distribution,
                                          sessions=self.sessions)
                    self.total_written_data -= self.get_total_size_from_distribution(
                        delete_object_distribution)
                    await self.display_storage_consumed(operation="delete")
                # Cleanup data as per cleanup percentage.
                if size_to_cleanup_all_data:
                    if self.total_written_data >= size_to_cleanup_all_data:
                        self.log.info("cleanup objects from %s as storage consumption reached "
                                      "limit %s%% ", self.s3_url, self.cleanup_percentage)
                        self.execute_workload(operations="cleanup", sessions=self.sessions)
                        self.total_written_data *= 0
                        self.log.info("Data cleanup competed...")
                await self.display_storage_consumed(operation="")
            except Exception as err:
                exception = (f"bucket url: '{self.s3_url}', Exception: '{err}"'' if self.s3_url
                             else f"Exception: '{err}"'')
                self.log.exception(exception)
                assert False, exception
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                self.execute_workload(operations="cleanup", sessions=self.sessions)
                return True, "Object workload execution completed successfully."
            self.log.info("iteration %s is completed...", self.iteration)
            self.iteration += 1

    async def execute_object_crud_workload(self):
        """Execute Plain object operations workload for given distribution for specific duration."""
        while True:
            try:
                self.log.info("iteration %s is started...", self.iteration)
                # Write data to fill storage as per write percentage/distribution.
                self.execute_workload(operations="write",
                                      distribution=self.distribution,
                                      sessions=self.sessions)
                self.log.info("Able to write %s of data samples from cluster in %s iterations.",
                              self.distribution.keys(), self.distribution.values())
                # Read data as per read percentage/distribution.
                self.execute_workload(operations="read",
                                      distribution=self.distribution,
                                      sessions=self.sessions, validate=True)
                self.log.info("Able to read %s of data from cluster in %s iterations.",
                              self.distribution.keys(), self.distribution.values())
                # Delete data as per delete percentage.
                self.execute_workload(operations="delete",
                                      distribution=self.distribution,
                                      sessions=self.sessions)
                self.log.info("Able to delete %s of data samples from cluster in %s iterations.",
                              self.distribution.keys(), self.distribution.values())
                self.log.info("Cleaning up remaining buckets and objects")
                self.execute_workload(operations="cleanup", sessions=self.sessions)
                await asyncio.sleep(0)
            except Exception as err:
                self.log.exception("bucket url: {%s}\nException: {%s}", self.s3_url, err)
                assert False, f"bucket url: {self.s3_url}\nException: {err}"
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                self.execute_workload(operations="cleanup", sessions=self.sessions)
                return True, "Bucket operation execution completed successfully."
            self.log.info("iteration %s is completed...", self.iteration)
            self.iteration += 1
