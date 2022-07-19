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

import random
from datetime import datetime, timedelta

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
        if kwargs.get("duration"):
            self.finish_time = datetime.now() + kwargs.get("duration")
        else:
            self.finish_time = datetime.now() + timedelta(hours=int(100 * 24))
        if kwargs.get("total_storage_size"):
            self.initialize_variables(**kwargs)
        else:
            self.distribution = kwargs.get("distribution")

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
        cls.total_storage = kwargs.get("total_storage_size")
        cls.object_size = kwargs.get("object_size")
        cls.sessions = kwargs.get("sessions")
        if not cls.total_storage:
            host, user, password = get_master_details()
            cluster_obj = ClusterServices(host, user, password)
            status, resp = cluster_obj.check_cluster_storage()
            assert status, f"Failed to get storage details: {resp}"
            cls.total_storage = resp["total_capacity"]
        cls.iteration = 0
        cls.total_written_data = 0
        cls.storage_size_to_fill = int(cls.total_storage / 100 * cls.write_percentage)
        cls.storage_size_to_read = int(cls.total_storage / 100 * cls.read_percentage)
        cls.storage_size_to_delete = int(cls.total_storage / 100 * cls.delete_percentage)
        cls.size_to_cleanup_all_data = int(cls.total_storage / 100 * cls.cleanup_percentage)
        cls.s3_url = None
        cls.write_samples = 0
        cls.read_samples = 0
        cls.delete_samples = 0
        cls.file_size = 0

    # pylint: disable=broad-except

    def execute_mix_object_workload(self):
        """Execute mix object operations workload for specific duration."""
        while True:
            self.log.info("iteration %s is started...", self.iteration)
            try:
                if int(self.total_written_data / self.total_storage * 100) < 100:
                    if isinstance(self.object_size, dict):
                        self.object_size = random.randrange(
                            self.object_size["start"], self.object_size["end"])
                    self.log.info("Single object size: %s bytes", self.object_size)
                    self.get_sample_details(self.object_size)
                    # Write data to fill storage as per write percentage.
                    written_data = 0
                    while self.storage_size_to_fill > written_data:
                        self.execute_workload(operations="write",
                                              distribution={self.object_size: self.write_samples},
                                              sessions=self.sessions)
                        written_data += self.write_samples * self.object_size
                    self.display_storage_consumed()
                # Read data as per read percentage.
                read_data, read_iter = 0, 0
                while self.storage_size_to_read > read_data:
                    self.execute_workload(operations="read",
                                          distribution={self.object_size: self.read_samples},
                                          sessions=self.sessions, validate=True)
                    read_data += self.read_samples * self.object_size
                    read_iter += 1
                read_percentage = int(read_data / self.total_storage * 100)
                self.log.info("Able to read %s%% of data from cluster in %s iterations.",
                              read_percentage, read_iter)
                # Delete data as per delete percentage.
                if self.storage_size_to_delete:
                    deleted_data = 0
                    while self.storage_size_to_delete > deleted_data:
                        self.execute_workload(operations="delete",
                                              distribution={self.object_size: self.delete_samples},
                                              sessions=self.sessions)
                        deleted_data += self.object_size * self.delete_samples
                    self.display_storage_consumed(operation="delete")
                # Cleanup data as per cleanup percentage.
                if self.size_to_cleanup_all_data:
                    if self.total_written_data >= self.size_to_cleanup_all_data:
                        self.log.info("cleanup objects from %s as storage consumption reached "
                                      "limit %s%% ", self.s3_url, self.cleanup_percentage)
                        self.execute_workload(operations="cleanup", sessions=self.sessions)
                        self.total_written_data *= 0
                        self.log.info("Data cleanup competed...")
                self.display_storage_consumed(operation="")
            except Exception as err:
                self.log.exception("bucket url: {%s}\nException: {%s}", self.s3_url, err)
                assert False, f"bucket url: {self.s3_url}\nException: {err}"
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                self.execute_workload(operations="cleanup", sessions=self.sessions)
                return True, "Bucket operation execution completed successfully."
            self.log.info("iteration %s is completed...", self.iteration)
            self.iteration += 1

    def display_storage_consumed(self, operation="write"):
        """Display storage consumed after specified operation."""
        consumed_per = int(self.total_written_data / self.total_storage * 100)
        storage_consumed = 100 if consumed_per > 100 else consumed_per
        if operation:
            self.log.info("Storage consumed %s%% after %s operations.", storage_consumed, operation)
        else:
            self.log.info("Storage consumed %s%% after iteration %s.", storage_consumed,
                          self.iteration)

    def get_sample_details(self, file_size: int) -> tuple:
        """
        Get samples for write, read, delete operation to be used in IO.

        :param file_size: Single object size used to calculate the number of sample.
        """
        err_str = "Number of samples '%s' should be greater/equal to number of sessions '%s'."
        sample_msg = "Number of samples '%s' will be used for %s operation."
        per_limit = "%s percentage should be less than or equal to 100%"
        # Logic behind adding extra 1 sample is to cover fractional part.
        assert self.write_percentage <= 100, per_limit.format("Write")
        assert self.delete_percentage <= 100, per_limit.format("Delete")
        w_chunks = modf(self.storage_size_to_fill / file_size)
        self.write_samples = int(w_chunks[1]) + 1 if w_chunks[0] else int(w_chunks[1])
        if self.write_samples < self.sessions:
            self.log.warning(err_str, self.write_samples, self.sessions)
        else:
            self.log.info(sample_msg, self.write_samples, "write")
        r_chunks = modf(self.storage_size_to_read / file_size)
        self.read_samples = int(r_chunks[1]) + 1 if r_chunks[0] else int(r_chunks[1])
        # Added if we are reading data in iteration more than 100 percent.
        self.read_samples = self.read_samples if self.read_percentage <= 100 else self.write_samples
        if self.read_samples < self.sessions:
            self.log.warning(err_str, self.read_samples, self.sessions)
        else:
            self.log.info(sample_msg, self.read_samples, "read")
        d_chunks = modf(self.storage_size_to_delete / file_size)
        self.delete_samples = int(d_chunks[1]) + 1 if d_chunks[0] else int(d_chunks[1])
        if self.delete_samples < self.sessions:
            self.log.warning(err_str, self.delete_samples, self.sessions)
        else:
            self.log.info(sample_msg, self.delete_samples, "delete")
        return self.write_samples, self.read_samples, self.delete_samples

    def execute_object_crud_workload(self):
        """Execute Plain object operations workload  for given distribution for specific duration."""
        distribution = self.distribution
        while True:
            crud_iter = 0
            try:
                self.log.info("iteration %s is started...", self.iteration)
                # Write data to fill storage as per write percentage/distribution.
                self.execute_workload(operations="write",
                                      distribution=self.distribution,
                                      sessions=self.sessions)
                self.log.info("Able to write %s of data samples from cluster in %s iterations.",
                              self.write_samples, crud_iter)
                # Read data as per read percentage/distribution.
                self.execute_workload(operations="read",
                                      distribution=self.distribution,
                                      sessions=self.sessions, validate=True)
                self.log.info("Able to read %s of data from cluster in %s iterations.",
                              self.read_samples, crud_iter)
                # Delete data as per delete percentage.
                self.execute_workload(operations="delete",
                                      distribution=self.distribution,
                                      sessions=self.sessions)
                self.log.info("Able to delete %s of data samples from cluster in %s iterations.",
                              self.delete_samples, crud_iter)
            except Exception as err:
                self.log.exception("bucket url: {%s}\nException: {%s}", self.s3_url, err)
                assert False, f"bucket url: {self.s3_url}\nException: {err}"
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                self.execute_workload(operations="cleanup", sessions=self.sessions)
                return True, "Bucket operation execution completed successfully."
            self.log.info("iteration %s is completed...", self.iteration)
            self.iteration += 1
