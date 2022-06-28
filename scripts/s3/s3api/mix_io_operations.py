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
from time import perf_counter_ns

from src.commons.utils.corio_utils import run_local_cmd
from src.libs.s3api.s3_bucket_ops import S3Bucket
from src.libs.s3api.s3_object_ops import S3Object


# pylint: disable=too-many-instance-attributes
class TestMixIOOps(S3Bucket, S3Object):
    """S3 mix object operations class for executing given io stability workload."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, **kwargs) -> None:
        """
        s3 Mix object operations init class.

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
        self.object_name = None
        self.sessions = kwargs.get("sessions")
        self.bucket_name = f"s3mix-bucket-{perf_counter_ns()}"
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.total_written_data = 0

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
        cls.write_percentage = kwargs.get("write_percentage")
        cls.read_percentage = kwargs.get("read_percentage")
        cls.delete_percentage = kwargs.get("delete_percentage")
        cls.cleanup_percentage = kwargs.get("cleanup_percentage")
        cls.total_storage = kwargs.get("total_storage_size")

    def execute_workload(self):
        """Execute mix object operations workload for specific duration."""
        self.create_s3_bucket(self.bucket_name)
        self.log.info("Created bucket: %s", self.bucket_name)
        self.s3_url = f"s3://{self.bucket_name}"

    def s3_cmd(self, object_size: int, number_sample: int) -> str:
        """
        Create s3 command as per number of sample and object size.

        :param object_size: Object size per sample.
        :param number_sample: Total number samples.
        """
        sessions = self.sessions if number_sample > self.sessions else number_sample
        return f"s3cmd -accessKey={self.access_key} -accessSecret={self.secret_key} " \
               f"-endpoint={self.endpoint_url} -bucket={self.bucket_name} " \
               f"-numClients={sessions} -skipSSLCertVerification=True " \
               f"-objectNamePrefix={self.object_name} " \
               f"-numSamples={number_sample} -objectSize={object_size}b"

    def execute_validate(self, cmd: str, validate: bool = False) -> None:
        """
        Execute command and validate the execution report and logs.

        :param cmd: s3 command to be executed with some workload.
        :param validate: bool
        """
        status, resp = run_local_cmd(cmd)
        if validate:
            self.log("TODO: Validate result")
        self.log(status, resp)

    def write_data(self, object_size: int, number_sample: int, validate: bool = False) -> None:
        """
        Write data to s3.

        :param object_size: Object size per sample.
        :param number_sample: total number objects to write.
        :param validate: validate data after write.
        """
        self.log.info("Writing data...")
        self.log.info("Single object size: %s, Number of samples: %s", object_size, number_sample)
        cmd = self.s3_cmd(object_size, number_sample)
        self.execute_validate(cmd, validate)
        self.total_written_data += object_size * number_sample
        self.log.info("writing completed...")

    def read_data(self, object_size: int, number_sample: int, validate: bool = False) -> None:
        """
        Read data from s3.

        :param object_size: Object size per sample.
        :param number_sample: total number objects to read.
        :param validate: validate data after reading.
        """
        self.log.info("Reading data...")
        self.log.info("Single object size: %s, Number of samples: %s", object_size, number_sample)
        cmd = self.s3_cmd(object_size, number_sample)
        self.execute_validate(cmd, validate)
        self.log.info("Reading completed...")

    def delete_data(self, object_size: int, number_sample: int, validate: bool = False) -> None:
        """
        Delete data from s3.

        :param object_size: Object size per sample.
        :param number_sample: total number objects to delete.
        :param validate: validate data before delete.
        """
        self.log.info("Deleting data...")
        self.log.info("Single object size: %s, Number of samples: %s", object_size, number_sample)
        cmd = self.s3_cmd(object_size, number_sample)
        self.execute_validate(cmd, validate)
        self.total_written_data -= object_size * number_sample
        self.log.info("Deletion completed...")

    def validate_data(self, object_size: int, number_sample: int) -> None:
        """
        Validate data from s3.

        :param object_size: Object size per sample.
        :param number_sample: total number objects to validate.
        """
        self.log.info("Validating data...")
        self.log.info("Single object size: %s, Number of samples: %s", object_size, number_sample)
        cmd = self.s3_cmd(object_size, number_sample)
        self.execute_validate(cmd)
        self.log.info("Validation completed...")

    def cleanup_data(self, object_size: int, number_sample: int) -> None:
        """
        cleanup data from s3.

        :param object_size: Object size per sample.
        :param number_sample: total number objects to cleanup.
        """
        self.log.info("Cleaning data...")
        self.log.info("Single object size: %s, Number of samples: %s", object_size, number_sample)
        cmd = self.s3_cmd(object_size, number_sample)
        self.execute_validate(cmd)
        self.log.info("Data cleanup completed...")

    def object_crud_operations(self, object_size: int, number_sample: int) -> None:
        """
        Perform object crud operations.

        :param object_size: Object size per sample.
        :param number_sample: total number objects to write, read, validate, delete.
        """
        self.log.info("Object CRUD operation started...")
        self.log.info("Single object size: %s, Number of samples: %s", object_size, number_sample)
        cmd = self.s3_cmd(object_size, number_sample)
        self.execute_validate(cmd)
        self.log.info("Object CRUD operation completed...")
