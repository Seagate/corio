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
import os.path
import random
from datetime import datetime, timedelta
from time import perf_counter_ns

from src.commons.constants import LATEST_LOG_PATH
from src.commons.constants import MIN_DURATION
from src.commons.utils.corio_utils import convert_size
from src.commons.utils.corio_utils import run_local_cmd
from src.libs.s3api.s3_bucket_ops import S3Bucket
from src.libs.s3api.s3_object_ops import S3Object
from src.libs.tools.s3bench import S3bench


class TestMixObjectOps(S3Bucket, S3Object):
    """S3 Bucket Operations class for executing given io stability workload."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, **kwargs) -> None:
        """
        s3 bucket operations init class.

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
        self.bucket_name = None
        if not S3bench.install_s3bench():
            raise Exception("s3bench tool is not installed.")
        if kwargs.get("duration"):
            self.finish_time = datetime.now() + kwargs.get("duration")
        else:
            self.finish_time = datetime.now() + timedelta(hours=int(100 * 24))
        self.define_variables(**kwargs)

    @classmethod
    def define_variables(cls, kwargs):
        """Initialize the variables."""
        cls.bucket_prefix = "s3mix-bucket-{}-{}"
        cls.object_prefix = "s3mix_object-"
        cls.iteration = 0
        cls.write_percentage = kwargs.get("write_percentage")
        cls.read_percentage = kwargs.get("read_percentage")
        cls.delete_percentage = kwargs.get("delete_percentage")
        cls.cleanup_percentage = kwargs.get("cleanup_percentage")
        cls.total_storage = kwargs.get("total_storage_size")
        cls.object_size = kwargs.get("object_size")
        cls.sessions = kwargs.get("sessions")
        cls.total_written_data = 0
        cls.report_path = os.path.join(
            LATEST_LOG_PATH, f"{kwargs.test_id}_mix_s3io_operations_report.log")
        cls.cli_log_path = os.path.join(
            LATEST_LOG_PATH, f"{kwargs.test_id}_mix_s3io_operations_console.log")
        cls.label = kwargs.get("test_id")

    # pylint: disable=broad-except
    def execute_mix_object_workload(self):
        """Execute mix object operations workload for specific duration."""
        # TODO: disable_background_delete/enable_background_delete
        self.bucket_name = self.bucket_prefix.format(self.iteration, perf_counter_ns())
        self.create_s3_bucket(self.bucket_name)
        s3_url = f"s3://{self.bucket_name}/{self.object_prefix}"
        storage_space = convert_size(self.total_storage)
        while True:
            self.log.info("self.iteration %s is started...", self.iteration)
            try:
                if isinstance(self.object_size, dict):
                    file_size = random.randrange(
                        self.object_size["start"], self.object_size["end"])
                else:
                    file_size = self.object_size
                storage_to_fill, number_sample, read_sample, delete_sample = \
                    self.get_sample_details(storage_space, file_size)
                self.log.info("Single file size: %sb", file_size)
                self.log.info("number of samples: %s", number_sample)
                written_data = 0
                while storage_to_fill <= written_data:
                    self.write_data(file_size, number_sample, validate=True)
                    written_data += number_sample * file_size
                self.read_data(file_size, read_sample, validate=True)
                self.validate_data(file_size, number_sample)
                self.delete_data(file_size, delete_sample)
                if self.total_written_data >= int(
                        self.total_storage / 100 * self.cleanup_percentage):
                    self.log.info("Deleting all object from %s", self.bucket_name)
                    self.delete_s3_objects(self.bucket_name, object_prefix=self.object_prefix)
                    self.total_written_data *= 0
                    self.log.info("Data cleanup competed...")
            except Exception as err:
                self.log.exception("bucket url: {%s}\nException: {%s}", s3_url, err)
                assert False, f"bucket url: {s3_url}\nException: {err}"
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                self.delete_s3_bucket(self.bucket_name, force=True)
                return True, "Bucket operation execution completed successfully."
            self.log.info("self.iteration %s is completed...", self.iteration)
            self.iteration += 1

    def get_sample_details(self, storage_space, file_size):
        """Get all stats to be used in IO."""
        storage_to_fill = round(int(storage_space) / 100 * self.write_percentage) if \
            self.write_percentage else 0
        number_sample = int(storage_to_fill / file_size)
        read_sample = int(number_sample / 100 * self.read_percentage) if \
            self.read_percentage else number_sample
        delete_sample = int(number_sample / 100 * self.delete_percentage) if \
            self.delete_percentage else number_sample
        return storage_to_fill, number_sample, read_sample, delete_sample

    def s3bench_cmd(self, object_size, number_sample):
        """Create s3bench command."""
        return f"s3bench -accessKey={self.access_key} -accessSecret={self.secret_key} " \
               f"-endpoint={self.endpoint_url} -bucket={self.bucket_name} " \
               f"-numClients={self.sessions} -skipSSLCertVerification=True " \
               f"-objectNamePrefix={self.object_prefix} -numSamples={number_sample} " \
               f"-objectSize={object_size}b "

    def cmd_reporting_params(self):
        """Append reporting parameters to s3bench command."""
        return f" -o {self.report_path} -label {self.label} >> {self.cli_log_path} 2>&1"

    def execute_validate_run(self, cmd):
        """Execute command and validate the execution report and logs."""
        cmd += self.cmd_reporting_params()
        status, resp = run_local_cmd(cmd)
        assert status, f"Failed execute '{cmd}', response: {resp}"
        status, resp = S3bench.check_log_file_error(self.report_path, self.cli_log_path)
        assert status, f"Observed failures for '{cmd}', response: {resp}"

    def write_data(self, object_size, number_sample, validate=False):
        """Write data to s3."""
        cmd = self.s3bench_cmd(object_size, number_sample)
        self.total_written_data += object_size + number_sample
        if validate:
            cmd += " -skipRead -skipCleanup"
        else:
            cmd += " -skipRead -skipCleanup -validate"
        self.execute_validate_run(cmd)

    def read_data(self, object_size, number_sample, validate=False):
        """Read data from s3."""
        cmd = self.s3bench_cmd(object_size, number_sample)
        if validate:
            cmd += " -skipWrite -skipCleanup"
        else:
            cmd += " -skipWrite -skipCleanup -validate"
        self.execute_validate_run(cmd)

    def delete_data(self, object_size, number_sample, validate=False):
        """Delete data from s3."""
        cmd = self.s3bench_cmd(object_size, number_sample)
        self.total_written_data -= object_size * number_sample
        if validate:
            cmd += " -skipWrite -skipRead"
        else:
            cmd += " -skipWrite -skipRead -validate"
        self.execute_validate_run(cmd)

    def validate_data(self, object_size, number_sample):
        """Validate data from s3."""
        cmd = self.s3bench_cmd(object_size, number_sample)
        cmd += " -skipWrite -skipRead -skipCleanup -validate"
        self.execute_validate_run(cmd)

    def cleanup_data(self, object_size, number_sample):
        """cleanup data from s3."""
        cmd = self.s3bench_cmd(object_size, number_sample)
        cmd += " -skipWrite -skipRead"
        self.execute_validate_run(cmd)

    def object_crud_operations(self, object_size, number_sample):
        """Perform object crud operations."""
        cmd = self.s3bench_cmd(object_size, number_sample)
        cmd += " -validate"
        self.execute_validate_run(cmd)
