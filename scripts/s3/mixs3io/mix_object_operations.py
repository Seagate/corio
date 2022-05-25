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
import random
from datetime import datetime, timedelta
from time import perf_counter_ns

from src.commons.constants import MIN_DURATION
from src.libs.s3api.s3_bucket_ops import S3Bucket
from src.libs.s3api.s3_object_ops import S3Object
from src.libs.tools.s3bench import S3bench


class TestMixObjectOps(S3Bucket, S3Object):
    """S3 Bucket Operations class for executing given io stability workload."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, test_id: str,
                 **kwargs) -> None:
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
            "use_ssl"), test_id=f"{test_id}_mix_s3io_operations")
        random.seed(kwargs.get("seed"))
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.test_id = test_id.lower()
        self.bucket_name = None
        if not S3bench.install_s3bench():
            raise Exception("s3bench tool is not installed.")
        if kwargs.get("duration"):
            self.finish_time = datetime.now() + kwargs.get("duration")
        else:
            self.finish_time = datetime.now() + timedelta(hours=int(100 * 24))
        self.define_variables(kwargs)

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
        cls.total_storage = kwargs.get("total_storage")
        cls.object_size = kwargs.get("object_size")
        cls.sessions = kwargs.get("sessions")
        cls.written_percentage = 0

    # pylint: disable=broad-except
    def execute_mix_object_workload(self):
        """Execute mix object operations workload for specific duration."""
        # TODO: disable_background_delete/enable_background_delete
        while True:
            self.bucket_name = self.bucket_prefix.format(self.iteration, perf_counter_ns())
            self.create_s3_bucket(self.bucket_name)
            s3_url = f"s3://{self.bucket_name}/{self.object_prefix}"
            self.log.info("self.iteration %s is started...", self.iteration)
            try:
                if isinstance(self.object_size, dict):
                    file_size = random.randrange(self.object_size["start"], self.object_size["end"])
                else:
                    file_size = self.object_size

                if self.written_percentage > self.cleanup_percentage:
                    self.delete_s3_objects(self.bucket_name, object_prefix=self.object_prefix)
            except Exception as err:
                self.log.exception("bucket url: {%s}\nException: {%s}", s3_url, err)
                assert False, f"bucket url: {s3_url}\nException: {err}"
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                self.delete_s3_bucket(self.bucket_name, force=True)
                return True, "Bucket operation execution completed successfully."
            self.log.info("self.iteration %s is completed...", self.iteration)
            self.iteration += 1
