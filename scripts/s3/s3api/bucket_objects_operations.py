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
"""Script type5 s3 bucket objects operation workload for io stability."""

import random
import time
from datetime import datetime, timedelta

from src.commons.constants import MIN_DURATION
from src.libs.s3api.s3io_utils import S3ApiIOUtils


class TestType5BucketObjectOps(S3ApiIOUtils):
    """S3 bucket objects type5 operations class."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, **kwargs) -> None:
        """
        s3 bucket objects operations init class.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :keyword test_id: Test ID string.
        :keyword use_ssl: To use secure connection.
        :keyword object_size: Object size to be used for bucket operation
        :keyword seed: Seed to be used for random data generator
        :keyword session: session name.
        :keyword sessions: Number of sessions used for IO.
        :keyword delay: Delay between percentage of object CRUD operations per bucket.
        :keyword number_of_objects: Total number of objects per bucket.
        :keyword delete_percentage_per_bucket: Delete percentage of objects per bucket.
        :keyword read_percentage_per_bucket: Read percentage of objects per bucket.
        :keyword overwrite_percentage_per_bucket: Overwrite percentage of objects per bucket.
        :keyword put_percentage_per_bucket: Write percentage of objects per bucket.
        :keyword duration: Duration timedelta object, if not given will run for 100 days.
        """
        super().__init__(
            access_key,
            secret_key,
            endpoint_url=endpoint_url,
            use_ssl=kwargs.get("use_ssl"),
            test_id=f"{kwargs.get('test_id')}_bucket_objects_operations",
        )
        random.seed(kwargs.get("seed"))
        self.kwargs = kwargs
        self.finish_time = datetime.now() + kwargs.get("duration", timedelta(hours=int(100 * 24)))

    # pylint: disable=broad-except
    async def execute_bucket_object_workload(self):
        """Execute bucket object operations workload for specific duration in parallel."""
        try:
            iteration = 1
            sessions = self.kwargs.get("sessions")
            delay = self.kwargs.get("delay")
            object_size = self.kwargs.get("object_size")
            self.log.info("Iteration %s is started.", iteration)
            buckets = await self.create_n_buckets(
                "bucket-objects-ops", self.kwargs.get("number_of_buckets")
            )
            self.log.info("Bucket list: %s", buckets)
            distribution = self.distribution_of_buckets_objects_per_session(
                buckets, self.kwargs.get("number_of_objects"), sessions
            )
            self.generate_objects_distribution(
                distribution,
                self.kwargs.get("delete_percentage_per_bucket"),
                self.kwargs.get("put_percentage_per_bucket"),
                self.kwargs.get("overwrite_percentage_per_bucket"),
                read_percentage_per_bucket=self.kwargs.get("read_percentage_per_bucket"),
            )
            self.starts_sessions(self.write_data, distribution, object_size)
            while True:
                if iteration > 1:
                    self.log.info("Iteration %s is started.", iteration)
                if delay:
                    sleep_time = self.get_random_sleep_time(delay)
                    self.log.info("sleep for %s hrs", sleep_time / (60**2))
                    time.sleep(sleep_time)
                if self.kwargs.get("read_percentage_per_bucket"):
                    self.starts_sessions(self.read_distribution_data, distribution, object_size)
                else:
                    self.starts_sessions(self.read_all_data, distribution)
                if self.kwargs.get("overwrite_percentage_per_bucket"):
                    self.starts_sessions(
                        self.overwrite_distribution_data, distribution, object_size
                    )
                if self.kwargs.get("delete_percentage_per_bucket"):
                    self.starts_sessions(self.delete_distribution_data, distribution)
                if self.kwargs.get("put_percentage_per_bucket"):
                    self.starts_sessions(self.write_distribution_data, distribution, object_size)
                self.log.info("Iteration %s is completed.", iteration)
                if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                    self.starts_sessions(self.cleanup_data, buckets)
                    return True, "bucket object workload execution completed successfully."
                iteration += 1
        except Exception as err:
            exception = f"Exception: {err}"
            if self.s3_url:
                exception = f"bucket url: '{self.s3_url}', {exception}"
            self.log.exception(exception)
            assert False, exception

    def create_distribution_as_per_percentage(self, buckets, sessions):
        """Create object distribution as per given percentage."""
        distribution = self.distribution_of_buckets_objects_per_session(
            buckets, self.kwargs.get("number_of_objects"), sessions
        )
        self.generate_objects_distribution(
            distribution,
            self.kwargs.get("delete_percentage_per_bucket"),
            self.kwargs.get("put_percentage_per_bucket"),
            self.kwargs.get("overwrite_percentage_per_bucket"),
            read_percentage_per_bucket=self.kwargs.get("read_percentage_per_bucket"),
        )
        return distribution
