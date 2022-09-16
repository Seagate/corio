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
"""Script type5 s3 object operation negative scenario workload for io stability."""

import random
from datetime import timedelta, datetime
from time import perf_counter_ns

from botocore.exceptions import ClientError

from src.commons.constants import MIN_DURATION
from src.libs.s3api import S3Api


class TestType5ObjectOpsNegative(S3Api):
    """S3 objects type5 operations negative scenario class."""

    def __init__(
        self, access_key: str, secret_key: str, endpoint_url: str, test_id: str, **kwargs
    ) -> None:
        """
        s3 objects operations negative scenario init class.

        :param access_key: access key.
        :param secret_key: secret key.
        :param endpoint_url: endpoint with http or https.
        :keyword test_id: Test ID string.
        :keyword use_ssl: To use secure connection.
        :keyword seed: Seed to be used for random data generator
        :keyword session: session name.
        :keyword sessions: Number of sessions used for IO.
        :keyword duration: Duration timedelta object, if not given will run for 100 days.
        """
        super().__init__(
            access_key,
            secret_key,
            endpoint_url=endpoint_url,
            use_ssl=kwargs.get("use_ssl"),
            test_id=f"{kwargs.get('test_id')}_object_negative",
        )
        random.seed(kwargs.get("seed"))
        self.test_id = test_id
        self.kwargs = kwargs
        self.session_id = kwargs.get("session")
        self.finish_time = datetime.now() + kwargs.get("duration", timedelta(hours=int(100 * 24)))

    async def execute_object_ops_negative_workload(self):
        """Execute object operation negative workload for specific duration in parallel."""
        iteration = 1
        while True:
            bucket = f"object-op-{self.test_id}-{perf_counter_ns()}".lower()
            self.log.info("Create bucket %s", bucket)
            resp = await self.create_bucket(bucket)
            self.log.info("Bucket created %s", resp)
            self.log.info("Iteration %s is started for %s...", iteration, self.session_id)
            object_key = f"object-op-{perf_counter_ns()}"
            try:
                resp = await self.get_object(bucket=bucket, key=object_key)
                assert False, (
                    f"Expected failure in GetObject API for "
                    f"non existing object:{object_key}, resp: {resp}"
                )
            except ClientError as err:
                self.log.info("Get Object exception for non existing object %s", err)
            try:
                resp = await self.get_object(bucket=bucket, key=object_key, ranges="bytes=0-9")
                assert False, (
                    f"Expected failure in GetObject range read for "
                    f"non existing object:{object_key}, resp: {resp}"
                )
            except ClientError as err:
                self.log.info("Get Object range read exception for non existing object %s", err)
            try:
                resp = await self.delete_object(bucket, object_key)
                assert False, (
                    f"Expected failure in DeleteObject API for "
                    f"non existing object:{object_key}, resp: {resp}"
                )
            except ClientError as err:
                self.log.info("Delete Object exception for non existing object %s", err)
            await self.delete_bucket(bucket, True)
            if (self.finish_time - datetime.now()).total_seconds() < MIN_DURATION:
                return True, "Object operation negative execution completed successfully."
            iteration += 1
