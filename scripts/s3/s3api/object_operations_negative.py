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
import time
from datetime import datetime, timedelta

from src.commons.constants import MIN_DURATION
from src.libs.s3api.s3io_utils import S3ApiIOUtils


class TestType5ObjectOpsNegative(S3ApiIOUtils):
    """S3 objects type5 operations negative scenario class."""

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, **kwargs) -> None:
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
            test_id=f"{kwargs.get('test_id')}_object_operation_negative",
        )
        random.seed(kwargs.get("seed"))
        self.kwargs = kwargs
        self.finish_time = datetime.now() + kwargs.get("duration", timedelta(hours=int(100 * 24)))

    async def execute_object_ops_negative_workload(self):
        """Execute object operation negative workload for specific duration in parallel."""
        buckets = await self.create_n_buckets("objects-ops-neg",
                                              self.kwargs.get("number_of_buckets"))
        self.log.info("Bucket list: %s", buckets)
        try:
            iteration = 1
            sessions = self.kwargs.get("sessions")
            self.log.info("Iteration %s is started.", iteration)
            getobject
            delete
            range read
        except Exception as err:
            assert err.message in



