#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Seagate Technology LLC and/or its Affiliates
#
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
#
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#
#

"""Unittest to test s3 parallel io ops lib."""

import sys
import unittest

from src.libs.s3api.parallel_io import S3ApiParallelIO


class TestS3ApiParallelIO(unittest.TestCase):
    """Tests suite for S3ApiParallelIO."""

    if len(sys.argv) >= 4:
        access_key = sys.argv[1]
        secret_key = sys.argv[2]
        endpoint = sys.argv[3]
        use_ssl = sys.argv[4]
    else:
        raise AssertionError(
            "Please provide access_key, secret_key, endpoint and "
            "use_ssl parameters in order to run execution."
        )

    @classmethod
    def setUpClass(cls) -> None:
        """Setup class method."""
        cls.s3obj = S3ApiParallelIO(
            access_key=cls.access_key,
            secret_key=cls.secret_key,
            endpoint_url=cls.endpoint,
            use_ssl=cls.use_ssl,
            test_id="UnitTest",
        )
        cls.object_sizes = [1024, 2048, 4096]
        cls.write_distribution = {1024: 115, 2048: 100, 4096: 225}
        cls.read_distribution = {1024: 200, 2048: 150, 4096: 250}
        cls.validate_distribution = {1024: 100, 2048: 50, 4096: 210}
        cls.partial_del_distribution = {1024: 100, 2048: 50, 4096: 210}

    def test_1_write_data(self):
        """Test write distribution."""
        self.s3obj.execute_workload(
            operations="write", sessions=5, distribution=self.write_distribution
        )
        for bucket in list(self.s3obj.io_ops_dict):
            for object_size, samples in self.write_distribution.items():
                if str(object_size) in bucket:
                    assert (
                        len(self.s3obj.io_ops_dict[bucket]) == samples
                    ), f"failed write distribution: {object_size}:{samples}"

    def test_2_read_data(self):
        """Test read distribution."""
        self.s3obj.execute_workload(
            operations="read",
            sessions=5,
            distribution=self.read_distribution,
            validate=True,
        )
        for bucket in list(self.s3obj.read_files):
            for object_size, samples in self.read_distribution.items():
                if str(object_size) in bucket:
                    self.s3obj.log.info(self.s3obj.read_files[bucket])
                    assert (
                        self.s3obj.read_files[bucket]["total_count"] == samples
                    ), f"failed read distribution: {object_size}:{samples}"

    def test_3_validate_data(self):
        """Test validate data."""
        self.s3obj.execute_workload(
            operations="validate", sessions=5, distribution=self.validate_distribution
        )
        for bucket in list(self.s3obj.validated_files):
            for object_size, samples in self.validate_distribution.items():
                if str(object_size) in bucket:
                    assert (
                        self.s3obj.validated_files[bucket]["total_count"] == samples
                    ), f"failed read distribution: {object_size}:{samples}"

    def test_4_partial_delete(self):
        """Test partial delete."""
        self.s3obj.execute_workload(
            operations="delete", sessions=5, distribution=self.partial_del_distribution
        )
        for bucket in list(self.s3obj.deleted_files):
            for object_size, samples in self.partial_del_distribution.items():
                if str(object_size) in bucket:
                    assert (
                        self.s3obj.deleted_files[bucket]["total_count"] == samples
                    ), f"failed validate distribution: {object_size}:{samples}"

    def test_5_complete_delete(self):
        """Test complete delete."""
        distribution = {}
        for bucket in list(self.s3obj.io_ops_dict):
            for object_size in self.object_sizes:
                if str(object_size) in bucket:
                    distribution[object_size] = len(self.s3obj.io_ops_dict[bucket])
        self.s3obj.log.info(distribution)
        self.s3obj.execute_workload(operations="delete", sessions=5, distribution=distribution)
        for bucket in list(self.s3obj.io_ops_dict):
            assert (
                len(self.s3obj.io_ops_dict[bucket]) == 0
            ), f"Failed to complete data from {bucket}"

    def test_6_cleanup(self):
        """Test cleanup."""
        self.s3obj.execute_workload(operations="cleanup", sessions=3)
        list_buckets = self.s3obj.list_s3_buckets()
        self.s3obj.log.info(list_buckets)
        assert len(list_buckets) == 0, f"Failed to cleanup data: {list_buckets}"


if __name__ == "__main__":
    test_loader = unittest.TestLoader()
    test_names = test_loader.getTestCaseNames(TestS3ApiParallelIO)

    suite = unittest.TestSuite()
    for test_name in test_names:
        suite.addTest(TestS3ApiParallelIO(test_name))

    result = unittest.TextTestRunner().run(suite)
    sys.exit(not result.wasSuccessful())
