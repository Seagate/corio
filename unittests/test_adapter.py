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

"""Unit Test Class to check adapter interface."""
import unittest
from src.libs.adapter import Adapter
from src.libs.tools.warpInterface import WarpInterface
from src.libs.tools.s3benchInterface import S3benchInterface


class AdapterTest(unittest.TestCase):
    """Adapter Test Class."""

    @staticmethod
    def test_warp_adapter():
        """List to store objects."""
        objects = []
        iotool = WarpInterface()
        adapter = Adapter(iotool, execution=iotool.run)
        objects.append(adapter)
        adapter.execute()

    @staticmethod
    def test_s3bench_adapter():
        """List to store objects."""
        objects = []
        iotool = S3benchInterface()
        adapter = Adapter(iotool, execution=iotool.run)
        objects.append(adapter)
        adapter.execute()

    @staticmethod
    def test_adapter():
        """Test s3bench and Warp Together."""
        objects = []
        warp = WarpInterface()
        objects.append(Adapter(warp, execution=warp.run))
        s3bench = S3benchInterface()
        objects.append(Adapter(s3bench, execution=s3bench.run))
        for obj in objects:
            print("{obj} is {} IO",obj.run())

    @staticmethod
    def test_adapter_interface():
        """ Test S3bench interface."""
        objects = []
        iotool = S3benchInterface()
        adapter = Adapter(iotool, execution=iotool.run)
        objects.append(adapter)
        print(adapter)


if __name__ == '__main__':
    unittest.main()
