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
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#

"""S3bench Library for IO driver."""
import random

from src.libs.tools.s3bench import S3bench


# The existing interface, Adaptee
class S3benchInterface(S3bench):
    """S3bench Interface"""

    def __init__(self,access=None, secret=None, test_id=None):
        super().__init__(access, secret, test_id, seed=random.randint(1, 100))

    def __str__(self):
        """Object Representation"""
        return 's3bench'

    def run(self):
        """run warp
        """
        # status, _ = self.execute_s3bench_workload()
        # return status
        return True
