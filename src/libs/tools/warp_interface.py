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

"""Warp Interface."""
from src.libs.tools.warp import Warp


# The existing interface, Adaptee
class WarpInterface(Warp):
    """S3bench Interface."""

    def __init__(self, operation: str = None, access: str = None, secret: str = None):
        """Initialize warp interface."""
        super().__init__(operation, access, secret)

    def __str__(self):
        """Object Representation."""
        return "warp"

    def run(self):
        """Run warp."""
        status, _ = self.execute_workload()
        return status
