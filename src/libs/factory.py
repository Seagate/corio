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

"""Python Library to implement factory pattern for tools."""


class ToolsFactory:
    """Tools Factory."""

    def __init__(self, tool):
        """Initialize tools factory."""
        self.tool = tool

    def __str__(self):
        """Object representation."""
        return f'Tool name is {self.tool}'

    def __repr__(self):
        """Object name."""
        return f'Tool:{self.tool}'

    def __call__(self):
        """Get Created object."""
        return self.get_tool_object()

    def get_tool_object(self):
        """Create tools objects as per client requirements."""
        target_tool = self.tool.capitalize()
        return globals()[target_tool]()
