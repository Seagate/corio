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

"""Libraries package."""


from src.libs.restapi.iam_ops import IAMClient
from src.libs.s3api.iam import IAMUserAPI


# pylint: disable=broad-except
class IAMInterface(IAMClient, IAMUserAPI):
    """IAM Interface class for IAM user operations using rest or boto3 api."""

    async def create_s3iam_user(self, user_name):
        """Create s3 iam user using rest or boto api."""
        try:
            response = await self.create_iam_user(user_name)
        except Exception as err:
            self.log.warning(err)
            response = IAMClient().create_user(user_name)
        self.log.info(response)
        return response

    async def delete_s3iam_user(self, user_name):
        """Delete s3 iam user using rest or boto api."""
        try:
            response = await self.delete_iam_user(user_name)
        except Exception as err:
            self.log.warning(err)
            response = IAMClient().delete_user(user_name)
        self.log.info(response)
        return response
