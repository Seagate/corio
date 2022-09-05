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

"""Library for s3 IAM operations using asyncio."""

from src.libs.s3api.s3_restapi import S3RestApi


class IAMUserAPI(S3RestApi):
    """IAM user api using asyncio."""

    async def create_user(self, user_name: str) -> dict:
        """Create IAM user."""
        async with self.get_client(service_name="iam") as client:
            response = await client.create_user(UserName=user_name)
            self.log.debug(response)
        return response

    async def list_users(self) -> dict:
        """List all IAM users."""
        async with self.get_client(service_name="iam") as client:
            response = await client.list_users()
            self.log.debug(response)
        return response

    async def delete_user(self, user_name: str = None) -> dict:
        """
        Delete IAM user.

        :param user_name: Name of the IAM user.
        """
        async with self.get_client(service_name="iam") as client:
            response = await client.delete_user(UserName=user_name)
        return response

    async def update_user(self, new_user_name: str = None, user_name: str = None) -> dict:
        """
        Update name of the IAM user.

        :param new_user_name: New name of the IAM user.
        :param user_name: Old name of the IAM user.
        """
        async with self.get_client(service_name="iam") as client:
            response = await client.update_user(NewUserName=new_user_name, UserName=user_name)
            self.log.debug(response)
        return response

    async def create_access_key(self, user_name: str = None) -> dict:
        """
        Create access key for given IAM user.

        :param user_name: Name of the IAM user.
        """
        async with self.get_client(service_name="iam") as client:
            response = await client.create_access_key(UserName=user_name)
            self.log.debug(response)
        return response

    async def list_access_keys(self, user_name: str = None) -> dict:
        """
        List access keys of given IAM user.

        :param user_name: Name of the IAM user.
        """
        async with self.get_client(service_name="iam") as client:
            response = await client.list_access_keys(UserName=user_name)
            self.log.debug(response)
        return response

    async def delete_access_key(self, user_name: str = None, access_key_id: str = None) -> dict:
        """
        Delete access key of given IAM user.

        :param user_name: Name of the IAM user.
        :param access_key_id: Key to delete for IAM user.
        """
        async with self.get_client(service_name="iam") as client:
            response = await client.delete_access_key(AccessKeyId=access_key_id, UserName=user_name)
            self.log.debug(response)
        return response

    async def create_user_login_profile(
        self, user_name: str = None, password: str = None, password_reset: bool = False
    ) -> dict:
        """
        Create IAM user login profile.

        :param user_name: Name of the IAM user.
        :param password: Password of the IAM user.
        :param password_reset: True/False
        """
        async with self.get_client(service_name="iam") as client:
            response = await client.create_login_profile(
                UserName=user_name, Password=password, PasswordResetRequired=password_reset
            )
            self.log.debug(response)
        return response

    async def delete_user_login_profile(self, user_name: str) -> dict:
        """
        Delete the login profile of the specified IAM user.

        :param user_name: The name of the iam user whose password you want to delete.
        """
        async with self.get_client(service_name="iam") as client:
            response = await client.delete_login_profile(UserName=user_name)
            self.log.debug(response)
        return response

    async def create_iam_user(self, user_name: str, user_password="qawSdm!2864@hnr") -> dict:
        """Create IAM user, login profile and access key."""
        response = {}
        # Create iam user.
        resp = await self.create_user(user_name)
        response.update(resp)
        # Create login profile for iam user.
        resp = await self.create_user_login_profile(user_name, user_password)
        response.update(resp)
        # Generated access key for iam user.
        resp = await self.create_access_key(user_name)
        response.update(resp)
        return response

    async def delete_iam_user(self, user_name: str = None) -> dict:
        """
        Delete given IAM user along with all access keys and login profile.

        :param user_name: Name of the IAM user.
        """
        async with self.get_client(service_name="iam") as client:
            response = {}
            # Delete all access keys.
            resp = await self.list_access_keys(user_name)
            for ele in resp["AccessKeyMetadata"]:
                await self.delete_access_key(user_name, ele["AccessKeyId"])
            # Delete user login profile.
            resp = await self.delete_user_login_profile(user_name)
            response.update(resp)
            # Delete user.
            resp = await client.delete_user(UserName=user_name)
            response.update(resp)
        return response
