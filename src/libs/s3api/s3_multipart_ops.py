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

"""Python Library to perform multipart operations using aiobotocore module."""

from src.commons.utils.corio_utils import retries
from src.libs.s3api.s3_restapi import S3RestApi


class S3MultiParts(S3RestApi):
    """Class for Multipart operations."""

    def __init__(self, *args, **kwargs):
        """Initialize S3MultiParts operations."""
        super().__init__(*args, **kwargs)
        self.s3_url = None

    @retries()
    async def create_multipart_upload(self, bucket_name: str, obj_name: str) -> dict:
        """
        Request to initiate a multipart upload.

        :param bucket_name: Name of the bucket.
        :param obj_name: Name of the object.
        :return: Response of create multipart upload.
        """
        async with self.get_client() as client:
            self.s3_url = s3_url = f"s3://{bucket_name}/{obj_name}"
            response = await client.create_multipart_upload(Bucket=bucket_name, Key=obj_name)
            self.log.info("create_multipart_upload: %s, Response: %s", s3_url, response)

        return response

    @retries()
    async def upload_part(self, body: bytes, bucket_name: str, object_name: str, **kwargs) -> dict:
        """
        Upload parts of a specific multipart upload.

        :param body: content of the object.
        :param bucket_name: Name of the bucket.
        :param object_name: Name of the object.
        :upload_id: upload id of the multipart upload.
        :part_number: part number to be uploaded.
        :return: response of upload part.
        """
        upload_id = kwargs.get("upload_id")
        part_number = kwargs.get("part_number")
        async with self.get_client() as client:
            self.s3_url = s3_url = f"s3://{bucket_name}/{object_name}"
            response = await client.upload_part(
                Body=body,
                Bucket=bucket_name,
                Key=object_name,
                UploadId=upload_id,
                PartNumber=part_number,
            )
            self.log.info(
                "upload_part: %s, UploadID: %s, PartNumber: %s, Response: %s",
                s3_url,
                upload_id,
                part_number,
                response,
            )

        return response

    @retries()
    async def list_parts(self, mpu_id: str, bucket_name: str, object_name: str) -> list:
        """
        List parts of a specific multipart upload.

        :param mpu_id: Multipart upload ID.
        :param bucket_name: Name of the bucket.
        :param object_name: Name of the object.
        :return: Response of list parts.
        """
        parts = []
        async with self.get_client() as client:
            paginator = client.get_paginator("list_parts")
            self.s3_url = s3_url = f"s3://{bucket_name}/{object_name}"
            async for result in paginator.paginate(
                Bucket=bucket_name, Key=object_name, UploadId=mpu_id
            ):
                for content in result.get("Parts", []):
                    parts.append(content)
            self.log.info("list_parts: %s, parts: %s", s3_url, parts)

        return parts

    @retries()
    async def complete_multipart_upload(
        self, mpu_id: str, parts: list, bucket: str, object_name: str
    ) -> dict:
        """
        Complete a multipart upload, s3 creates an object by concatenating the parts.

        :param mpu_id: Multipart upload ID.
        :param parts: Uploaded parts in sorted ordered.
        :param bucket: Name of the bucket.
        :param object_name: Name of the object.
        :return: response of complete multipart upload.
        """
        self.log.info("initiated complete multipart upload")
        async with self.get_client() as client:
            self.s3_url = s3_url = f"s3://{bucket}/{object_name}"
            response = await client.complete_multipart_upload(
                Bucket=bucket,
                Key=object_name,
                UploadId=mpu_id,
                MultipartUpload={"Parts": parts},
            )
            self.log.info("complete_multipart_upload: %s, response: %s", s3_url, response)

        return response

    @retries()
    async def list_multipart_uploads(self, bucket_name: str) -> list:
        """
        List all initiated multipart uploads.

        :param bucket_name: Name of the bucket.
        :return: response of list multipart uploads.
        """
        uploads = []
        async with self.get_client() as client:
            self.s3_url = s3_url = f"s3://{bucket_name}"
            paginator = client.get_paginator("list_multipart_uploads")
            async for result in paginator.paginate(Bucket=bucket_name):
                for content in result.get("Uploads", []):
                    uploads.append(content)
            self.log.info("list_multipart_uploads: %s, Uploads: %s", s3_url, uploads)

        return uploads

    @retries()
    async def abort_multipart_upload(
        self, bucket_name: str, object_name: str, upload_id: str
    ) -> dict:
        """
        Abort multipart upload for given upload_id.

        :param bucket_name: Name of the bucket.
        :param object_name: Name of the object.
        :param upload_id: Name of the object.
        :return: Response of abort multipart upload.
        """
        async with self.get_client() as client:
            self.s3_url = s3_url = f"s3://{bucket_name}/{object_name}"
            response = await client.abort_multipart_upload(
                Bucket=bucket_name, Key=object_name, UploadId=upload_id
            )
            self.log.info("abort_multipart_upload: %s, Response: %s", s3_url, response)

        return response

    @retries()
    async def upload_part_copy(
        self, copy_source: str, bucket_name: str, object_name: str, **kwargs
    ) -> dict:
        """
        Upload parts of a specific multipart upload from existing object.

        :param copy_source: source of part copy.
        :param bucket_name: Name of the bucket.
        :param object_name: Name of the object.
        :upload_id: upload id of the multipart upload.
        :part_number: part number to be uploaded.
        :return: response of the upload part copy.
        """
        upload_id = kwargs.get("upload_id")
        part_number = kwargs.get("part_number")
        async with self.get_client() as client:
            self.s3_url = s3_url = f"s3://{bucket_name}/{object_name}"
            response = await client.upload_part_copy(
                Bucket=bucket_name,
                Key=object_name,
                UploadId=upload_id,
                PartNumber=part_number,
                CopySource=copy_source,
            )
            self.log.info(
                "upload_part_copy: copy source: %s to %s, Response: %s",
                copy_source,
                s3_url,
                response,
            )

        return response
