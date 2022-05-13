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

"""Python Library to perform object operations using aiobotocore module."""

import hashlib
import os
from typing import List

from src.libs.s3api.s3_restapi import S3RestApi


class S3Object(S3RestApi):
    """Class for object operations."""

    def __init__(self, *args, **kwargs):
        """Initializer for S3Object operations."""
        super().__init__(*args, **kwargs)
        self.s3_url = None

    async def upload_object(self, bucket: str, key: str, **kwargs) -> dict:
        """
        Uploading object to the Bucket, file_path or body is compulsory.

        :param bucket: Name of the bucket.
        :param key: Name of the object.
        :keyword file_path: Path of the file.
        :keyword body: Content of Object
        :return: Response of the upload s3 object.
        """
        body = kwargs.get('body', None)
        file_path = kwargs.get("file_path", None)
        if file_path:
            with open(file_path, "rb") as f_obj:
                body = f_obj.read()
        self.s3_url = f"s3://{bucket}/{key}"
        async with self.get_client() as s3client:
            response = await s3client.put_object(Body=body, Bucket=bucket, Key=key)
            self.log.info("upload_object %s Response: %s", self.s3_url, response)

        return response

    async def list_objects(self, bucket: str) -> list:
        """
        Listing Objects.

        :param bucket: Name of the bucket.
        :return: Response of the list objects.
        """
        objects = []
        async with self.get_client() as s3client:
            self.s3_url = f"s3://{bucket}"
            paginator = s3client.get_paginator('list_objects')
            async for result in paginator.paginate(Bucket=bucket):
                objects += [c['Key'] for c in result.get('Contents', [])]
        self.log.info("list_objects %s Objects: %s", self.s3_url, objects)

        return objects

    async def delete_object(self, bucket: str, key: str) -> dict:
        """
        Deleting object.

        :param bucket: Name of the bucket.
        :param key: Name of object.
        :return: Response of delete object.
        """
        async with self.get_client() as s3client:
            self.s3_url = f"s3://{bucket}/{key}"
            response = await s3client.delete_object(Bucket=bucket, Key=key)
            self.log.info("delete_object %s Response: %s", self.s3_url, response)

        return response

    async def delete_objects(self, bucket: str, keys: List[str]) -> dict:
        """
        Deleting object.

        :param bucket: Name of the bucket.
        :param keys: List of object names.
        :return: Response of delete object.
        """
        objects = [{'Key': key} for key in keys]
        self.log.info("Deleting %s", keys)
        async with self.get_client() as s3client:
            self.s3_url = f"s3://{bucket}"
            response = await s3client.delete_objects(Bucket=bucket, Delete={'Objects': objects})
            self.log.info("delete_objects %s Response: %s", self.s3_url, response)

        return response

    async def head_object(self, bucket: str, key: str) -> dict:
        """
        Retrieve metadata from an object without returning the object itself.

        :param bucket: Name of the bucket.
        :param key: Name of object.
        :return: Response of head object.
        """
        async with self.get_client() as s3client:
            self.s3_url = f"s3://{bucket}/{key}"
            response = await s3client.head_object(Bucket=bucket, Key=key)
            self.log.info("head_object s3://%s/%s Response: %s", bucket, key, response)

        return response

    async def get_object(self, bucket: str, key: str, ranges: str = None) -> dict:
        """
        Getting object or byte range of the object.

        :param bucket: Name of the bucket.
        :param key: Name of object.
        :param ranges: Byte range to be retrieved
        :return: response.
        """
        async with self.get_client() as s3client:
            self.s3_url = f"s3://{bucket}/{key}"
            if ranges:
                response = await s3client.get_object(Bucket=bucket, Key=key, Range=ranges)
            else:
                response = await s3client.get_object(Bucket=bucket, Key=key)
            self.log.info("get_object %s Response: %s", self.s3_url, response)

        return response

    async def download_object(self, bucket: str, key: str, file_path: str,
                              chunk_size: int = 1024) -> dict:
        """
        Downloading Object of the required Bucket.

        :param bucket: Name of the bucket.
        :param key: Name of object.
        :param file_path: Path of the file.
        :param chunk_size: Download object in chunk sizes.
        :return: Response of download object.
        """
        async with self.get_client() as s3client:
            self.s3_url = f"s3://{bucket}/{key}"
            response = await s3client.get_object(Bucket=bucket, Key=key)
            self.log.info("download_object %s Response %s", self.s3_url, response)
            async with response['Body'] as stream:
                chunk = await stream.read(chunk_size)
                self.log.debug("Reading chunk length: %s", len(chunk))
                while len(chunk) > 0:
                    with open(file_path, "wb+") as file_obj:
                        file_obj.write(chunk)
                    chunk = await stream.read(chunk_size)
        if os.path.exists(file_path):
            self.log.info("download_object %s Path: %s Response %s", self.s3_url, file_path,
                          response)

        return response

    async def copy_object(self, src_bucket: str, src_key: str, des_bucket: str, des_key: str,
                          **kwargs) -> dict:
        """
        Create a copy of an object that is already stored in S3.

        :param src_bucket: The name of the source bucket.
        :param src_key: The name of the source object.
        :param des_bucket: The name of the destination bucket.
        :param des_key: The name of the destination object.
        :return: Response of copy object.
        """
        async with self.get_client() as s3client:
            self.s3_url = f"s3://{src_bucket}/{src_key} to s3://{des_bucket}/{des_key}"
            response = await s3client.copy_object(Bucket=des_bucket,
                                                  CopySource=f'/{src_bucket}/{src_key}',
                                                  Key=des_key, **kwargs)
            self.log.info("copy_object: %s,  Response %s", self.s3_url, response)

        return response

    async def get_s3object_checksum(self, bucket: str, key: str, chunk_size: int = 1024,
                                    ranges: str = None) -> str:
        """
        Read object in chunk and calculate md5sum.

        Do not store the object in local storage.
        :param bucket: The name of the s3 bucket.
        :param key: Name of object.
        :param chunk_size: size to read the content of s3 object.
        :param ranges: number of bytes to be read
        """
        async with self.get_client() as s3client:
            self.s3_url = f"s3://{bucket}/{key}"
            if ranges:
                response = await s3client.get_object(Bucket=bucket, Key=key, Range=ranges)
            else:
                response = await s3client.get_object(Bucket=bucket, Key=key)
            self.log.info("get_s3object_checksum %s Response %s", self.s3_url, response)
            async with response['Body'] as stream:
                chunk = await stream.read(chunk_size)
                file_hash = hashlib.sha256()
                self.log.debug("Reading chunk length: %s", len(chunk))
                while len(chunk) > 0:
                    file_hash.update(chunk)
                    chunk = await stream.read(chunk_size)
        sha256_digest = file_hash.hexdigest()
        self.log.debug("get_s3object_checksum %s, SHA-256: %s", self.s3_url, sha256_digest)

        return sha256_digest

    def checksum_file(self, file_path: str, chunk_size: int = 1024 * 1024):
        """
        Calculate checksum of given file_path by reading file chunk_size at a time.

        :param file_path: Local file path
        :param chunk_size: single chunk size to read the content of given file
        """
        with open(file_path, 'rb') as f_obj:
            file_hash = hashlib.sha256()
            chunk = f_obj.read(chunk_size)
            self.log.debug("Reading chunk length: %s", len(chunk))
            while len(chunk) > 0:
                file_hash.update(chunk)
                chunk = f_obj.read(chunk_size)
                self.log.debug("Reading chunk length: %s", len(chunk))
        return file_hash.hexdigest()

    def checksum_part_file(self, file_path: str, offset: int, read_size: int,
                           chunk_size: int = 1024 * 1024):
        """
        Calculate checksum of read_size bytes starting from offset in given file_path.

        Uses chunk_size=1MB if read_size > 1MB
        :param: file_path: File location
        :param: offset: Offset to start reading from
        :param: read_size: Size in bytes to read from offset
        :param: chunk_size: Single chunk size in bytes to read
        """
        file_size = os.path.getsize(file_path)
        if file_size < offset + read_size:
            raise IOError(f"{offset + read_size} is less than file size {file_size} ")
        chunk_size = read_size if read_size < chunk_size else chunk_size
        self.log.info("Reading size is %s", chunk_size)
        file_hash = hashlib.sha256()
        read_length = read_size
        with open(file_path, 'rb') as f_obj:
            f_obj.seek(offset)
            while read_length:
                current_read_length = chunk_size if read_length >= chunk_size else read_length
                self.log.debug("Reading %s from starting offset %s", current_read_length,
                               f_obj.tell())
                content = f_obj.read(current_read_length)
                file_hash.update(content)
                read_length -= len(content)

        return file_hash.hexdigest()
