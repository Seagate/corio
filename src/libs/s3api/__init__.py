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

"""s3api package."""


from src.libs.s3api.s3_bucket_ops import S3Bucket
from src.libs.s3api.s3_multipart_ops import S3MultiParts
from src.libs.s3api.s3_object_ops import S3Object


class S3Api(S3Bucket, S3Object, S3MultiParts):
    """Common class for all aiobotocore, boto3 api."""

    def __int__(self, *args, **kwargs):
        """Initialize members of S3Api."""
        super().__init__(*args, **kwargs)

    def __repr__(self):
        """Representation of an S3API object."""
        return "S3RestApi for s3 operations using aiobotocore and boto3."
