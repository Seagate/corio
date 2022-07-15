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
"""Module to map script with workload."""

from scripts.s3.mixs3io import mix_object_operations
from scripts.s3.s3api import bucket_operations
from scripts.s3.s3api import copy_object
from scripts.s3.s3api import multipart_operations
from scripts.s3.s3api import object_operations

# mapping_dict = {operation_name_from_workload: [script.class, function_name],}
SCRIPT_MAPPING = {
    'copy_object': [copy_object.TestS3CopyObjects, 'execute_copy_object_workload'],
    'copy_object_range_read': [copy_object.TestS3CopyObjects, 'execute_copy_object_workload'],
    'bucket': [bucket_operations.TestBucketOps, 'execute_bucket_workload'],
    'multipart': [multipart_operations.TestMultiParts, 'execute_multipart_workload'],
    'multipart_partcopy': [multipart_operations.TestMultiParts, 'execute_multipart_workload'],
    'multipart_range_read': [multipart_operations.TestMultiParts, 'execute_multipart_workload'],
    'multipart_partcopy_range_read': [multipart_operations.TestMultiParts,
                                      'execute_multipart_workload'],
    'multipart_partcopy_random': [multipart_operations.TestMultiParts,
                                  'execute_multipart_workload'],
    'multipart_random': [multipart_operations.TestMultiParts, 'execute_multipart_workload'],
    'object_random_size': [object_operations.TestS3Object, 'execute_object_workload'],
    'object_fix_size': [object_operations.TestS3Object, 'execute_object_workload'],
    'object_range_read': [object_operations.TestS3Object, 'execute_object_workload'],
    'copy_object_fix_size': [copy_object.TestS3CopyObjects, 'execute_copy_object_workload'],
    'mix_object_ops': [mix_object_operations.TestMixObjectOps, 'execute_mix_object_workload']
}
