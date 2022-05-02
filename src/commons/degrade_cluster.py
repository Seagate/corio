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

"""Module to degrade cluster nodes"""
import logging
import os
import time

from config import CLUSTER_CFG
from src.commons.constants import ROOT
from src.commons.utils.cluster_utils import ClusterServices
from src.libs.s3api.s3_bucket_ops import S3Bucket
from src.commons.constants import DATA_POD_NAME_PREFIX, SERVER_POD_NAME_PREFIX
LOGGER = logging.getLogger(ROOT)


def get_degraded_mode():
    """
    Function to get degraded mode.

    :return: Credentials Tuple.
    """
    try:
        LOGGER.info("Degraded pod count is %s", os.environ['DEGRADED_PODS_CNT'])
        LOGGER.info("Whether POD needs to be degraded %s", os.environ['DEGRADE_POD'])
        LOGGER.info("POD prefix which need to be degraded %s", os.environ['POD_PREFIX'])

    except KeyError as error:
        LOGGER.error(error)
        degraded_pods = input("Enter Number of Pods to be degraded.\nDEGRADED_PODS: ")
        degrade_pod = input("Do you want to degraded pods with this tool y/n.\nDEGRADE_POD: ")
        if degrade_pod.lower() == 'y':
            pod_prefix = input("Which pod you want to degraded data/server .\nPOD_PREFIX: ")
        os.environ['DEGRADED_PODS_CNT'] = degraded_pods
        os.environ['DEGRADE_POD'] = degrade_pod
        os.environ['POD_PREFIX'] = pod_prefix


def activate_degraded_mode(options: dict):
    """Create bucket and degrade node as needed .

    :param options : dictionary to fetch command line params
    :return : None
    """
    get_degraded_mode()
    bucket = f'object-op-{time.perf_counter_ns()}'.lower()
    response = S3Bucket(access_key=options.access_key,
                        secret_key=options.secret_key,
                        endpoint_url=options.endpoint_url,
                        use_ssl=options.use_ssl).create_bucket(bucket)
    if response:
        os.environ["d_bucket"] = bucket

    if os.environ['DEGRADE_POD'].lower() == 'y':
        nodes = CLUSTER_CFG["nodes"]
        (host, user, password) = (None, None, None)
        for node in nodes:
            if node["node_type"] == "master":
                host = node["hostname"]
                user = node["username"]
                password = node["password"]
                break
        logical_node = ClusterServices(host=host,
                                       user=user,
                                       password=password)
        if os.environ['POD_PREFIX'].lower() == 'data':
            pod_prefix = DATA_POD_NAME_PREFIX
        elif os.environ['POD_PREFIX'].lower() == 'server':
            pod_prefix = SERVER_POD_NAME_PREFIX
        logical_node.degrade_nodes(pod_prefix)
