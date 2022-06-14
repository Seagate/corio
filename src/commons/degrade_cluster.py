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
import random
from time import perf_counter_ns

from munch import munchify

from config import CLUSTER_CFG, S3_ENDPOINT
from src.commons.constants import DATA_POD_NAME_PREFIX, SERVER_POD_NAME_PREFIX
from src.commons.constants import ROOT
from src.commons.exception import NoBucketExistsException
from src.commons.utils.cluster_utils import ClusterServices
from src.libs.s3api.s3_bucket_ops import S3Bucket

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
        LOGGER.warning(error)
        degraded_pods_cnt = input("Enter Number of Pods to be degraded.\nDEGRADED_PODS_CNT: ")
        degrade_pod = input("Degrade pods with CORIO y/n, Default is n.\nDEGRADE_POD: ") or "n"
        if degrade_pod.lower() == 'y':
            pod_prefix = input("Which pod you want to degraded data/server .\nPOD_PREFIX: ")
            os.environ['POD_PREFIX'] = pod_prefix
        else:
            try:
                LOGGER.info("Name of degraded pods %s", os.environ['DEGRADED_PODS'])
            except KeyError as error:
                LOGGER.warning(error)
                degraded_pods = input("Enter name of degraded pods,or press enter key to skip")
                os.environ['DEGRADED_PODS'] = degraded_pods
        os.environ['DEGRADED_PODS_CNT'] = degraded_pods_cnt
        os.environ['DEGRADE_POD'] = degrade_pod


def activate_degraded_mode(options: munchify):
    """Create bucket and degrade node as needed .

    :param options : dictionary to fetch command line params
    :return : None
    """
    get_degraded_mode()
    bucket_obj = S3Bucket(access_key=options.access_key,
                          secret_key=options.secret_key,
                          endpoint_url=S3_ENDPOINT,
                          use_ssl=options.use_ssl)
    if os.getenv('DEGRADE_POD') == 'n':
        bucket_list = bucket_obj.list_s3_buckets()
        if bucket_list:
            os.environ["d_bucket"] = random.choice(bucket_list)
        else:
            LOGGER.critical("No Bucket exist")
            raise NoBucketExistsException("Bucket need to be created first for degraded mode IOs")
    else:
        bucket = f'object-op-{perf_counter_ns()}'.lower()
        resp = bucket_obj.create_s3_bucket(bucket)
        if resp:
            os.environ["d_bucket"] = bucket
    if os.environ['DEGRADE_POD'].lower() == 'y':
        logical_node = get_logical_node()
        os.environ["logical_node"] = str(logical_node)
        if os.environ['POD_PREFIX'].lower() == 'data':
            pod_prefix = DATA_POD_NAME_PREFIX
        elif os.environ['POD_PREFIX'].lower() == 'server':
            pod_prefix = SERVER_POD_NAME_PREFIX
        logical_node.degrade_nodes(pod_prefix)


def get_logical_node() -> object:
    """Create cluster services object and returns object"""
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
    return logical_node


def restore_pod():
    """ Restore pod which is already degraded."""
    logical_node = get_logical_node()
    logical_node.create_pod_replicas(num_replica=1, deploy=os.getenv('DEGRADED_PODS'))
