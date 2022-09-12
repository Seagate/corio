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

"""Module to degrade cluster nodes."""
import logging
import os
import random
import time
from datetime import datetime
from time import perf_counter_ns

from munch import munchify

from config import CLUSTER_CFG, S3_ENDPOINT
from src.commons import constants as const
from src.commons.exception import NoBucketExistsException
from src.commons.utils.cluster_utils import ClusterServices
from src.libs.s3api.s3_bucket_ops import S3Bucket

LOGGER = logging.getLogger(const.ROOT)


def get_degraded_mode():
    """
    Get degraded mode.

    :return: Credentials Tuple.
    """
    try:
        LOGGER.info("Degraded pod count is %s", os.environ["DEGRADED_PODS_CNT"])
        LOGGER.info("Whether POD needs to be degraded %s", os.environ["DEGRADE_POD"])
        LOGGER.info("POD prefix which need to be degraded %s", os.environ["POD_PREFIX"])

    except KeyError as error:
        LOGGER.warning(error)
        logical_node = get_logical_node()
        degraded_pods_cnt = input("Enter Number of Pods to be degraded.\nDEGRADED_PODS_CNT: ")
        durability_val = logical_node.retrieve_durability_values()
        sns = {key: int(value) for key, value in durability_val.items()}
        LOGGER.debug("Durability Values (SNS) %s", sns["parity"])
        if degraded_pods_cnt > sns["parity"]:
            LOGGER.warning(
                "Taking MAX SNS value %s for degraded pod as given count is greater "
                "then supported value.",
                degraded_pods_cnt,
            )
            degraded_pods_cnt = sns["parity"]

        degrade_pod = input("Degrade pods with CORIO y/n, Default is n.\nDEGRADE_POD: ") or "n"
        if degrade_pod.lower() == "y":
            pod_prefix = input("Which pod you want to degraded data/server .\nPOD_PREFIX: ")
            pod_prefix = pod_prefix.lower()
            if pod_prefix not in ["data", "server"]:
                pod_prefix = "server"
            os.environ["POD_PREFIX"] = pod_prefix
        else:
            try:
                LOGGER.info("Name of degraded pods %s", os.environ["DEGRADED_PODS"])
            except KeyError as error:
                LOGGER.warning(error)
                degraded_pods = input("Enter name of degraded pods,or press enter key to skip")
                os.environ["DEGRADED_PODS"] = degraded_pods
        os.environ["DEGRADED_PODS_CNT"] = degraded_pods_cnt
        os.environ["DEGRADE_POD"] = degrade_pod


def activate_degraded_mode_parallel(return_dict, m_conf):
    """Create bucket and degrade node as needed .

    :param return_dict : dictionary to share data between parallel processes.
    :param m_conf : dictionary to fetch required info for parallel mode setup.
    :return : None
    """
    get_degraded_mode()
    logical_node = get_logical_node()
    os.environ["logical_node"] = str(logical_node)
    total_t = datetime.strptime(m_conf["degraded_io"]["pod_downtime_schedule"], "%H:%M:%S").time()
    downtime = total_t.hour * 3600 + total_t.minute * 60 + total_t.second
    total_t = datetime.strptime(m_conf["degraded_io"]["pod_uptime_schedule"], "%H:%M:%S").time()
    uptime = total_t.hour * 3600 + total_t.minute * 60 + total_t.second
    # Set default pod_prefix
    pod_prefix = const.SERVER_POD_NAME_PREFIX
    if os.environ["POD_PREFIX"].lower() == "data":
        pod_prefix = const.DATA_POD_NAME_PREFIX
    return_dict.update({"degraded_done": False})
    for cnt in range(m_conf["degraded_io"]["schedule_frequency"]):
        LOGGER.info("Degrading Cluster for %s count", cnt)
        LOGGER.info("Degraded Cluster Process Sleeping for %s sec before downtime.", downtime)
        time.sleep(downtime)
        return_dict.update({"is_deg_on": True})
        LOGGER.info("***** Making Degraded Cluster *****")
        resp = logical_node.degrade_nodes(pod_prefix)
        deleted_pods = resp[1]
        if not m_conf["degraded_io"]["keep_degraded"]:
            LOGGER.info("Degraded Cluster Process Sleeping for %s sec before uptime", uptime)
            time.sleep(uptime)
            LOGGER.info("Restoring From Degraded Cluster")
            restore_pod(deleted_pods=deleted_pods)
            return_dict.update({"is_deg_on": False})
            LOGGER.info("***** Cluster Back To Online *****")
    if m_conf["degraded_io"]["keep_degraded"]:
        try:
            while True:
                LOGGER.info(
                    "Degraded Cluster Process Sleeping for %s sec with %s pods down",
                    downtime,
                    os.environ["DEGRADED_PODS_CNT"],
                )
                time.sleep(downtime)
        except KeyboardInterrupt:
            pass
    return_dict.update({"degraded_done": True})


def activate_degraded_mode(options: munchify) -> None:
    """Activate the degraded mode."""
    bucket_obj = S3Bucket(
        access_key=options.access_key,
        secret_key=options.secret_key,
        endpoint_url=S3_ENDPOINT,
        use_ssl=options.use_ssl,
    )
    if os.getenv("DEGRADE_POD") == "n":
        bucket_list = bucket_obj.list_s3_buckets()
        if bucket_list:
            os.environ["d_bucket"] = random.choice(bucket_list)
        else:
            LOGGER.critical("No Bucket exist")
            raise NoBucketExistsException("Bucket need to be created first for degraded mode IOs")
    else:
        bucket = f"object-op-{perf_counter_ns()}".lower()
        resp = bucket_obj.create_s3_bucket(bucket)
        if resp:
            os.environ["d_bucket"] = bucket
    if os.environ["DEGRADE_POD"].lower() == "y":
        logical_node = get_logical_node()
        os.environ["logical_node"] = str(logical_node)
        if os.environ["POD_PREFIX"].lower() == "data":
            logical_node.degrade_nodes(const.DATA_POD_NAME_PREFIX)
        elif os.environ["POD_PREFIX"].lower() == "server":
            logical_node.degrade_nodes(const.SERVER_POD_NAME_PREFIX)
        else:
            LOGGER.warning("Incorrect pod prefix: %s", os.environ["POD_PREFIX"])


def get_logical_node() -> ClusterServices:
    """Create cluster services object and returns object."""
    master = [node for node in CLUSTER_CFG["nodes"] if node["node_type"] == "master"][-1]
    host, user, password = master["hostname"], master["username"], master["password"]
    logical_node = ClusterServices(host=host, user=user, password=password)
    return logical_node


def restore_pod(deleted_pods=None):
    """Restore pod which is already degraded."""
    logical_node = get_logical_node()
    if deleted_pods is not None:
        for pod in deleted_pods:
            logical_node.create_pod_replicas(num_replica=1, deploy=pod)
    else:
        logical_node.create_pod_replicas(num_replica=1, deploy=os.getenv("DEGRADED_PODS"))
