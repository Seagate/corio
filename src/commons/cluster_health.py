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

"""Module to check cluster health and storage."""

import time
import logging
from config import CLUSTER_CFG
from src.commons.constants import ROOT
from src.commons.exception import HealthCheckError
from src.commons.utils.cluster_utils import ClusterServices

LOGGER = logging.getLogger(ROOT)


def check_cluster_health():
    """K8s based health check."""
    nodes = CLUSTER_CFG["nodes"]
    host, user, password = None, None, None
    for node in nodes:
        if node["node_type"] == "master":
            host = node["hostname"]
            user = node["username"]
            password = node["password"]
            break
    if not host:
        raise HealthCheckError(f"Incorrect master details: {CLUSTER_CFG['nodes'][0]} ")
    cluster_obj = ClusterServices(host, user, password)
    status, response = cluster_obj.check_cluster_health()
    if not status:
        raise HealthCheckError(f"Cluster is not healthy. response: {response}")
    status, response = cluster_obj.check_cluster_storage()
    if not status:
        raise HealthCheckError(f"Failed to get cluster storage. response: {response}")
    LOGGER.info(response)
    LOGGER.info("Cluster is healthy, all services are up and running.")


def health_check_process(interval):
    """Health check wrapper.

    :param interval: Interval in Seconds.
    """
    while True:
        time.sleep(interval)
        check_cluster_health()
