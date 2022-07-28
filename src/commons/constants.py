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

"""All common constants and params for corio."""

import os
import socket
from datetime import datetime


CORIO_ROOT = os.getcwd()  # Fetches you CWD of the runner.
CONFIG_DIR = 'config'
DATA_DIR = 'TestData'

CLUSTER_CFG = os.path.join(CONFIG_DIR, 'cluster_config.yaml')
S3_CONFIG = os.path.join(CONFIG_DIR, 's3', 's3_config.yaml')
CORIO_CFG_PATH = os.path.join(CONFIG_DIR, "corio_config.yaml")
S3_TOOL_PATH = os.path.join(CONFIG_DIR, 's3', "s3_tools.yaml")
MOUNT_DIR = os.path.join("/mnt", "nfs_share")
DATA_DIR_PATH = os.path.join(CORIO_ROOT, DATA_DIR)
LOG_DIR = os.path.join(CORIO_ROOT, "log")
REPORTS_DIR = os.path.join(CORIO_ROOT, "reports")
CMN_LOG_DIR = os.path.join(MOUNT_DIR, "CorIO-Execution", socket.gethostname())
LATEST_LOG_PATH = os.path.join(LOG_DIR, "latest")

# k8s constant for cortx.
HAX_CONTAINER_NAME = "cortx-hax"
NAMESPACE = "cortx"
DATA_POD_NAME_PREFIX = "cortx-data"
SERVER_POD_NAME_PREFIX = "cortx-server"
HA_POD_NAME_PREFIX = "cortx-ha"
CONTROL_POD_NAME_PREFIX = "cortx-control"
CLIENT_POD_NAME_PREFIX = "cortx-client"

K8S_SB_SCRIPT = "logs-cortx-cloud.sh"
K8S_CFT_SCRIPTS_PATH = "/root/deploy-scripts/k8_cortx_cloud"
K8S_RE_SCRIPTS_PATH = "/root/cortx-k8s/k8_cortx_cloud"
FORMATTER = '[%(asctime)s] [%(process)d] [%(threadName)-6s] [%(name)s] [%(levelname)-6s] ' \
            '[%(filename)s: %(lineno)d]: %(message)s'
ROOT = "corio"  # root logger name.
MIN_DURATION = 10  # Minimum execution duration in seconds.
DT_STRING = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")

# Supported type of object size.
KB = 1000
KIB = 1024

# resource utilization package.
NMON = "nmon"

# SB extensions
EXTS = [".tbz", ".tgz", ".txz", ".tar", ".gz", ".zip"]
