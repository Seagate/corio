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
from datetime import datetime


CORIO_ROOT = os.getcwd()  # Fetches you CWD of the runner.
CONFIG_DIR = 'config'
DATA_DIR = 'TestData'

CLUSTER_CFG = os.path.join(CONFIG_DIR, 'cluster_config.yaml')
S3_CONFIG = os.path.join(CONFIG_DIR, 's3', 's3_config.yaml')
CORIO_CFG_PATH = os.path.join(CONFIG_DIR, "corio_config.yaml")
S3_TOOL_PATH = os.path.join(CONFIG_DIR, 's3', "s3_tools.yaml")
MOUNT_DIR = os.path.join("/root", "nfs_share")
DATA_DIR_PATH = os.path.join(CORIO_ROOT, DATA_DIR)


# k8s constant for cortx.
POD_NAME_PREFIX = "cortx-server"
HAX_CONTAINER_NAME = "cortx-hax"
NAMESPACE = "default"

K8S_SCRIPTS_PATH = "/root/deploy-scripts/k8_cortx_cloud/"
FORMATTER = '[%(asctime)s] [%(process)d] [%(threadName)-6s] [%(name)s] [%(levelname)-6s] ' \
            '[%(filename)s: %(lineno)d]: %(message)s'
ROOT = "corio"  # root logger name.
MIN_DURATION = 20  # Minimum execution duration in seconds.
DT_STRING = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")

CMD_YUM_NMON = "yum install -y nmon"
K8S_WORKER_NODES = "kubectl get nodes -l node-role.kubernetes.io/worker=worker | awk '{print $1}'"
CMD_RUN_NMON = "nmon -f -s 60 -TU"
CMD_KILL_NMON = "kill -USR2 $(ps ax | grep nmon  | grep -v grep | awk '{print $1}')"
CMD_NMON_FILE = "find . -name '*.nmon'"
CMD_RM_NMON = "rm -f {}"
# Supported type of object size.
KB = 1000
KIB = 1024
# nimon installation
YUM_UNZIP = "yum install -y unzip"
CMD_WGET_NIMON = "wget http://sourceforge.net/projects/nmon/files/njmon_linux_binaries_v78.zip --no-check-certificate"
UNZIP_NIMON = "unzip njmon_linux_binaries_v78.zip"
CMD_CHMOD = "chmod 655 ninstall"
CMD_NINSTALL = "./ninstall"
CMD_RUN_NIMON = "nimon -s 30 -k -i ssc-vm-g4-rhev4-0125.colo.seagate.com -p 8086 -x njmon -W"
CMD_KILL_NIMON = "kill -USR2 $(ps ax | grep nimon  | grep -v grep | awk '{print $1}')"
