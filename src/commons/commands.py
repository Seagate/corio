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

"""All commands for corio."""

# K8s commands for cortx.
CMD_K8S_WORKERS_NAME = "kubectl get nodes -l node-role.kubernetes.io/worker=worker|awk '{print $1}'"
# Parameters: pod_name, container_name, command.
CMD_K8S_CLUSTER_HEALTH = "kubectl exec -it {} -c {} -- {}"
CMD_K8S_PODS_NAME = "kubectl get pods -o=custom-columns=NAME:.metadata.name"
CMD_HCTL_STATUS = "hctl status --json"
CLSTR_LOGS_CMD = "cd {}; sh logs-cortx-cloud.sh"
CMD_GENERATE_CLSTR_LOGS = "cd {}; sh logs-cortx-cloud.sh"
