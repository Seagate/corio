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
# Parameters: pod_name, container_name, command.
KUBECTL_CMD = "kubectl {} {} -n {} {}"
KUBECTL_SET_CONTEXT = "kubectl config set-context --current --namespace={}"
CMD_K8S_WORKERS_NAME = "kubectl get nodes -l node-role.kubernetes.io/worker=worker|awk '{print $1}'"
CMD_POD_STATUS = "kubectl get pods"
KUBECTL_GET_POD_DETAILS = "kubectl get pods --show-labels | grep {}"
KUBECTL_GET_REPLICASET = "kubectl get rs | grep '{}'"
KUBECTL_GET_POD_CONTAINERS = "kubectl get pods {} -o jsonpath='{{.spec.containers[*].name}}'"
KUBECTL_GET_POD_IPS = 'kubectl get pods --no-headers -o ' \
                      'custom-columns=":metadata.name,:.status.podIP"'
KUBECTL_CREATE_REPLICA = "kubectl scale --replicas={} deployment/{}"
KUBECTL_DEPLOY_BACKUP = "kubectl get deployment {} -o yaml > {}"
KUBECTL_GET_POD_HOSTNAME = "kubectl exec -it {} -c cortx-hax -- hostname"
KUBECTL_RECOVER_DEPLOY = "kubectl create -f {}"
KUBECTL_GET_POD_NAMES = 'kubectl get pods --no-headers -o custom-columns=":metadata.name"'
CMD_K8S_CLUSTER_HEALTH = "kubectl exec -it {} -c {} -- {}"
CMD_K8S_PODS_NAME = "kubectl get pods -o=custom-columns=NAME:.metadata.name"
CMD_HCTL_STATUS = "hctl status --json"
CMD_GENERATE_CLSTR_LOGS = "cd {}; sh {}"
# System commands.
CMD_MOUNT = "mount -t nfs {} {}"
