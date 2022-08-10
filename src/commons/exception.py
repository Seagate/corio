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

"""Exception module for corio tool."""


class CheckError(Exception):
    """Class for health check error."""

    def __init__(self, message=''):
        """Initialize error."""
        super().__init__(message)
        self.message = message

    def __str__(self):
        """Print error message."""
        return self.message


class HealthCheckError(CheckError):
    """Exception class for health check error."""


class PodReplicaError(CheckError):
    """Exception class for PodReplica Error"""


class DeployReplicasetError(CheckError):
    """Exception class for Replicaset Error"""


class NumReplicaError(CheckError):
    """Exception class for NumReplica Error"""


class K8sDeploymentRecoverError(CheckError):
    """Exception class for K8sDeployment Recover Error"""


class DeploymentBackupException(CheckError):
    """Exception class for Deployment Backup"""


class NoBucketExistsException(CheckError):
    """Exception class for if buckets created/exists"""


class CorIOException(Exception):
    """General exception class for corio tool."""
