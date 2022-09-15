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
"""Common operations/methods for remote host."""

import logging
import os

import paramiko

from src.commons import commands as cmd
from src.commons.constants import ROOT
from src.commons.utils.utility import decode_bytes_to_string

LOGGER = logging.getLogger(ROOT)


class RemoteHost:
    """Class for execution of commands on remote machine."""

    def __init__(self, host: str, user: str, password: str, timeout: int = 20 * 60) -> None:
        """Initialize parameters."""
        self.host = host
        self.user = user
        self.password = password
        self.timeout = timeout
        self.host_obj = paramiko.SSHClient()
        self.host_obj.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.sftp_obj = None

    def connect(self) -> None:
        """Connect to remote machine."""
        self.host_obj.connect(
            hostname=self.host,
            username=self.user,
            password=self.password,
            timeout=self.timeout,
        )
        self.sftp_obj = self.host_obj.open_sftp()
        LOGGER.debug("connected to %s", self.host)

    def disconnect(self) -> None:
        """Close remote machine connection."""
        self.sftp_obj.close()
        self.host_obj.close()
        LOGGER.debug("disconnected %s", self.host)

    def __del__(self):
        """Delete the connection object."""
        del self.sftp_obj
        del self.host_obj

    def execute_command(self, command: str, read_lines: bool = False) -> tuple:
        """
        Execute command on remote machine and return output/error response.

        :param command: command to be executed on remote machine.
        :param read_lines: read lines if set to True else read as a single string.
        """
        self.connect()
        LOGGER.info("Executing command: %s", command)
        # nosec
        _, stdout, stderr = self.host_obj.exec_command(command=command, timeout=self.timeout)
        exit_status = stdout.channel.recv_exit_status() == 0
        error = decode_bytes_to_string(stderr.readlines() if read_lines else stderr.read())
        output = decode_bytes_to_string(stdout.readlines() if read_lines else stdout.read())
        LOGGER.debug("Execution status %s", exit_status)
        LOGGER.debug(output)
        LOGGER.debug(error)
        response = output if exit_status else error
        self.disconnect()
        return exit_status, response

    def download_file(self, local_path: str, remote_path: str) -> None:
        """
        Download remote file to local path.

        :param local_path: Local file path.
        :param remote_path: remote file path.
        """
        self.connect()
        self.sftp_obj.get(remote_path, local_path)
        if not os.path.exists(local_path):
            raise IOError(f"Failed to download '{remote_path}' file")
        LOGGER.info("Remote file %s downloaded to %s successfully.", remote_path, local_path)
        self.disconnect()

    def delete_file(self, remote_path: str) -> None:
        """
        Delete remote file.

        :param remote_path: Remote file path.
        """
        self.connect()
        self.sftp_obj.remove(remote_path)
        LOGGER.info("Removed file %s", remote_path)
        self.disconnect()

    def path_exists(self, remote_path: str) -> bool:
        """
        Check remote file/directory path exists.

        :param remote_path: Remote file/directory path.
        """
        self.connect()
        try:
            self.sftp_obj.stat(remote_path)
            return True
        except IOError:
            return False
        finally:
            self.disconnect()

    def list_dirs(self, remote_path: str) -> list:
        """List all files and directories from remote path."""
        self.connect()
        try:
            return self.sftp_obj.listdir(remote_path)
        except IOError as err:
            LOGGER.error(err)
            return []
        finally:
            self.disconnect()

    def install_package(self, package_name: str) -> tuple:
        """Install package on remote machine."""
        self.connect()
        resp = (False, None)
        try:
            resp = self.execute_command(cmd.CMD_CHK_PKG_INSTALLED.format(package_name))
            if package_name not in resp[1]:
                resp = self.execute_command(cmd.CMD_INSTALL_PKG.format(package_name))
        except IOError as err:
            LOGGER.error(err)
        finally:
            self.disconnect()
        return resp

    def remove_package(self, package_name: str) -> tuple:
        """Remove package from remote machine."""
        self.connect()
        resp = (False, None)
        try:
            resp = self.execute_command(cmd.CMD_CHK_PKG_INSTALLED.format(package_name))
            if package_name in resp[1]:
                resp = self.execute_command(cmd.CMD_REMOVE_PKG.format(package_name))
        except IOError as err:
            LOGGER.error(err)
        finally:
            self.disconnect()
        return resp
