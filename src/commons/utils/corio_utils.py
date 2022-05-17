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

"""common operations/methods from corio tool."""

import glob
import logging
import math
import os
import shutil
from base64 import b64encode
from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError
from typing import Union

import paramiko
import psutil as ps

from config import CORIO_CFG
from src.commons.commands import CMD_MOUNT
from src.commons.constants import CMN_LOG_DIR, MOUNT_DIR
from src.commons.constants import DATA_DIR_PATH, LOG_DIR, REPORTS_DIR
from src.commons.constants import KB, KIB
from src.commons.constants import ROOT
from src.commons.commands import CHECK_RPM, CHECK_H

LOGGER = logging.getLogger(ROOT)


def log_cleanup() -> None:
    """
    Create backup of log/latest & reports.

    Renames the latest folder to a name with current timestamp and creates a folder named latest.
    Create directory inside reports and copy all old report's in it.
    """
    LOGGER.info("Backup all old execution logs into current timestamp directory.")
    now = str(datetime.now()).replace(' ', '-').replace(":", "_").replace(".", "_")
    if os.path.isdir(LOG_DIR):
        latest = os.path.join(LOG_DIR, 'latest')
        if os.path.isdir(latest):
            log_list = glob.glob(latest + "/*")
            if log_list:
                os.rename(latest, os.path.join(LOG_DIR, now))
                LOGGER.info("Backup directory: %s", os.path.join(LOG_DIR, now))
            if not os.path.isdir(latest):
                os.makedirs(latest)
        else:
            os.makedirs(latest)
    else:
        LOGGER.info("Created log directory '%s'", )
        os.makedirs(os.path.join(LOG_DIR, 'latest'))
    LOGGER.info("Backup all old report into current timestamp directory.")
    if os.path.isdir(REPORTS_DIR):
        report_list = glob.glob(REPORTS_DIR + "/*")
        if report_list:
            now_dir = os.path.join(REPORTS_DIR, now)
            if not os.path.isdir(now_dir):
                os.makedirs(now_dir)
            for file in report_list:
                fpath = os.path.abspath(file)
                if os.path.isfile(fpath):
                    os.rename(file, os.path.join(now_dir, os.path.basename(fpath)))
            LOGGER.info("Backup directory: %s", now_dir)
    else:
        os.makedirs(REPORTS_DIR)


def cpu_memory_details() -> None:
    """Cpu and memory usage."""
    cpu_usages = ps.cpu_percent()
    if cpu_usages > 85.0:
        LOGGER.warning("Client: CPU Usages are: %s", cpu_usages)
        if cpu_usages > 98.0:
            LOGGER.critical("Client: CPU usages are greater then %s, hence tool may stop execution",
                            cpu_usages)
    memory_usages = ps.virtual_memory().percent
    if memory_usages > 85.0:
        LOGGER.warning("Client: Memory usages are: %s", memory_usages)
        available_memory = (ps.virtual_memory().available * 100) / ps.virtual_memory().total
        LOGGER.warning("Available Memory is: %s", available_memory)
        if memory_usages > 98.0:
            LOGGER.critical("Client: Memory usages greater then %s, hence tool may stop execution",
                            memory_usages)
            raise MemoryError(memory_usages)
        run_local_cmd("top -b -o +%MEM | head -n 22 > reports/topreport.txt")


def run_local_cmd(cmd: str) -> tuple:
    """
    Execute any given command on local machine(Windows, Linux).

    :param cmd: command to be executed.
    :return: bool, response.
    """
    if not cmd:
        raise ValueError(f"Missing required parameter: {cmd}")
    LOGGER.debug("Command: %s", cmd)
    try:
        with Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE, encoding="utf-8") as proc:
            output, error = proc.communicate()
            LOGGER.debug("output = %s", str(output))
            LOGGER.debug("error = %s", str(error))
            if proc.returncode != 0:
                return False, error
        return True, output
    except (CalledProcessError, OSError) as error:
        LOGGER.error(error)
        return False, error


def create_file(file_name: str, size: int, data_type: Union[str, bytes] = bytes) -> str:
    """
    Create file with random data(string/bytes), default is bytes.

    :param size: Size in bytes.
    :param file_name: File name or file path.
    :param data_type: supported data type string(str)/byte(bytes) while create file.
    """
    base = KIB * KIB
    if os.path.isdir(os.path.split(file_name)[0]):
        file_path = file_name
    else:
        file_path = os.path.join(DATA_DIR_PATH, file_name)
    while size > base:
        if issubclass(data_type, bytes):
            with open(file_path, 'ab+') as bf_out:
                bf_out.write(os.urandom(base))
        else:
            with open(file_path, 'a+', encoding="utf-8") as sf_out:
                sf_out.write(b64encode(os.urandom(base)).decode("utf-8")[:base])
        size -= base
    if issubclass(data_type, bytes):
        with open(file_path, 'ab+') as bf_out:
            bf_out.write(os.urandom(size))
    else:
        with open(file_path, 'a+', encoding="utf-8") as sf_out:
            sf_out.write(b64encode(os.urandom(size)).decode("utf-8")[:size])
    return file_path


def convert_size(size_bytes) -> str:
    """
    Convert byte size to KiB, MiB, KB, MB etc.

    :param size_bytes: Size in bytes.
    """
    if size_bytes:
        size_name_1024 = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
        size_name_1000 = ("B", "KB", "MB", "GB", "TB", "PB")
        if (size_bytes % KB) == 0:
            check_pow = int(math.floor(math.log(size_bytes, KB)))
            power = math.pow(KB, check_pow)
            size = int(round(size_bytes / power, 2))
            part_size = f"{size}{size_name_1000[check_pow]}"
        elif (size_bytes % KIB) == 0:
            check_pow = int(math.floor(math.log(size_bytes, KIB)))
            power = math.pow(KIB, check_pow)
            size = int(round(size_bytes / power, 2))
            part_size = f"{size}{size_name_1024[check_pow]}"
        else:
            part_size = f"{size_bytes}B"
    else:
        part_size = f"{size_bytes}B"

    return part_size


def rotate_logs(dpath: str, max_count: int = 0):
    """
    Remove old logs based on creation time and keep as per max log count, default is 5.

    :param: dpath: Directory path of log files.
    :param: max_count: Maximum count of log files to keep.
    """
    max_count = max_count if max_count else CORIO_CFG.get("max_sb", 5)
    if not os.path.exists(dpath):
        raise IOError(f"Directory '{dpath}' path does not exists.")
    files = sorted(glob.glob(dpath + '/**'), key=os.path.getctime, reverse=True)
    LOGGER.debug(files)
    if len(files) > max_count:
        for fpath in files[max_count:]:
            if os.path.exists(fpath):
                if os.path.isfile(fpath):
                    os.remove(fpath)
                    LOGGER.debug("Removed: Old log file: %s", fpath)
                if os.path.isdir(fpath):
                    shutil.rmtree(fpath)
                    LOGGER.debug("Removed: Old log directory: %s", fpath)

    if len(os.listdir(dpath)) > max_count:
        raise IOError(f"Failed to rotate SB logs: {os.listdir(dpath)}")


def mount_nfs_server(host_dir: str, mnt_dir: str) -> bool:
    """
    Mount nfs server on mount directory.

    :param: host_dir: Link of NFS server with path.
    :param: mnt_dir: Path of directory to be mounted.
    """
    try:
        if not os.path.exists(mnt_dir):
            os.makedirs(mnt_dir)
            LOGGER.debug("Created directory: %s", mnt_dir)
        if host_dir:
            if not os.path.ismount(mnt_dir):
                resp = os.system(CMD_MOUNT.format(host_dir, mnt_dir))
                if resp:
                    raise IOError(f"Failed to mount server: {host_dir} on {mnt_dir}")
                LOGGER.debug("NFS Server: %s, mount on %s successfully.", host_dir, mnt_dir)
            else:
                LOGGER.debug("NFS Server already mounted.")
            return os.path.ismount(mnt_dir)
        LOGGER.debug("NFS Server not provided, Storing logs locally at %s", mnt_dir)
        return os.path.isdir(mnt_dir)
    except OSError as error:
        LOGGER.error(error)
        return False


def decode_bytes_to_string(text):
    """Convert byte to string."""
    if isinstance(text, bytes):
        text = text.decode("utf-8")
    else:
        if isinstance(text, list):
            text_list = []
            for byt in text:
                if isinstance(byt, bytes):
                    text_list.append(byt.decode("utf-8"))
                else:
                    text_list.append(byt)
            return text_list
    return text


def setup_environment():
    """Prepare client for workload execution with CORIO."""
    LOGGER.info("Setting up environment to start execution!!")
    ret = mount_nfs_server(CORIO_CFG["nfs_server"], MOUNT_DIR)
    assert ret, "Error while Mounting NFS directory"
    if os.path.exists(DATA_DIR_PATH):
        shutil.rmtree(DATA_DIR_PATH)
    os.makedirs(DATA_DIR_PATH, exist_ok=True)
    LOGGER.debug("Data directory path created: %s", DATA_DIR_PATH)
    LOGGER.info("environment setup completed.")


def store_logs_to_nfs_local_server():
    """Copy/Store workload, support bundle and client/server resource log to local/NFS server."""
    # Copy workload execution logs to nfs/local server.
    latest = os.path.join(LOG_DIR, 'latest')
    if os.path.exists(latest):
        shutil.copytree(latest, os.path.join(CMN_LOG_DIR, os.getenv("run_id"), "log", "latest"))
    # Copy reports to nfs/local server.
    reports = glob.glob(f"{REPORTS_DIR}/*.*")
    svr_report_dir = os.path.join(CMN_LOG_DIR, os.getenv("run_id"), "reports")
    if not os.path.exists(svr_report_dir):
        os .makedirs(svr_report_dir)
    for report in reports:
        shutil.copyfile(report, os.path.join(svr_report_dir, os.path.basename(report)))
    LOGGER.info("All logs copied to %s", os.path.join(CMN_LOG_DIR, os.getenv("run_id")))
    # Cleaning up TestData.
    if os.path.exists(DATA_DIR_PATH):
        shutil.rmtree(DATA_DIR_PATH)


def is_package_installed_local(package_name):
    """
    check package is installed or not.
    :param package_name: package name to check
    """
    resp = run_local_cmd(CHECK_RPM.format(package_name))
    LOGGER.info("resp: %s", str(resp))
    if not resp[0]:
        resp = run_local_cmd(CHECK_H.format(package_name))
        LOGGER.info("resp: %s", str(resp))
    return resp


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
        self.host_obj.connect(hostname=self.host, username=self.user, password=self.password,
                              timeout=self.timeout)
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

    def is_package_installed(self, package_name):
        """
        check package is installed or not.

        :param package_name: package name to check
        """
        self.connect()
        resp = self.execute_command(CHECK_RPM.format(package_name))
        LOGGER.info("resp: %s", str(resp))
        if not resp[0]:
            resp = self.execute_command(CHECK_H.format(package_name))
            LOGGER.info("resp: %s", str(resp))
        return resp
