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
import os
from datetime import datetime
from subprocess import Popen, PIPE, CalledProcessError

import psutil as ps

LOGGER = logging.getLogger(__name__)


def log_cleanup():
    """
    Create backup of log/latest & reports.

    Renames the latest folder to a name with current timestamp and creates a folder named latest.
    Create directory inside reports and copy all old report's in it.
    """
    LOGGER.info("Backup all old execution logs into current timestamp directory.")
    log_dir = os.path.join(os.getcwd(), 'log')
    now = str(datetime.now()).replace(' ', '-').replace(":", "_").replace(".", "_")
    if os.path.isdir(log_dir):
        latest = os.path.join(log_dir, 'latest')
        if os.path.isdir(latest):
            log_list = glob.glob(latest + "/*")
            if log_list:
                os.rename(latest, os.path.join(log_dir, now))
                LOGGER.info("Backup directory: %s", os.path.join(log_dir, now))
            if not os.path.isdir(latest):
                os.makedirs(latest)
        else:
            os.makedirs(latest)
    else:
        os.makedirs(os.path.join(log_dir, 'latest'))
    LOGGER.info("Backup all old report into current timestamp directory.")
    reports_dir = os.path.join(os.getcwd(), "reports")
    if os.path.isdir(reports_dir):
        report_list = glob.glob(reports_dir + "/*")
        if report_list:
            now_dir = os.path.join(reports_dir, now)
            if not os.path.isdir(now_dir):
                os.makedirs(now_dir)
            for file in report_list:
                fpath = os.path.abspath(file)
                if os.path.isfile(fpath):
                    os.rename(file, os.path.join(now_dir, os.path.basename(fpath)))
            LOGGER.info("Backup directory: %s", now_dir)
    else:
        os.makedirs(reports_dir)


def cpu_memory_details():
    """Cpu and memory usage."""
    cpu_usages = ps.cpu_percent()
    LOGGER.debug("Real Time CPU usage: %s", cpu_usages)
    if cpu_usages > 80.0:
        LOGGER.info("CPU Usages are: %s", cpu_usages)
        if cpu_usages > 95.0:
            LOGGER.info("usages greater then 95 percent hence tool may stop execution")
    memory_usages = ps.virtual_memory().percent
    LOGGER.debug("Real Time memory usages are: %s", memory_usages)
    if memory_usages > 80.0:
        LOGGER.info("Memory Usages are: %s", memory_usages)
        available_memory = (ps.virtual_memory().available * 100) / ps.virtual_memory().total
        LOGGER.info("Available Memory is: %s", available_memory)
        if memory_usages > 95.0:
            LOGGER.warning("memory usages greater then 95 percent hence tool may stop execution")
            raise MemoryError(memory_usages)
        top_processes = run_local_cmd("top -b -o +%MEM | head -n 22 > corio/reports/topreport.txt")
        LOGGER.info(top_processes)


def run_local_cmd(cmd: str) -> tuple:
    """
    Execute any given command on local machine(Windows, Linux).
    :param cmd: command to be executed.
    :return: bool, response.
    """
    if not cmd:
        raise ValueError(f"Missing required parameter: {cmd}")
    LOGGER.debug("Command: %s", cmd)
    proc = None
    try:
        proc = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
        output, error = proc.communicate()
        LOGGER.debug("output = %s", str(output))
        LOGGER.debug("error = %s", str(error))
        if proc.returncode != 0:
            return False, str(error)
        return True, str(output)
    except (CalledProcessError, OSError) as error:
        LOGGER.error(error)
        return False, error
    finally:
        if proc:
            proc.terminate()


def create_file(path: str, size: int):
    """
    Create file with random data

    :param size: Size in bytes
    :param path: File name with path
    """
    base = 1024 * 1024
    while size > base:
        with open(path, 'ab+') as f_out:
            f_out.write(os.urandom(base))
        size -= base
    with open(path, 'ab+') as f_out:
        f_out.write(os.urandom(size))
