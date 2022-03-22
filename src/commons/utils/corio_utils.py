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

import os
import logging
import glob
from datetime import datetime

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
