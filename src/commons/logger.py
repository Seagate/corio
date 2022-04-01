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
""" Logger for CorIO tool."""

import os
import gzip
import shutil
import datetime
import logging
from os import path
from logging import handlers
from config import CORIO_CFG
from src.commons.constants import FORMATTER


class StreamToLogger:
    """logger class for corio."""

    def __init__(self, file_path, logger, stream=False):
        """"
        Initialize root logger.

        :param file_path: File path of the logger.
        :param logger: logger object from logging.getLogger(__name__).
        :param stream: To enable/disable stream handler/logging.
        """
        self.file_path = file_path
        self.logger = logger
        self.formatter = FORMATTER
        self.make_logdir()
        if stream:
            self.set_stream_logger()
        self.set_filehandler_logger()

    def make_logdir(self) -> None:
        """Create log directory if not exists."""
        head, _ = path.split(self.file_path)
        if not os.path.exists(head):
            os.makedirs(head, exist_ok=True)

    def set_stream_logger(self):
        """Add a stream handler for the logging module. This logs all messages to ``stdout``."""
        handler = logging.StreamHandler()
        formatter = logging.Formatter(self.formatter)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def set_filehandler_logger(self, max_byte=0, backup_count=5):
        """
        Add a file handler for the logging module. this logs all messages to ``file_name``.

        :param max_byte: Rollover occurs whenever the current log file is nearly maxBytes in length.
        :param backup_count: count of the max rotation/rollover of logs.
        """
        maxbyte = max_byte if max_byte else CORIO_CFG["log_size"]
        handler = CorIORotatingFileHandler(
            self.file_path, maxbyte=maxbyte, backupcount=backup_count)
        formatter = logging.Formatter(self.formatter)
        handler.setLevel(logging.getLevelName(self.logger.level))
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)


class CorIORotatingFileHandler(handlers.RotatingFileHandler):
    """Handler overriding the existing RotatingFileHandler for switching corio log files."""

    def __init__(self, filename, maxbyte, backupcount):
        """
        Initialization for cortx rotating file handler.

        :param filename: Filename of the log.
        :param maxbyte: Rollover occurs whenever the current log file is nearly maxBytes in
        length.
        :param backupcount: count of the max rotation/rollover of logs.
        """
        super().__init__(filename=filename, maxBytes=maxbyte, backupCount=backupcount)

    def rotation_filename(self, default_name):
        """
        Method to form log file name for rotation internally called by rotation_filename method.

        :param default_name: name of the base file
        :return: rotated log file name e.g., io_driver-YYYY-MM-DD-1.gz
        """
        return f"{default_name}-{str(datetime.date.today())}.gz"

    def rotate(self, source, dest):
        """
        Method to compress and rotate the current log when size limit is reached.

        :param source: current log file path.
        :param dest: destination path for rotated file.
        """
        with open(source, "rb") as sf_obj:
            with gzip.open(dest, "wb", 9) as df_obj:
                shutil.copyfileobj(sf_obj, df_obj)
        os.remove(source)


def get_logger(level, name) -> object:
    """
    Initialize and get the logger object.

    :param level: Set logging level, which is used across execution.
    :param name: Name of the logger.
    :returns: logger object.
    """
    logger = logging.Logger.manager.loggerDict.get(name)
    if logger:
        return logger
    level = logging.getLevelName(level)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    dir_path = os.path.join(os.getcwd(), "log", "latest")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    fpath = os.path.join(dir_path, f"{name}_console.log")
    StreamToLogger(fpath, logger)

    return logger
