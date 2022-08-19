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
"""Logger for CorIO tool."""

import datetime
import gzip
import logging
import os
import shutil
from logging import handlers
from os import path

from config import CORIO_CFG
from src.commons import constants as const


class StreamToLogger:
    """logger class for corio."""

    def __init__(self, file_path, logger, **kwargs):
        """
        Initialize root logger.

        :param file_path: File path of the logger
        :param logger: logger object from logging.getLogger(__name__)
        :keyword stream: To enable/disable stream handler/logging
        :keyword max_byte: Rollover occurs whenever the current logfile is nearly maxBytes in length
        :keyword backup_count: count of the max rotation/rollover of logs
        :keyword log_rotate: Rotate log once reached the max_bytes
        """
        self.log_rotate = kwargs.get("log_rotate", True)
        self.max_byte = kwargs.get("max_byte", CORIO_CFG["log_size"])
        self.backup_count = kwargs.get("backup_count", CORIO_CFG["log_backup_count"])
        self.file_path = file_path
        self.logger = logger
        self.formatter = const.FORMATTER
        self.make_logdir()
        if kwargs.get("stream", False):
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

    def set_filehandler_logger(self):
        """Add a file handler for the logging module. this logs all messages to ``file_name``."""
        if self.log_rotate:
            handler = CorIORotatingFileHandler(
                self.file_path, maxbyte=self.max_byte, backupcount=self.backup_count)
        else:
            handler = logging.FileHandler(filename=self.file_path)
        formatter = logging.Formatter(self.formatter)
        handler.setLevel(logging.getLevelName(self.logger.level))
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)


class CorIORotatingFileHandler(handlers.RotatingFileHandler):
    """Handler overriding the existing RotatingFileHandler for switching corio log files."""

    def __init__(self, filename, maxbyte, backupcount):
        """
        Initialize for cortx rotating file handler.

        :param filename: Filename of the log.
        :param maxbyte: Rollover occurs whenever the current log file is nearly maxBytes in
        length.
        :param backupcount: count of the max rotation/rollover of logs.
        """
        super().__init__(filename=filename, maxBytes=maxbyte, backupCount=backupcount)

    def rotation_filename(self, default_name):
        """
        Form log file name for rotation internally called by rotation_filename method.

        :param default_name: name of the base file
        :return: rotated log file name e.g., io_driver-YYYY-MM-DD-1.gz
        """
        return f"{default_name}-{str(datetime.date.today())}.gz"

    def rotate(self, source, dest):
        """
        Compress and rotate the current log when size limit is reached.

        :param source: current log file path.
        :param dest: destination path for rotated file.
        """
        with open(source, "rb") as sf_obj:
            with gzip.open(dest, "wb", 9) as df_obj:
                shutil.copyfileobj(sf_obj, df_obj)
        os.remove(source)

    @staticmethod
    def get_logger(level, name, **kwargs) -> object:
        """
        Initialize and get the logger object.

        :param level: Set logging level, which is used across execution.
        :param name: Name of the logger.
        :returns: logger object.
        """
        logger = logging.Logger.manager.loggerDict.get(name)
        if logger:
            return logger
        dir_path = os.path.join(os.getcwd(), "log", "latest")
        if not os.path.exists(dir_path):
            os.makedirs(dir_path, exist_ok=True)
        level = logging.getLevelName(level)
        if level == logging.DEBUG:
            logger = logging.getLogger()
            for pkg in ['boto', 'boto3', 'botocore', 's3transfer', name]:
                logging.getLogger(pkg).setLevel(logging.DEBUG)
            fpath = os.path.join(dir_path, f"{name}_console.DEBUG")
        else:
            logger = logging.getLogger(name)
            fpath = os.path.join(dir_path, f"{name}_console.INFO")
        logger.setLevel(level)
        StreamToLogger(fpath, logger, **kwargs)
        return logger


def initialize_loghandler(logger, verbose=False):
    """Initialize io driver runner logging with stream and file handlers."""
    # If log level provided then it will use DEBUG else will use default INFO.
    dir_path = os.path.join(os.path.join(const.LOG_DIR, "latest"))
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    name = os.path.splitext(os.path.basename(__file__))[0]
    if verbose:
        level = logging.getLevelName(logging.DEBUG)
        log_path = os.path.join(dir_path, f"{name}_console_{const.DT_STRING}.DEBUG")
    else:
        level = logging.getLevelName(logging.INFO)
        log_path = os.path.join(dir_path, f"{name}_console_{const.DT_STRING}.INFO")
    os.environ["log_level"] = level
    logger.setLevel(level)
    StreamToLogger(log_path, logger, stream=True)
    os.environ["log_path"] = log_path
