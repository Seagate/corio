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

"""Perform parallel S3 operations as per the given test input YAML using Asyncio."""

import os
import logging
import argparse
import random
from datetime import datetime
from distutils.util import strtobool
from src.commons.logger import StreamToLogger

LOGGER = logging.getLogger()


def initialize_loghandler(level=logging.INFO):
    """
    Initialize io driver runner logging with stream and file handlers.

    param level: logging level used in CorIO tool.
    """
    LOGGER.setLevel(level)
    dir_path = os.path.join(os.path.join(os.getcwd(), "log", "latest"))
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)
    name = os.path.splitext(os.path.basename(__file__))[0]
    dt_string = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
    name = os.path.join(dir_path, f"{name}_console_{dt_string}.log")
    StreamToLogger(name, LOGGER)


def parse_args():
    """Commandline arguments for CORIO Driver."""
    parser = argparse.ArgumentParser()
    parser.add_argument("-ti", "--test_input", type=str,
                        help="Directory path containing test data input yaml files or "
                             "input yaml file path.")
    parser.add_argument("-ll", "--logging-level", type=int, default=20,
                        help="log level value as defined below: " +
                             "CRITICAL=50 " +
                             "ERROR=40 " +
                             "WARNING=30 " +
                             "INFO=20 " +
                             "DEBUG=10"
                        )
    parser.add_argument("-us", "--use_ssl", type=lambda x: bool(strtobool(str(x))), default=True,
                        help="Use HTTPS/SSL connection for S3 endpoint.")
    parser.add_argument("-sd", "--seed", type=int, help="seed.",
                        default=random.SystemRandom().randint(1, 9999999))
    parser.add_argument("-sk", "--secret_key", type=str, help="s3 secret Key.")
    parser.add_argument("-ak", "--access_key", type=str, help="s3 access Key.")
    parser.add_argument("-ep", "--endpoint", type=str,
                        help="fqdn of s3 endpoint for io operations.", default="s3.seagate.com")
    parser.add_argument("-nn", "--number_of_nodes", type=int,
                        help="number of nodes in k8s system", default=1)
    return parser.parse_args()


if __name__ == '__main__':
    opts = parse_args()
    log_level = logging.getLevelName(opts.logging_level)
    initialize_loghandler(level=log_level)
    LOGGER.info("Arguments: %s", opts)
