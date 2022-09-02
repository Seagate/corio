#!/usr/bin/python
# -*- coding: utf-8 -*-
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
# For any questions about this software or licensing,
# please email opensource@seagate.com or cortx-questions@seagate.com.
#

"""Configs are initialized here."""

import ast
import sys
from typing import List

import munch

from arguments import opts
from src.commons import constants as const
from src.commons import yaml_parser


def split_args(sys_cmd: List):
    """Split args and make it compliant."""
    _args = []
    for item in sys_cmd:
        if item.find("=") != -1:
            _args.extend(item.split("="))
        else:
            _args.extend([item])
    return _args


CORIO_CFG = yaml_parser.read_yaml(fpath=const.CORIO_CFG_PATH)
S3_CFG = yaml_parser.read_yaml(fpath=const.S3_CONFIG)
CLUSTER_CFG = yaml_parser.read_yaml(fpath=const.CLUSTER_CFG)
S3_TOOLS_CFG = yaml_parser.read_yaml(fpath=const.S3_TOOL_PATH)
MASTER_CFG = yaml_parser.read_yaml(fpath=const.CORIO_MASTER_CONFIG)


IO_DRIVER_ARGS = split_args(sys.argv)
_USE_SSL = (
    "-us"
    if "-us" in IO_DRIVER_ARGS
    else "--use_ssl"
    if "--use_ssl" in IO_DRIVER_ARGS
    else None
)
SSL_FLG = IO_DRIVER_ARGS[IO_DRIVER_ARGS.index(_USE_SSL) + 1] if _USE_SSL else True
_ENDPOINT = (
    "-ep"
    if "-ep" in IO_DRIVER_ARGS
    else "--endpoint"
    if "--endpoint" in IO_DRIVER_ARGS
    else None
)
S3_URL = (
    IO_DRIVER_ARGS[IO_DRIVER_ARGS.index(_ENDPOINT) + 1]
    if _ENDPOINT
    else "s3.seagate.com"
)
_S3MAX_RETRY = (
    "-mr"
    if "-mr" in IO_DRIVER_ARGS
    else "--s3max_retry"
    if "--s3max_retry" in IO_DRIVER_ARGS
    else None
)
S3MAX_RETRY = (
    IO_DRIVER_ARGS[IO_DRIVER_ARGS.index(_S3MAX_RETRY) + 1] if _S3MAX_RETRY else 1
)
USE_SSL = ast.literal_eval(str(SSL_FLG).title())
S3_ENDPOINT = f"{'https' if USE_SSL else 'http'}://{S3_URL}"

S3_CFG["access_key"] = opts.access_key
S3_CFG["secret_key"] = opts.secret_key
S3_CFG["use_ssl"] = USE_SSL
S3_CFG["endpoint"] = S3_ENDPOINT
S3_CFG["s3max_retry"] = int(S3MAX_RETRY)

# Munched configs. These can be used by dot "." operator.
S3_CFG = munch.munchify(S3_CFG)
CORIO_CFG = munch.munchify(CORIO_CFG)
CLUSTER_CFG = munch.munchify(CLUSTER_CFG)
S3_TOOLS_CFG = munch.munchify(S3_TOOLS_CFG)
