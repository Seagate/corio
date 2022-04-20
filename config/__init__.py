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

from src.commons.constants import CLUSTER_CFG
from src.commons.constants import CORIO_CFG_PATH
from src.commons.constants import S3_CONFIG, S3_TOOL_PATH
from src.commons.utils import config_utils


def split_args(sys_cmd: List):
    """Split args and make it compliant."""
    _args = []
    for item in sys_cmd:
        if item.find('=') != -1:
            _args.extend(item.split('='))
        else:
            _args.extend([item])
    return _args


CORIO_CFG = config_utils.get_config_yaml(CORIO_CFG_PATH)
S3_CFG = config_utils.get_config_yaml(fpath=S3_CONFIG)
CLUSTER_CFG = config_utils.get_config_yaml(fpath=CLUSTER_CFG)
S3_TOOLS_CFG = config_utils.get_config_yaml(fpath=S3_TOOL_PATH)


IO_DRIVER_ARGS = split_args(sys.argv)
_USE_SSL = '-us' if '-us' in IO_DRIVER_ARGS else '--use_ssl' if '--use_ssl' in IO_DRIVER_ARGS\
    else None
SSL_FLG = IO_DRIVER_ARGS[IO_DRIVER_ARGS.index(_USE_SSL) + 1] if _USE_SSL else True
_ENDPOINT = '-ep' if '-ep' in IO_DRIVER_ARGS else '--endpoint' if '--endpoint' in IO_DRIVER_ARGS\
    else None
S3_URL = IO_DRIVER_ARGS[IO_DRIVER_ARGS.index(_ENDPOINT) + 1] if _ENDPOINT else "s3.seagate.com"
_ACCESS_KEY = "-ak" if '-ak' in IO_DRIVER_ARGS else '--access_key' if '--access_key' in\
                                                                      IO_DRIVER_ARGS else None
ACCESS_KEY = IO_DRIVER_ARGS[IO_DRIVER_ARGS.index(_ACCESS_KEY) + 1] if _ACCESS_KEY else None
_SECRT_KEY = "-sk" if '-sk' in IO_DRIVER_ARGS else '--secret_key' if '--secret_key' in\
                                                                      IO_DRIVER_ARGS else None
SECRT_KEY = IO_DRIVER_ARGS[IO_DRIVER_ARGS.index(_SECRT_KEY) + 1] if _SECRT_KEY else None
USE_SSL = ast.literal_eval(str(SSL_FLG).title())
S3_ENDPOINT = f"{'https' if USE_SSL else 'http'}://{S3_URL}"

LOCAL_ACC_KEY, LOCAL_SEC_KEY = config_utils.get_local_aws_keys(S3_CFG["aws_path"],
                                                               S3_CFG["aws_cred_section"])

S3_CFG["access_key"] = ACCESS_KEY if ACCESS_KEY else LOCAL_ACC_KEY
S3_CFG["secret_key"] = SECRT_KEY if SECRT_KEY else LOCAL_SEC_KEY
S3_CFG["use_ssl"] = USE_SSL
S3_CFG["endpoint"] = S3_ENDPOINT
S3_CFG["s3api_retry"] = 5

# Munched configs. These can be used by dot "." operator.
S3_CFG = munch.munchify(S3_CFG)
CORIO_CFG = munch.munchify(CORIO_CFG)
CLUSTER_CFG = munch.munchify(CLUSTER_CFG)
S3_TOOLS_CFG = munch.munchify(S3_TOOLS_CFG)
