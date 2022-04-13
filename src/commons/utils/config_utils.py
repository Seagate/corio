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

"""library having config related operations."""

import logging
import os
from configparser import ConfigParser
from configparser import MissingSectionHeaderError
from configparser import NoSectionError
import yaml

from src.commons.constants import ROOT

LOGGER = logging.getLogger(ROOT)


def get_config_yaml(fpath: str) -> dict:
    """
    Read the config and decrypts the passwords.

    :param fpath: configuration file path.
    :return [type]: dictionary containing config data.
    """
    with open(fpath, encoding="utf-8") as fin:
        LOGGER.debug("Reading details from file : %s", fpath)
        data = yaml.safe_load(fin)

    return data


def get_config_section_key(path: str, section: str = None, key: str = None) -> list or str:
    """
    Get config file value as per the section and key.

    :param path: File path.
    :param section: Section name.
    :param key: Section key name.
    :return: key value else all items else None.
    """
    try:
        config = ConfigParser()
        config.read(path)
        if section and key:
            return config.get(section, key)

        return config.items(section)
    except (MissingSectionHeaderError, NoSectionError) as error:
        LOGGER.error(error)
        return None


def get_local_aws_keys(fpath, section="default"):
    """
    Fetch local aws access secret keys.

    :param fpath: aws config file path.
    :param section: Section as per aws config.
    """
    if os.path.exists(fpath):
        try:
            aws_access_key = get_config_section_key(fpath, section, "aws_access_key_id")
            aws_secret_key = get_config_section_key(fpath, section, "aws_secret_access_key")
            return aws_access_key, aws_secret_key
        except KeyError:
            LOGGER.error(KeyError)

    return None, None
