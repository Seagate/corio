# -*- coding: utf-8 -*-
# !/usr/bin/python
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

"""Yaml Parser for IO stability."""
import copy
import datetime
import logging

import yaml

from src.commons.constants import KB
from src.commons.constants import KIB
from src.commons.constants import ROOT

LOGGER = logging.getLogger(ROOT)


def apply_master_config(workload: dict, master_cfg: dict) -> dict:
    """
    Add missing parameters to tests based on operation and tool mentioned in the test
    :param workload: Parsed test config
    :param master_cfg: Parsed master config
    :return: dict: Final test dictionary to be scheduled.
    """
    for test, config in workload.items():
        # Check for tests with no parameters
        if not config:
            raise AssertionError(f"No parameter for a {test}.")
        existing_params = config.keys()
        LOGGER.debug("Existing params are %s", existing_params)
        # Check for minimum required parameters (TestID, Tool, Operation)
        required = master_cfg['common']
        if not existing_params or (set(required) - set(existing_params) != set()):
            raise AssertionError(f"Minimum required parameters are missing {required} for {test}")
        tool = config['tool']
        operation = config['operation']
        required_params = list(master_cfg[tool][operation].keys()) + required
        LOGGER.debug("Required params are %s", required_params)
        # Check for unknown parameters
        for param in existing_params:
            if param not in required_params:
                raise AssertionError(f"Wrong parameter {param} in {test} test.")
        to_be_added = required_params - existing_params
        # Add missing parameters from master config file
        for param in to_be_added:
            config[param] = copy.deepcopy(master_cfg[tool][operation][param])
        LOGGER.info("Added %s parameters to %s test", to_be_added, test)
    return workload


def yaml_parser(yaml_file) -> dict:
    """
    YAML file to python dictionary.

    :param yaml_file: yaml file to parse.
    :return python dict containing file contents.
    """
    LOGGER.debug("YAML file selected for parse: %s", yaml_file)
    yaml_dict = {}
    with open(yaml_file, "r", encoding="utf-8") as obj:
        data = yaml.safe_load(obj)
        yaml_dict.update(data)
    LOGGER.debug("YAML file data: %s", yaml_dict)
    return yaml_dict


def convert_to_bytes(size):
    """
    Convert any size to bytes.

    :param size: object size
    can be provided as byte(s), kb, kib, mb, mib, gb, gib, tb, tib
    :return equivalent bytes value for object size.
    """
    size = size.lower()
    size_bytes = 0
    if 'bytes' in size or 'byte' in size:
        size_bytes = int(size.split('byte')[0])
    if 'kb' in size:
        size_bytes = int(size.split('kb')[0]) * KB
    if 'kib' in size:
        size_bytes = int(size.split('kib')[0]) * KIB
    if 'mb' in size:
        size_bytes = int(size.split('mb')[0]) * KB * KB
    if 'mib' in size:
        size_bytes = int(size.split('mib')[0]) * KIB * KIB
    if 'gb' in size:
        size_bytes = int(size.split('gb')[0]) * KB * KB * KB
    if 'gib' in size:
        size_bytes = int(size.split('gib')[0]) * KIB * KIB * KIB
    if 'tb' in size:
        size_bytes = int(size.split('tb')[0]) * KB * KB * KB * KB
    if 'tib' in size:
        size_bytes = int(size.split('tib')[0]) * KIB * KIB * KIB * KIB
    LOGGER.debug(size_bytes)
    return size_bytes


def convert_to_time_delta(time):
    """
    Convert execution time in time delta format.

    :param time : accepts time in format 0d0h0m0s
    :return python timedelta object.
    """
    time = time.lower()
    days = hrs = mnt = sec = 00
    if 'd' in time:
        days = int(time.split('d')[0])
        time = time.split('d')[1]
    if 'h' in time:
        hrs = int(time.split('h')[0])
        time = time.split('h')[1]
    if 'm' in time:
        mnt = int(time.split('m')[0])
        time = time.split('m')[1]
    if 's' in time:
        sec = int(time.split('s')[0])
    datetime_obj = datetime.timedelta(days=days, hours=hrs, minutes=mnt, seconds=sec)
    return datetime_obj


def test_parser(yaml_file, number_of_nodes):
    """
    Parse a workload yaml file.

    :param yaml_file: accepts and parses a test YAML file.
    :param number_of_nodes: accepts number of nodes to calculate sessions (default=1).
    :return python dictionary containing file contents.
    """
    workload_data = yaml_parser(yaml_file)
    master_config = yaml_parser("workload/master_config.yaml")
    workload_data = apply_master_config(workload_data, master_config)
    delta_list = []
    for test, data in workload_data.items():
        # Check compulsory workload parameter 'Object size' from workload.
        if "object_size" not in data:
            raise AssertionError(
                f"Object size is compulsory, which is missing in workload {yaml_file}")
        if "total_samples" in data and isinstance(data["object_size"], dict):
            convert_object_size_to_bytes_samples(data)
            convert_min_runtime_to_time_delta(test, delta_list, data)
        else:
            convert_object_part_size_to_bytes(data)
            convert_range_read_to_bytes(data)
            convert_min_runtime_to_time_delta(test, delta_list, data)
        if 'sessions_per_node' in data.keys():
            data['sessions'] = data['sessions_per_node'] * number_of_nodes
    LOGGER.debug("test object %s: ", workload_data)
    return workload_data


def convert_min_runtime_to_time_delta(test, delta_list, data):
    """Convert min_runtime to time_delta."""
    if test.lower() == "test_1":
        data['start_time'] = datetime.timedelta(hours=00, minutes=00, seconds=00)
        delta_list.append(convert_to_time_delta(data['min_runtime']))
    else:
        data['start_time'] = delta_list.pop()
        delta_list.append(data['start_time'] + convert_to_time_delta(data['min_runtime']))
    data['min_runtime'] = convert_to_time_delta(data['min_runtime'])


def convert_object_part_size_to_bytes(data):
    """Convert object_size, part_size to bytes."""
    for size_type in ["object_size", "part_size", "total_storage_size"]:
        if size_type in data:
            if isinstance(data[size_type], dict):
                if "start" not in data[size_type] or "end" not in data[size_type]:
                    raise AssertionError(f"Range using start and end keys for '{data[size_type]}'"
                                         f"missing in workload '{data}'")
                data[size_type]["start"] = convert_to_bytes(data[size_type]["start"])
                data[size_type]["end"] = convert_to_bytes(data[size_type]["end"])
            elif isinstance(data[size_type], list):
                data[size_type] = [convert_to_bytes(item) for item in data[size_type]]
            else:
                data[size_type] = convert_to_bytes(data[size_type])


def convert_range_read_to_bytes(data):
    """Convert range_read to bytes."""
    if "range_read" in data:
        if isinstance(data["range_read"], dict):
            if "start" not in data["range_read"] or "end" not in data["range_read"]:
                raise AssertionError(
                    f"Please define range using start and end keys: {data['range_read']}")
            data["range_read"]["start"] = convert_to_bytes(data["range_read"]["start"])
            data["range_read"]["end"] = convert_to_bytes(data["range_read"]["end"])
        elif isinstance(data["range_read"], str):
            data["range_read"] = convert_to_bytes(data["range_read"])


def convert_object_size_to_bytes_samples(data):
    """Convert object_operations_type1 to bytes and distribution to samples.
    sample data:
    test_1:
      TEST_ID: TEST-40039
      object_size:
        0Kb: 2%
        1Kb: 24.79%
        10Kb: 18.84%
        100Kb: 17.87%
        1Mb: 18.2%
        10Mb: 16.7%
        100Mb: 1.56%
        1Gb: 0.03%
        2Gb: 0.01%
      total_samples: 10000
    """
    object_size, distribution = zip(*data['object_size'].items())
    object_size = convert_object_size_to_bytes(object_size)
    samples = data["total_samples"]
    distribution = list(distribution)
    distribution = convert_distribution_to_sample(distribution, samples)
    new_data = dict(zip(object_size, distribution))
    data['object_size'] = new_data


def convert_object_size_to_bytes(object_size: tuple) -> tuple:
    """Convert object size of object_operations_type1 to bytes."""
    return tuple(convert_to_bytes(item) for item in object_size)


def convert_distribution_to_sample(distribution: list, samples: int) -> tuple:
    """Convert object size distribution to samples."""
    for sample, _ in enumerate(distribution):
        distribution[sample] = int(round(((float(distribution[sample][:-1]) / 100.0) * samples), 2))
    return tuple(distribution)
