# !/usr/bin/python
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
"""Module to parse commandline arguments for CORIO Driver."""

import random
from argparse import ArgumentParser, Action
from distutils.util import strtobool


def parse_args():
    """Commandline arguments for CORIO Driver."""
    parser = ArgumentParser()
    parser.add_argument(
        "-ti",
        "--test_input",
        type=str,
        required=True,
        help="Directory path containing test data input yaml files or "
        "input yaml file path.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="log level used verbose(debug), default is info.",
    )
    parser.add_argument(
        "-us",
        "--use_ssl",
        type=lambda x: bool(strtobool(str(x))),
        default=True,
        help="Use HTTPS/SSL connection for S3 endpoint.",
    )
    parser.add_argument(
        "-sd",
        "--seed",
        type=int,
        help="seed.",
        default=random.SystemRandom().randint(1, 9999999),
    )
    parser.add_argument(
        "-sk",
        "--secret_key",
        type=str,
        nargs="+",
        action=SplitArguments,
        required=True,
        help="One or more(space/comma separated) s3 secret Key or keys in order.",
    )
    parser.add_argument(
        "-ak",
        "--access_key",
        type=str,
        nargs="+",
        action=SplitArguments,
        required=True,
        help="One or more(space/comma separated) s3 access Key or keys in order.",
    )
    parser.add_argument(
        "-ep",
        "--endpoint",
        type=str,
        required=True,
        help="fqdn/ip:port of s3 endpoint for io operations without http/https."
        "protocol in endpoint is based on use_ssl flag.",
        default="s3.seagate.com",
    )
    parser.add_argument(
        "-nn",
        "--number_of_nodes",
        type=int,
        help="number of nodes in k8s system",
        default=1,
    )
    parser.add_argument(
        "-sb",
        "--support_bundle",
        type=lambda x: bool(strtobool(str(x))),
        default=False,
        help="Capture Support bundle.",
    )
    parser.add_argument(
        "-hc",
        "--health_check",
        type=lambda x: bool(strtobool(str(x))),
        default=False,
        help="Health Check.",
    )
    parser.add_argument(
        "-tp", "--test_plan", type=str, default=None, help="jira xray test plan id"
    )
    parser.add_argument(
        "-dm",
        "--degraded_mode",
        type=lambda x: bool(strtobool(str(x))),
        default=False,
        help="Degraded Mode, True/False",
    )
    parser.add_argument(
        "-mr",
        "--s3max_retry",
        type=int,
        default=0,
        help="Max number of retries in case of any type of failure.",
    )
    parser.add_argument(
        "-sr",
        "--sequential_run",
        action="store_true",
        help="Run test sequentially from workload.",
    )
    return parser.parse_args()


class SplitArguments(Action):
    """Split space, comma separated arguments and set it to list."""

    def __call__(self, parser, namespace, values, option_string=None) -> None:
        """strip comma from arguments."""
        setattr(
            namespace,
            self.dest,
            [
                value.strip(",")
                for value in (
                    values
                    if isinstance(values, list)
                    else (values.split(",") if ", " in values else values.split())
                    if isinstance(values, str)
                    else values
                )
            ],
        )

    def __repr__(self):
        """string representation."""
        return repr(self)


opts = parse_args()
