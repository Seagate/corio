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

"""library having s3 related operations."""

import logging
import random
import string

from src.commons import constants as const

LOGGER = logging.getLogger(const.ROOT)


def get_bucket_name():
    """
    generate bucket name string
    first letter should be a number or lowercase letter
    rest letters can include number, lowercase, hyphens and dots.
    bucket length can vary from 3 to 63
    """
    return (''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits) +
            ''.join(random.SystemRandom().choice(string.ascii_lowercase + string.digits + "." + '-')
                    for _ in range(random.randint(2, 63)))))
