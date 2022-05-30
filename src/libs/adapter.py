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

"""Python Library to implement adapter pattern.Adapter Pattern:
It adapts an interface to another, in order to match the client’s expectations."""

import abc


# Target
class Target(metaclass=abc.ABCMeta):
    """
    Define the domain-specific interface that Client uses.
    """

    def __init__(self, adaptee):
        self._adaptee = adaptee

    @abc.abstractmethod
    def size(self):
        """Object Size
        """
        pass

    @abc.abstractmethod
    def time(self):
        """Execution Time"""
        pass


# Adapter the interface of Adaptee to the Target(i.e. client) interface.

class Adapter:
    """
    Adapts an object by replacing methods.
    Usage:
    motorCycle = MotorCycle()
    motorCycle = Adapter(motorCycle, wheels = motorCycle.TwoWheeler)
    """

    def __init__(self, obj, **adapted_methods):
        """We set the adapted methods in the object's dict"""
        self.obj = obj
        self.__dict__.update(adapted_methods)

    def __str__(self):
        """Adapter Object Representation"""
        return f'obj name is {self.obj}'

    def __getattr__(self, attr):
        """All non-adapted calls are passed to the object"""
        return getattr(self.obj, attr)

    def get_objects(self):
        """Print original object dict"""
        return self.obj.__dict__

    def execute(self):
        """execute adapted object
        """
        self.obj.run()
