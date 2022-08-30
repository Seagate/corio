#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2020 Seagate Technology LLC and/or its Affiliates
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
"""AsyncIO utility."""

import asyncio


def run_event_loop_until_complete(logger, func, *args, **kwargs):
    """Run the event """
    new_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(new_loop)
    try:
        new_loop.run_until_complete(func(*args, **kwargs))
    except KeyboardInterrupt:
        logger.warning("Loop interrupted for %s", func.__name__)
    except Exception as err:
        logger.exception(err)
        raise err from Exception
    finally:
        if new_loop.is_running():
            new_loop.stop()
            logger.warning("Event loop stopped: %s", not new_loop.is_running())
        if not new_loop.is_closed():
            new_loop.close()
            logger.info("Event loop closed: %s", new_loop.is_closed())


async def schedule_tasks(logger, tasks):
    """Schedule tasks and wait unit complete or first exception."""
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    if pending:
        logger.critical("Terminating pending task: %s", pending)
        for task in pending:
            task.cancel()
    logger.info(done)
    for task in done:
        task.result()
