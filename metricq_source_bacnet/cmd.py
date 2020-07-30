# metricq-source-bacnet
# Copyright (C) 2019 ZIH, Technische Universitaet Dresden, Federal Republic of Germany
#
# All rights reserved.
#
# This file is part of metricq-source-bacnet.
#
# metricq-source-bacnet is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# metricq-source-bacnet is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with metricq-source-bacnet.  If not, see <http://www.gnu.org/licenses/>.
import logging

import click as click

import aiomonitor
import click_log
from metricq.logging import get_logger

from .source import BacnetSource

logger = get_logger()

click_log.basic_config(logger)
logger.setLevel("INFO")
logger.handlers[0].formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)-8s] [%(name)-20s] %(message)s"
)


@click.command()
@click.option("--server", default="amqp://localhost/")
@click.option("--token", default="source-bacnet")
@click.option("--monitor/--no-monitor", default=False)
@click.option("--log-to-journal/--no-log-to-journal", default=False)
@click.option("--disk-cache-filename", default="metricq-source-bacnet-disk-cache.json")
@click_log.simple_verbosity_option(logger)
def source_cmd(server, token, monitor, log_to_journal, disk_cache_filename):
    if log_to_journal:
        try:
            from systemd import journal

            logger.handlers[0] = journal.JournaldLogHandler()
        except ImportError:
            logger.error("Can't enable journal logger, systemd package not found!")

    src = BacnetSource(
        token=token, management_url=server, disk_cache_filename=disk_cache_filename
    )
    if monitor:
        with aiomonitor.start_monitor(src.event_loop, locals={"source": src}):
            src.run(cancel_on_exception=True)
    else:
        src.run(cancel_on_exception=True)
