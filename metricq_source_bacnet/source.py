# metricq-source-bacnet
# Copyright (C) 2020 ZIH, Technische Universitaet Dresden, Federal Republic of Germany
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
import asyncio
import functools
import random
import threading
from asyncio import Future
from string import Template
from typing import Dict, List, Optional, Tuple, Union

from metricq import Source, Timedelta, Timestamp, get_logger, rpc_handler
from metricq_source_bacnet.bacnet_application import BacNetMetricQReader

logger = get_logger(__name__)


def unpack_range(range_str: str) -> List[int]:
    ret = []
    for r in range_str.split(","):
        if "-" in r:
            start, stop = r.split("-")
            for i in range(int(start), int(stop) + 1):
                ret.append(i)
        else:
            ret.append(int(r))
    return ret


def substitute_all(string: str, substitutions: dict) -> str:
    for k, v in substitutions.items():
        string = string.replace(k, v)
    return string


class BacnetSource(Source):
    def __init__(self, *args, disk_cache_filename=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._bacnet_reader: Optional[BacNetMetricQReader] = None
        self._result_queue = asyncio.Queue()
        self._main_task_stop_future = None
        self._worker_stop_futures: List[Future] = []
        self.disk_cache_filename = disk_cache_filename

    @rpc_handler("config")
    async def _on_config(self, **config):

        if self._bacnet_reader:
            self._bacnet_reader.stop()

        for worker_stop_future in self._worker_stop_futures:
            worker_stop_future.set_result(None)

        self._bacnet_reader = BacNetMetricQReader(
            reader_address=config["bacnetReaderAddress"],
            reader_object_identifier=config["bacnetReaderObjectIdentifier"],
            put_result_in_source_queue_fn=self._bacnet_reader_put_result_in_source_queue,
            disk_cache_filename=self.disk_cache_filename,
        )
        self._bacnet_reader.start()

        self._object_groups: List[Dict[str, Union[str, int]]] = []
        self._device_config: Dict[str, Dict] = {}
        for device_address_str, device_config in config["devices"].items():
            object_group_device_config = {
                "metric_id": device_config["metricId"],
                "description": device_config.get("description", "$objectDescription"),
            }

            self._device_config[device_address_str] = object_group_device_config

            object_group_device_config["device_address_str"] = device_address_str

            for object_config in device_config["objectGroups"]:
                object_group_config = {
                    "object_type": object_config["objectType"],
                    "object_instances": unpack_range(object_config["objectInstance"]),
                    "interval": object_config["interval"],
                }
                object_group_config.update(object_group_device_config)

                if "description" in object_config:
                    object_group_config["description"] = object_config["description"]

                self._object_groups.append(object_group_config)

        self._object_name_vendor_specific_mapping = config.get(
            "vendorSpecificMapping", {}
        )

        self._object_description_vendor_specific_substitutions = config.get(
            "vendorSpecificDescriptionSubstitutions", {}
        )

        self._worker_stop_futures = []
        for object_group in self._object_groups:
            worker_stop_future = self.event_loop.create_future()
            self._worker_stop_futures.append(worker_stop_future)

            self.event_loop.create_task(
                self._worker_task(object_group, worker_stop_future)
            )

    async def task(self):
        self._main_task_stop_future = self.event_loop.create_future()

        while True:
            queue_get_task = asyncio.create_task(self._result_queue.get())
            done, pending = await asyncio.wait(
                {queue_get_task, self._main_task_stop_future},
                return_when=asyncio.FIRST_COMPLETED,
            )

            if queue_get_task in done:
                result: Tuple[Timestamp, str, str, Dict] = queue_get_task.result()

                timestamp, device_name, device_address_string, result_values = result

                device_config = self._device_config[device_address_string]
                device_name = self._object_name_vendor_specific_mapping.get(
                    device_name, device_name
                )

                for object_name, object_result in result_values.items():
                    object_name = self._object_name_vendor_specific_mapping.get(
                        object_name, object_name
                    )

                    # TODO maybe support more placeholders
                    metric_id = (
                        Template(device_config["metric_id"])
                        .safe_substitute(
                            {"objectName": object_name, "deviceName": device_name}
                        )
                        .replace("'", ".")
                        .replace("`", ".")
                        .replace("´", ".")
                        .replace(" ", "")
                    )
                    if "presentValue" in object_result and (
                        object_result["presentValue"],
                        (int, float),
                    ):
                        await self.send(
                            metric_id, timestamp, object_result["presentValue"]
                        )

                self._result_queue.task_done()

            if self._main_task_stop_future in done:
                logger.info("stopping BACnetSource main task")
                break

    async def stop(self, exception: Optional[Exception]):
        logger.debug("stop()")
        self._bacnet_reader.stop()

        for worker_stop_future in self._worker_stop_futures:
            worker_stop_future.set_result(None)

        await self._result_queue.join()

        if self._main_task_stop_future is not None:
            self._main_task_stop_future.set_result(None)

        await super().stop(exception)

    # this method is called from bacpypes runner thread, so make queue put threadsafe
    def _bacnet_reader_put_result_in_source_queue(
        self, device_name: str, device_address_string: str, result_values: Dict
    ):
        fut = asyncio.run_coroutine_threadsafe(
            self._result_queue.put(
                (Timestamp.now(), device_name, device_address_string, result_values)
            ),
            loop=self.event_loop,
        )
        try:
            fut.result()
        except Exception:
            logger.exception("Can't put BACnet result in queue!")

    async def _worker_task(self, object_group, worker_task_stop_future):
        interval = object_group["interval"]
        device_address_str = object_group["device_address_str"]
        object_type = object_group["object_type"]
        objects = [
            (object_type, instance) for instance in object_group["object_instances"]
        ]

        logger.debug(
            "This is {} the main thread.",
            "" if threading.current_thread() == threading.main_thread() else "not",
        )

        # wait for random time between 10 ms and 10.01s
        random_wait_time = random.random() * 10 + 0.01
        await asyncio.sleep(random_wait_time)

        await self.event_loop.run_in_executor(
            None,
            functools.partial(
                self._bacnet_reader.request_device_properties,
                device_address_str,
                skip_when_cached=True,
            ),
        )
        await self.event_loop.run_in_executor(
            None,
            functools.partial(
                self._bacnet_reader.request_object_properties,
                device_address_str,
                objects,
                skip_when_cached=True,
            ),
        )

        device_info = self._bacnet_reader.get_device_info(device_address_str)
        if device_info is None:
            logger.error(
                "Missing device info for {}. Stopping worker task!", device_address_str
            )
            return

        device_name = self._object_name_vendor_specific_mapping.get(
            device_info["objectName"], device_info["objectName"]
        )

        metrics = {}

        for object_instance in object_group["object_instances"]:
            metadata = {"rate": 1.0 / interval}
            object_info = self._bacnet_reader.get_object_info(
                device_address_str, object_type, object_instance
            )
            if object_info is None:
                logger.error(
                    "No object info for ({}, {}) of {} available!",
                    object_type,
                    object_instance,
                    device_address_str,
                )
                continue

            object_name = self._object_name_vendor_specific_mapping.get(
                object_info["objectName"], object_info["objectName"]
            )

            metric_id = (
                Template(object_group["metric_id"])
                .safe_substitute({"objectName": object_name, "deviceName": device_name})
                .replace("'", ".")
                .replace("`", ".")
                .replace("´", ".")
                .replace(" ", "")
            )
            if "description" in object_group:
                description = (
                    Template(object_group["description"])
                    .safe_substitute(
                        {
                            "objectName": object_name,
                            "objectDescription": object_info["description"],
                            "deviceName": device_name,
                            "deviceDescription": device_info["description"],
                        }
                    )
                    .replace("'", ".")
                    .replace("`", ".")
                    .replace("´", ".")
                )
                metadata["description"] = substitute_all(
                    description, self._object_description_vendor_specific_substitutions
                )
            if "units" in object_info:
                metadata["unit"] = object_info["units"]

            metrics[metric_id] = metadata

        await self.declare_metrics(metrics)

        deadline = Timestamp.now()
        while True:
            self._bacnet_reader.request_values(device_address_str, objects)

            try:
                deadline += Timedelta.from_s(interval)
                now = Timestamp.now()
                while now >= deadline:
                    logger.warn("Missed deadline {}, it is now {}", deadline, now)
                    deadline += Timedelta.from_s(interval)

                timeout = (deadline - now).s
                await asyncio.wait_for(
                    asyncio.shield(worker_task_stop_future), timeout=timeout
                )
                worker_task_stop_future.result()
                logger.info("stopping BACnetSource worker task")
                break
            except asyncio.TimeoutError:
                # This is the normal case, just continue with the loop
                continue
