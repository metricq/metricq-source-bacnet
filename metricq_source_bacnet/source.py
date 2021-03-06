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

from bacpypes.pdu import Address
from metricq import Source, Timedelta, Timestamp, get_logger, rpc_handler
from metricq_source_bacnet.bacnet.application import BACnetMetricQReader
from metricq_source_bacnet.bacnet.object_types import register_extended_object_types

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
        self._bacnet_reader: Optional[BACnetMetricQReader] = None
        self._result_queue = asyncio.Queue()
        self._main_task_stop_future = None
        self._worker_stop_futures: List[Future] = []
        self.disk_cache_filename = disk_cache_filename

        register_extended_object_types()

    @rpc_handler("config")
    async def _on_config(self, **config):

        if self._bacnet_reader:
            self._bacnet_reader.stop()

        for worker_stop_future in self._worker_stop_futures:
            worker_stop_future.set_result(None)

        self._bacnet_reader = BACnetMetricQReader(
            reader_address=config["bacnetReaderAddress"],
            reader_object_identifier=config["bacnetReaderObjectIdentifier"],
            put_result_in_source_queue_fn=self._bacnet_reader_put_result_in_source_queue,
            disk_cache_filename=self.disk_cache_filename,
            retry_count=config.get("bacnetReaderRetryCount", 10),
        )
        self._bacnet_reader.start()

        self._object_groups: List[Dict[str, Union[str, int]]] = []
        self._device_config: Dict[str, Dict] = {}
        for device_address_str, device_config in config["devices"].items():
            object_group_device_config = {
                "metric_id": device_config["metricId"],
                "description": device_config.get("description", "$objectDescription"),
                "chunk_size": device_config.get("chunkSize"),
                "device_identifier": device_config.get("deviceIdentifier"),
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

        self._object_name_vendor_specific_substitutions = config.get(
            "vendorSpecificNameSubstitutions", {}
        )

        self._object_type_filter = (
            config.get(
                "discoverObjectTypeFilter",
                ["analogValue", "analogInput", "analogOutput", "pulseConverter"],
            )
            + ["device"]
        )

        self._worker_stop_futures = []
        for object_group in self._object_groups:
            if object_group["object_type"] not in self._object_type_filter:
                self._object_type_filter.append(object_group["object_type"])

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

                device_name = substitute_all(
                    device_name, self._object_name_vendor_specific_substitutions
                )

                for object_name, object_result in result_values.items():
                    object_name = self._object_name_vendor_specific_mapping.get(
                        object_name, object_name
                    )

                    object_name = substitute_all(
                        object_name, self._object_name_vendor_specific_substitutions
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
                    if "presentValue" in object_result and isinstance(
                        object_result["presentValue"], (int, float)
                    ):
                        await self.send(
                            metric_id, timestamp, object_result["presentValue"]
                        )

                self._result_queue.task_done()

            if self._main_task_stop_future in done:
                logger.info("stopping BACnetSource main task")
                break

    async def stop(self, exception: Optional[Exception] = None):
        logger.debug("stop()")

        for worker_stop_future in self._worker_stop_futures:
            if not worker_stop_future.done():
                worker_stop_future.set_result(None)

        try:
            self._bacnet_reader.stop()
        except Exception as ex:
            logger.error(
                "Exception while stopping bacnet_reader: {}",
                type(ex).__qualname__,
                exc_info=(ex.__class__, ex, ex.__traceback__),
            )

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
        chunk_size = object_group.get("chunk_size")

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
                chunk_size=chunk_size,
            ),
        )

        device_info = self._bacnet_reader.get_device_info(
            device_address_str, device_identifier=object_group.get("device_identifier")
        )
        if device_info is None:
            logger.error(
                "Missing device info for {}. Stopping worker task!", device_address_str
            )
            return

        device_name = self._object_name_vendor_specific_mapping.get(
            device_info["objectName"], device_info["objectName"]
        )

        device_name = substitute_all(
            device_name, self._object_name_vendor_specific_substitutions
        )

        metrics = {}

        for object_instance in object_group["object_instances"]:
            metadata = {
                "rate": 1.0 / interval,
                "device": device_address_str,
                "objectType": object_type,
                "objectInstance": object_instance,
            }
            object_info = self._bacnet_reader.get_object_info(
                device_address_str, object_type, object_instance
            )
            if (
                object_info is None
                or "objectName" not in object_info
                or "description" not in object_info
            ):
                logger.error(
                    "No object info for ({}, {}) of {} available!",
                    object_type,
                    object_instance,
                    device_address_str,
                )
                continue

            # Get vendor-specific-address from object cache
            object_name = object_info.get("3000", object_info["objectName"])

            object_name = self._object_name_vendor_specific_mapping.get(
                object_name, object_name
            )

            object_name = substitute_all(
                object_name, self._object_name_vendor_specific_substitutions
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

        segmentationSupport = "unknown"
        device_address = Address(device_address_str)
        device_info = self._bacnet_reader.deviceInfoCache.get_device_info(
            device_address
        )
        if device_info:
            segmentationSupport = device_info.segmentationSupported

        deadline = Timestamp.now()
        while True:
            self._bacnet_reader.request_values(
                device_address_str, objects, chunk_size=chunk_size
            )

            try:
                deadline += Timedelta.from_s(interval)
                now = Timestamp.now()
                while now >= deadline:
                    logger.warn(
                        "Missed deadline {}, it is now {}. Device: {}, {}, chunk size: {}",
                        deadline,
                        now,
                        device_address_str,
                        segmentationSupport,
                        chunk_size,
                    )
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

    @rpc_handler("source_bacnet.get_advertised_devices")
    async def _on_get_advertised_devices(self, **kwargs):
        # {"ip": {"device_id": 1234, "device_name": "TRE.BLOB"}}
        cached_devices = self._bacnet_reader.get_device_info_for_cached_devices()
        for address in cached_devices.keys():
            device_name = cached_devices[address]["device_name"]
            if device_name in self._object_name_vendor_specific_mapping:
                cached_devices[address][
                    "device_name"
                ] = f"{self._object_name_vendor_specific_mapping[device_name]} (orig: {device_name})"
        return cached_devices

    @rpc_handler("source_bacnet.get_device_name_from_ip")
    async def _on_get_device_name_from_ip(self, ips, **kwargs):
        # {"ip": {"device_id": 1234, "device_name": "TRE.BLOB"}}
        devices = {}
        for ip in ips:
            device_info = self._bacnet_reader.get_device_info(ip)
            device_id = self._bacnet_reader.get_device_id_for_ip(ip)
            if device_info:
                device_name = device_info["objectName"]
            else:
                device_name = "N/A"

            devices[ip] = {"device_id": device_id, "device_name": device_name}

            if device_name in self._object_name_vendor_specific_mapping:
                devices[ip][
                    "device_name"
                ] = f"{self._object_name_vendor_specific_mapping[device_name]} (orig: {device_name})"
        return devices

    @rpc_handler("source_bacnet.get_object_list_with_info")
    async def _on_get_object_list_with_info(self, ip, **kwargs):
        device_properties = await self.event_loop.run_in_executor(
            None,
            functools.partial(
                self._bacnet_reader.request_device_properties,
                device_address_str=ip,
                properties=["objectList"],
            ),
        )
        if device_properties and "objectList" in device_properties:
            object_instance_list = device_properties["objectList"]
            objects_not_in_cache = []
            object_info_list_from_cache = {}
            for object_identifier in object_instance_list:
                object_type, object_instance = object_identifier
                if object_type not in self._object_type_filter:
                    logger.debug(f"Ignoring object type: {object_type}")
                    continue

                object_info_from_cache = self._bacnet_reader.get_object_info(
                    device_address_str=ip,
                    object_type=object_type,
                    object_instance=object_instance,
                )
                if (
                    object_info_from_cache is None
                    or "objectName" not in object_info_from_cache
                    or "description" not in object_info_from_cache
                ):
                    objects_not_in_cache.append(object_identifier)
                else:
                    object_info_list_from_cache[
                        object_identifier
                    ] = object_info_from_cache

            logger.debug(f"Objects missing in cache: {len(objects_not_in_cache)}")
            object_info_list = await self.event_loop.run_in_executor(
                None,
                functools.partial(
                    self._bacnet_reader.request_object_properties,
                    device_address_str=ip,
                    objects=objects_not_in_cache,
                    properties=["objectName", "description"],
                ),
            )

            if object_info_list:
                object_info_list_from_cache.update(object_info_list)

            if object_info_list_from_cache:
                return {
                    "{}-{}".format(*k): v
                    for k, v in object_info_list_from_cache.items()
                }
        return {}
