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
import json
import threading
import time
from threading import RLock, Thread
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Union

from bacpypes.apdu import (
    ReadAccessResult,
    ReadAccessResultElement,
    ReadAccessResultElementChoice,
    ReadAccessSpecification,
    ReadPropertyMultipleACK,
    ReadPropertyMultipleRequest,
    ConfirmedRequestPDU,
)
from bacpypes.app import BIPSimpleApplication, DeviceInfo
from bacpypes.basetypes import PropertyIdentifier, PropertyReference
from bacpypes.constructeddata import Array
from bacpypes.core import deferred
from bacpypes.core import run as bacnet_run
from bacpypes.core import stop as bacnet_stop
from bacpypes.core import enable_sleeping as bacnet_enable_sleeping
from bacpypes.iocb import IOCB
from bacpypes.local.device import LocalDeviceObject
from bacpypes.object import get_datatype
from bacpypes.pdu import Address
from bacpypes.primitivedata import ObjectIdentifier, Unsigned, Enumerated
from metricq import get_logger, Timedelta
from metricq_source_bacnet.bacnet_utils import BetterDeviceInfoCache

logger = get_logger(__name__)


def _cachekey_tuple_to_str(cache_key_tuple: Tuple[str, str, int]) -> str:
    return "{}-*-{}-*-{}".format(*cache_key_tuple)


def _cachekey_str_to_tuple(cache_key_str: str) -> Tuple[str, str, int]:
    device_address_str, object_type, object_instance = cache_key_str.split("-*-", 3)
    return device_address_str, object_type, int(object_instance)


# from https://stackoverflow.com/a/312464
def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


class BACnetMetricQReader(BIPSimpleApplication):
    def __init__(
        self,
        reader_address,
        reader_object_identifier,
        put_result_in_source_queue_fn: Callable[[str, str, Dict], None],
        disk_cache_filename=None,
        retry_count=10,
    ):
        self._thread = Thread(target=bacnet_run)
        # MetricQ Bacnet Run Thread
        self._thread.name = "MQBRT#{}".format(reader_address)

        bacnet_enable_sleeping()

        self._put_result_in_source_queue = put_result_in_source_queue_fn

        #  cache for object properties
        #  key is (device address, object type, object instance)
        #  value is dict of property name and value
        self._disk_cache_filename = disk_cache_filename
        self._object_info_cache_lock = RLock()
        with self._object_info_cache_lock:
            self._object_info_cache: Dict[Tuple[str, str, int], Dict[str, Any]] = {}
            if disk_cache_filename:
                try:
                    with open(disk_cache_filename) as disk_cache_file:
                        self._object_info_cache = {
                            _cachekey_str_to_tuple(key): value
                            for key, value in json.load(disk_cache_file).items()
                        }
                except (OSError, json.decoder.JSONDecodeError):
                    logger.warning(
                        "Can't read disk cache file. Starting with empty cache!",
                        exc_info=True,
                    )

        self._retry_count = retry_count

        local_device_object = LocalDeviceObject(
            objectName="MetricQReader",
            objectIdentifier=reader_object_identifier,
            vendorIdentifier=15,
            maxApduLengthAccepted=1476,  # 1024
            segmentationSupported="segmentedBoth",
        )

        BIPSimpleApplication.__init__(
            self,
            local_device_object,
            reader_address,
            deviceInfoCache=BetterDeviceInfoCache(),
        )

    def start(self):
        self._thread.start()

    def stop(self):
        bacnet_stop()

        try:
            self.close_socket()
        except:
            logger.exception("Error while closing bacpypes socket!")

        if self._disk_cache_filename:
            try:
                with open(self._disk_cache_filename, "w") as disk_cache_file:
                    json.dump(
                        {
                            _cachekey_tuple_to_str(key): value
                            for key, value in self._object_info_cache.items()
                        },
                        disk_cache_file,
                    )
            except OSError:
                logger.warning("Can't write disk cache file!", exc_info=True)

        self._thread.join(timeout=30.0)

        if self._thread.is_alive():
            logger.error("BACnet thread join timeouted")
            raise RuntimeError("Joining BACnet runner thread timeouted!")

    # TODO maybe at more checks from _iocb_callback
    def _unpack_iocb(
        self, iocb: IOCB, enum_to_int: bool = False
    ) -> Optional[Dict[Tuple[Union[str, int], int], Dict[str, Any]]]:
        apdu = iocb.ioResponse

        # should be ack
        if not isinstance(apdu, ReadPropertyMultipleACK):
            return None

        apdu: ReadPropertyMultipleACK

        # loop through the results
        result_values: Dict[Tuple[Union[str, int], int], Dict[str, Any]] = {}

        result: ReadAccessResult
        for result in apdu.listOfReadAccessResults:
            object_identifier: Tuple[Union[str, int], int] = result.objectIdentifier
            object_type, object_instance = object_identifier

            results_for_object = result_values.get(object_identifier, {})

            # now come the property values per object
            element: ReadAccessResultElement
            for element in result.listOfResults:
                # get the property and array index
                property_identifier: PropertyIdentifier = element.propertyIdentifier
                property_label = str(property_identifier)

                property_array_index: int = element.propertyArrayIndex

                # here is the read result
                read_result: ReadAccessResultElementChoice = element.readResult

                # check for an error
                if read_result.propertyAccessError is not None:
                    if property_array_index is not None:
                        property_label += "[" + str(property_array_index) + "]"
                    logger.error(
                        "Error reading property {} for object {} from device {}: {} ({})",
                        property_label,
                        object_identifier,
                        apdu.pduSource,
                        read_result.propertyAccessError.errorClass,
                        read_result.propertyAccessError.errorCode,
                    )

                else:
                    # here is the value
                    property_value = read_result.propertyValue

                    # find the datatype
                    datatype = get_datatype(object_type, property_identifier)

                    if datatype is not None:
                        # special case for array parts, others are managed by cast_out
                        if issubclass(datatype, Array) and (
                            property_array_index is not None
                        ):
                            # build a dict for the array collecting length and all indices
                            if property_array_index == 0:
                                # array length as Unsigned
                                property_result = results_for_object.get(
                                    property_label, {}
                                )
                                property_result["length"] = property_value.cast_out(
                                    Unsigned
                                )
                            else:
                                property_result = results_for_object.get(
                                    property_label, {}
                                )
                                property_result[
                                    property_array_index
                                ] = property_value.cast_out(datatype.subtype)
                        elif issubclass(datatype, Enumerated) and enum_to_int:
                            results_for_object[property_label] = datatype(
                                property_value.cast_out(datatype)
                            ).get_long()
                        else:
                            results_for_object[
                                property_label
                            ] = property_value.cast_out(datatype)

            result_values[object_identifier] = results_for_object

        return result_values

    def _iocb_callback(self, iocb: IOCB):
        if iocb.ioResponse:
            apdu = iocb.ioResponse

            # should be ack
            if not isinstance(apdu, ReadPropertyMultipleACK):
                return

            apdu: ReadPropertyMultipleACK

            device_addr_str = str(apdu.pduSource)  # address of the device
            device_info: DeviceInfo = self.deviceInfoCache.get_device_info(
                apdu.pduSource
            )
            device_name: Optional[str] = self._object_info_cache.get(
                (device_addr_str, "device", device_info.deviceIdentifier), {}
            ).get("objectName")

            if device_name:
                result_values_by_id = self._unpack_iocb(iocb, enum_to_int=True)

                result_values = {}
                for object_type, object_instance in result_values_by_id:
                    object_name: Optional[str] = self._object_info_cache.get(
                        (device_addr_str, object_type, object_instance), {}
                    ).get("objectName")

                    if object_name:
                        result_values[object_name] = result_values_by_id[
                            (object_type, object_instance)
                        ]

                self._put_result_in_source_queue(
                    device_name, device_addr_str, result_values
                )

        # do something for error/reject/abort
        if iocb.ioError:
            device_addr = ""
            if len(iocb.args) > 0 and isinstance(iocb.args[0], ConfirmedRequestPDU):
                device_addr = iocb.args[0].pduDestination

            logger.error(
                "IOCB returned with error in _iocb_callback (device {}): {}",
                device_addr,
                iocb.ioError,
            )

    def who_is(self, low_limit=None, high_limit=None, address=None):
        super(BACnetMetricQReader, self).who_is(low_limit, high_limit, address)
        # TODO maybe save request for incoming I-Am

    def do_IAmRequest(self, apdu):
        super(BACnetMetricQReader, self).do_IAmRequest(apdu)

        if threading.current_thread() != self._thread:
            logger.warning(
                "IAm-Request handler not called from BACpypes thread! {}",
                threading.current_thread(),
            )

        logger.debug("New device info {}", apdu.pduSource)
        self.deviceInfoCache.iam_device_info(apdu)

    def request_device_properties(
        self,
        device_address_str: str,
        properties=None,
        skip_when_cached=False,
        request_timeout: Optional[Timedelta] = None,
    ):
        if threading.current_thread() == threading.main_thread():
            logger.error(
                "request_device_properties called from main thread! Run it from an executor!",
                stack_info=True,
            )

        if properties is None:
            properties = ["objectName", "description"]

        device_address = Address(device_address_str)
        device_info: DeviceInfo = self.deviceInfoCache.get_device_info(device_address)

        if not device_info:
            for retry in range(self._retry_count):
                deferred(self.who_is, address=device_address)
                time.sleep(5 * (retry + 1))

                device_info: DeviceInfo = self.deviceInfoCache.get_device_info(
                    device_address
                )
                if device_info:
                    break
            else:
                device_info: DeviceInfo = self.deviceInfoCache.get_device_info(
                    device_address
                )

                if not device_info:
                    logger.error(
                        "Device with address {} is not in device cache!",
                        device_address_str,
                    )
                    return

        if skip_when_cached:
            cache_key = (device_address_str, "device", device_info.deviceIdentifier)
            if cache_key in self._object_info_cache:
                cached_object_info = self._object_info_cache[cache_key]
                if all(property in cached_object_info for property in properties):
                    logger.debug("Device info already in cache. Skipping!")
                    return

        prop_reference_list = [
            PropertyReference(propertyIdentifier=property) for property in properties
        ]

        device_object_identifier = ("device", device_info.deviceIdentifier)

        read_access_spec = ReadAccessSpecification(
            objectIdentifier=device_object_identifier,
            listOfPropertyReferences=prop_reference_list,
        )

        request = ReadPropertyMultipleRequest(listOfReadAccessSpecs=[read_access_spec])
        request.pduDestination = device_address

        iocb = IOCB(request)
        deferred(self.request_io, iocb)
        if request_timeout:
            iocb.set_timeout(request_timeout.s)

        iocb.wait()

        if iocb.ioResponse:
            result_values = self._unpack_iocb(iocb)
            if device_object_identifier in result_values:
                with self._object_info_cache_lock:
                    cache_key = (
                        device_address_str,
                        "device",
                        device_info.deviceIdentifier,
                    )
                    if cache_key not in self._object_info_cache:
                        self._object_info_cache[cache_key] = {}
                    self._object_info_cache[cache_key].update(
                        result_values[device_object_identifier]
                    )

            return result_values[device_object_identifier]

        # do something for error/reject/abort
        if iocb.ioError:
            logger.error(
                "IOCB returned with error for device properties request (device {}, objects {}, props {}) : {}",
                device_address_str,
                device_object_identifier,
                properties,
                iocb.ioError,
            )

        return None

    def request_object_properties(
        self,
        device_address_str: str,
        objects: Sequence[Tuple[Union[int, str], int]],
        properties=None,
        skip_when_cached=False,
        chunk_size: Optional[int] = None,
        request_timeout: Optional[Timedelta] = None,
    ):
        if threading.current_thread() == threading.main_thread():
            logger.error(
                "request_object_properties called from main thread! Run it from an executor!",
                stack_info=True,
            )

        if properties is None:
            properties = ["objectName", "description", "units"]

        device_address = Address(device_address_str)
        device_info: DeviceInfo = self.deviceInfoCache.get_device_info(device_address)

        if skip_when_cached:
            object_to_request = []
            for object_type, object_instance in objects:
                cache_key = (device_address_str, object_type, object_instance)
                if cache_key in self._object_info_cache:
                    cached_object_info = self._object_info_cache[cache_key]
                    if all(property in cached_object_info for property in properties):
                        logger.debug(
                            "Object info for {} already in cache. Skipping!",
                            (object_type, object_instance),
                        )
                        continue

                object_to_request.append((object_type, object_instance))

            if not object_to_request:
                logger.debug("All objects already in cache")
                return

            objects = object_to_request

        result_values = {}

        if not chunk_size:
            chunk_size = 20
            if device_info and device_info.segmentationSupported == "noSegmentation":
                chunk_size = 4

        for objects_chunk in chunks(objects, chunk_size):
            prop_reference_list = [
                PropertyReference(propertyIdentifier=property)
                for property in properties
            ]

            read_access_specs = [
                ReadAccessSpecification(
                    objectIdentifier=ObjectIdentifier(object_identifier),
                    listOfPropertyReferences=prop_reference_list,
                )
                for object_identifier in objects_chunk
            ]

            request = ReadPropertyMultipleRequest(
                listOfReadAccessSpecs=read_access_specs
            )
            request.pduDestination = device_address

            iocb = IOCB(request)
            deferred(self.request_io, iocb)
            if request_timeout:
                iocb.set_timeout(request_timeout.s)

            iocb.wait()

            if iocb.ioResponse:
                chunk_result_values = self._unpack_iocb(iocb)
                for object_identifier in chunk_result_values:
                    object_type, object_instance = object_identifier
                    with self._object_info_cache_lock:
                        cache_key = (device_address_str, object_type, object_instance)
                        if cache_key not in self._object_info_cache:
                            self._object_info_cache[cache_key] = {}
                        self._object_info_cache[cache_key].update(
                            chunk_result_values[object_identifier]
                        )

                result_values.update(chunk_result_values)

            # do something for error/reject/abort
            if iocb.ioError:
                logger.error(
                    "IOCB returned with error for object properties request (device {}, objects {}, props {}): {}",
                    device_address_str,
                    objects,
                    properties,
                    iocb.ioError,
                )
                # TODO: maybe raise error here

        return result_values

    def request_values(
        self,
        device_address_str: str,
        objects: Sequence[Tuple[Union[int, str], int]],
        chunk_size: Optional[int] = None,
        request_timeout: Optional[Timedelta] = None,
    ):
        device_address = Address(device_address_str)
        device_info: DeviceInfo = self.deviceInfoCache.get_device_info(device_address)

        # we adjusted chunking for object property request, which requested 3 properties per object
        # here we request only 1 property so scale
        chunk_scale = 3

        if not chunk_size:
            chunk_size = 20 * chunk_scale
            if device_info and device_info.segmentationSupported == "noSegmentation":
                chunk_size = 4 * chunk_scale

        logger.debug(f"Chunking for device {device_address_str} is {chunk_size}")

        for objects_chunk in chunks(objects, chunk_size):
            prop_reference_list = [PropertyReference(propertyIdentifier="presentValue")]

            read_access_specs = [
                ReadAccessSpecification(
                    objectIdentifier=ObjectIdentifier(object_identifier),
                    listOfPropertyReferences=prop_reference_list,
                )
                for object_identifier in objects_chunk
            ]

            request = ReadPropertyMultipleRequest(
                listOfReadAccessSpecs=read_access_specs
            )
            request.pduDestination = device_address

            iocb = IOCB(request)
            iocb.add_callback(self._iocb_callback)
            if request_timeout:
                iocb.set_timeout(request_timeout.s)

            deferred(self.request_io, iocb)

    def get_device_info(
        self, device_address_str: str, device_identifier: Optional[int] = None
    ):
        device_address = Address(device_address_str)
        device_info: DeviceInfo = self.deviceInfoCache.get_device_info(device_address)
        if device_info:
            if device_identifier and device_info.deviceIdentifier != device_identifier:
                logger.warning(
                    f"Device identifier mismatch for device {device_address_str}: provided [{device_identifier}], bacnet [{device_info.deviceIdentifier}]"
                )
            return self.get_object_info(
                device_address_str, "device", device_info.deviceIdentifier
            )

        if device_identifier:
            logger.info(
                f"Using provided device identifier {device_identifier} for device {device_address_str}"
            )
            return self.get_object_info(device_address_str, "device", device_identifier)

        # TODO maybe fill cache here

        return None

    def get_object_info(
        self, device_address_str: str, object_type, object_instance
    ) -> Optional[Dict[str, Any]]:
        cache_key = (device_address_str, object_type, object_instance)
        if cache_key in self._object_info_cache:
            return self._object_info_cache[cache_key]

        # TODO maybe fill cache here

        return None

    def get_device_info_for_cached_devices(self):
        cached_devices = {}
        for key in self.deviceInfoCache.cache.keys():
            if isinstance(key, Address):
                if key not in cached_devices:
                    cached_devices[str(key)] = {
                        "device_id": self.deviceInfoCache.get_device_info(
                            key
                        ).deviceIdentifier
                    }

            if isinstance(key, int):
                device_info = self.deviceInfoCache.get_device_info(key)
                if device_info.address not in cached_devices:
                    cached_devices[str(device_info.address)] = {"device_id": key}

        for address in cached_devices.keys():
            object_info = self.get_object_info(
                address, "device", cached_devices[address]["device_id"]
            )
            if object_info and "objectName" in object_info:
                cached_devices[address]["device_name"] = object_info["objectName"]
            else:
                cached_devices[address]["device_name"] = "N/A"

        return cached_devices

    def get_device_id_for_ip(self, device_address_str: str):
        device_address = Address(device_address_str)
        device_info: DeviceInfo = self.deviceInfoCache.get_device_info(device_address)
        if device_info:
            return device_info.deviceIdentifier

        return None
