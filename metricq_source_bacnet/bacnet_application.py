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
import threading
import time
from threading import RLock, Thread
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Union

from bacpypes.apdu import (ReadAccessResult, ReadAccessResultElement,
                           ReadAccessResultElementChoice,
                           ReadAccessSpecification, ReadPropertyMultipleACK,
                           ReadPropertyMultipleRequest)
from bacpypes.app import BIPSimpleApplication, DeviceInfo
from bacpypes.basetypes import PropertyIdentifier, PropertyReference
from bacpypes.constructeddata import Array
from bacpypes.core import deferred
from bacpypes.core import run as bacnet_run
from bacpypes.core import stop as bacnet_stop
from bacpypes.iocb import IOCB
from bacpypes.local.device import LocalDeviceObject
from bacpypes.object import get_datatype
from bacpypes.pdu import Address
from bacpypes.primitivedata import ObjectIdentifier, Unsigned
from metricq import get_logger
from metricq_source_bacnet.bacnet_utils import BetterDeviceInfoCache

logger = get_logger(__name__)


class BacNetMetricQReader(BIPSimpleApplication):
    def __init__(
        self,
        reader_address,
        reader_object_identifier,
        put_result_in_source_queue_fn: Callable[[str, str, Dict], None],
    ):
        self._thread = Thread(target=bacnet_run)
        # MetricQ Bacnet Run Thread
        self._thread.name = "MQBRT#{}".format(reader_address)

        self._put_result_in_source_queue = put_result_in_source_queue_fn

        #  cache for object properties
        #  key is (device address, object type, object instance)
        #  value is dict of property name and value
        self._object_info_cache_lock = RLock()
        with self._object_info_cache_lock:
            self._object_info_cache: Dict[Tuple[str, str, int], Dict[str, Any]] = {}

        local_device_object = LocalDeviceObject(
            objectName="MetricQReader",
            objectIdentifier=reader_object_identifier,
            vendorIdentifier=15,
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
        self._thread.join()

    # TODO maybe at more checks from _iocb_callback
    def _unpack_iocb(
        self, iocb: IOCB
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
                        "Error reading property {} : {}",
                        property_label,
                        read_result.propertyAccessError,
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
            device_name: Optional[str] = self._object_info_cache[
                (device_addr_str, "device", device_info.deviceIdentifier)
            ].get("objectName")

            if device_name:
                result_values_by_id = self._unpack_iocb(iocb)

                result_values = {}
                for object_type, object_instance in result_values_by_id:
                    object_name: Optional[str] = self._object_info_cache[
                        (device_addr_str, object_type, object_instance)
                    ].get("objectName")

                    if object_name:
                        result_values[object_name] = result_values_by_id[
                            (object_type, object_instance)
                        ]

                self._put_result_in_source_queue(
                    device_name, device_addr_str, result_values
                )

        # do something for error/reject/abort
        if iocb.ioError:
            logger.error("IOCB returned with error: {}", iocb.ioError)

    def who_is(self, low_limit=None, high_limit=None, address=None):
        super(BacNetMetricQReader, self).who_is(low_limit, high_limit, address)
        # TODO maybe save request for incoming I-Am

    def do_IAmRequest(self, apdu):
        super(BacNetMetricQReader, self).do_IAmRequest(apdu)

        if threading.current_thread() != self._thread:
            logger.warning(
                "IAm-Request handler not called from BACpypes thread! {}",
                threading.current_thread(),
            )

        logger.debug("New device info {}", apdu.pduSource)
        self.deviceInfoCache.iam_device_info(apdu)

    def request_device_properties(
        self, device_address_str: str, properties=None, skip_when_cached=False
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
            deferred(self.who_is, address=device_address)
            time.sleep(5)

            device_info: DeviceInfo = self.deviceInfoCache.get_device_info(
                device_address
            )
            if not device_info:
                logger.error(
                    "Device with address {} is not in device cache!", device_address_str
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

        iocb.wait()

        if iocb.ioResponse:
            result_values = self._unpack_iocb(iocb)
            if device_object_identifier in result_values:
                with self._object_info_cache_lock:
                    self._object_info_cache[
                        (device_address_str, "device", device_info.deviceIdentifier)
                    ] = result_values[device_object_identifier]

        # do something for error/reject/abort
        if iocb.ioError:
            logger.error("IOCB returned with error: {}", iocb.ioError)

    def request_object_properties(
        self,
        device_address_str: str,
        objects: Sequence[Tuple[Union[int, str], int]],
        properties=None,
        skip_when_cached=False,
    ):
        if threading.current_thread() == threading.main_thread():
            logger.error(
                "request_object_properties called from main thread! Run it from an executor!",
                stack_info=True,
            )

        if properties is None:
            properties = ["objectName", "description", "units"]

        device_address = Address(device_address_str)

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

        prop_reference_list = [
            PropertyReference(propertyIdentifier=property) for property in properties
        ]

        read_access_specs = [
            ReadAccessSpecification(
                objectIdentifier=ObjectIdentifier(object_identifier),
                listOfPropertyReferences=prop_reference_list,
            )
            for object_identifier in objects
        ]

        request = ReadPropertyMultipleRequest(listOfReadAccessSpecs=read_access_specs)
        request.pduDestination = device_address

        iocb = IOCB(request)
        deferred(self.request_io, iocb)

        iocb.wait()

        if iocb.ioResponse:
            result_values = self._unpack_iocb(iocb)
            for object_identifier in result_values:
                object_type, object_instance = object_identifier
                with self._object_info_cache_lock:
                    self._object_info_cache[
                        (device_address_str, object_type, object_instance)
                    ] = result_values[object_identifier]

        # do something for error/reject/abort
        if iocb.ioError:
            logger.error("IOCB returned with error: {}", iocb.ioError)

    def request_values(
        self, device_address_str: str, objects: Sequence[Tuple[Union[int, str], int]]
    ):
        device_address = Address(device_address_str)

        prop_reference_list = [PropertyReference(propertyIdentifier="presentValue")]

        read_access_specs = [
            ReadAccessSpecification(
                objectIdentifier=ObjectIdentifier(object_identifier),
                listOfPropertyReferences=prop_reference_list,
            )
            for object_identifier in objects
        ]

        request = ReadPropertyMultipleRequest(listOfReadAccessSpecs=read_access_specs)
        request.pduDestination = device_address

        iocb = IOCB(request)
        iocb.add_callback(self._iocb_callback)

        deferred(self.request_io, iocb)

    def get_device_info(self, device_address_str: str):
        device_address = Address(device_address_str)
        device_info: DeviceInfo = self.deviceInfoCache.get_device_info(device_address)
        if device_info:
            return self.get_object_info(
                device_address_str, "device", device_info.deviceIdentifier
            )

        # TODO maybe fill cache here

        return None

    def get_object_info(self, device_address_str: str, object_type, object_instance):
        cache_key = (device_address_str, object_type, object_instance)
        if cache_key in self._object_info_cache:
            return self._object_info_cache[cache_key]

        # TODO maybe fill cache here

        return None
