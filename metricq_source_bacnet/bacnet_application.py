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

from threading import Thread
from typing import Callable, Dict, Tuple, Union

from bacpypes.apdu import (
    APDU,
    ReadAccessResult,
    ReadAccessResultElement,
    ReadAccessResultElementChoice,
    ReadPropertyMultipleACK,
)
from bacpypes.app import BIPSimpleApplication
from bacpypes.basetypes import PropertyIdentifier
from bacpypes.constructeddata import Array
from bacpypes.core import deferred
from bacpypes.core import run as bacnet_run
from bacpypes.core import stop as bacnet_stop
from bacpypes.iocb import IOCB
from bacpypes.local.device import LocalDeviceObject
from bacpypes.object import get_datatype
from bacpypes.primitivedata import Unsigned


class BacNetMetricQReader(BIPSimpleApplication):
    def __init__(
        self,
        reader_address,
        reader_object_identifier,
        put_result_in_source_queue_fn: Callable[[str, Dict], None],
    ):
        self._thread = Thread(target=bacnet_run)
        # MetricQ Bacnet Run Thread
        self._thread.name = "MQBRT#{}".format(reader_address)

        self._put_result_in_source_queue = put_result_in_source_queue_fn

        local_device_object = LocalDeviceObject(
            objectName="MetricQReader", objectIdentifier=reader_object_identifier
        )

        BIPSimpleApplication.__init__(self, local_device_object, reader_address)

    def start(self):
        self._thread.start()

    def stop(self):
        bacnet_stop()
        self._thread.join()

    def bacnet_request(self, request: APDU):
        iocb = IOCB(request)

        iocb.add_callback(self._iocb_callback)

        deferred(self.request_io, iocb)

    def _iocb_callback(self, iocb: IOCB):
        if iocb.ioResponse:
            apdu = iocb.ioResponse

            # should be ack
            if not isinstance(apdu, ReadPropertyMultipleACK):
                return

            apdu: ReadPropertyMultipleACK

            device_addr = str(apdu.pduSource)  # address of the device

            # loop through the results
            result_values: Dict[Tuple[Union[str, int], int]] = {}

            result: ReadAccessResult
            for result in apdu.listOfReadAccessResults:
                object_identifier: Tuple[
                    Union[str, int], int
                ] = result.objectIdentifier.get_tuple()
                object_type, object_instance = object_identifier

                object_name = object_info_cache[
                    (device_addr, object_type, object_instance)
                ].get("objectName")

                results_for_object = result_values.get(object_name, {})

                # now come the property values per object
                element: ReadAccessResultElement
                for element in result.listOfResults:
                    # get the property and array index
                    property_identifier: PropertyIdentifier = element.propertyIdentifier

                    property_array_index: int = element.propertyArrayIndex

                    # here is the read result
                    read_result: ReadAccessResultElementChoice = element.readResult

                    # check for an error
                    if read_result.propertyAccessError is not None:
                        # TODO error logging
                        property_label = str(property_identifier)
                        if property_array_index is not None:
                            property_label += "[" + str(property_array_index) + "]"
                        print(
                            "{} ! {}".format(
                                property_label, read_result.propertyAccessError
                            )
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
                                        property_identifier, {}
                                    )
                                    property_result["length"] = property_value.cast_out(
                                        Unsigned
                                    )
                                else:
                                    property_result = results_for_object.get(
                                        property_identifier, {}
                                    )
                                    property_result[
                                        property_array_index
                                    ] = property_value.cast_out(datatype.subtype)

                            else:
                                results_for_object[
                                    property_identifier
                                ] = property_value.cast_out(datatype)
                result_values[object_name] = results_for_object

            # do something with the result_values dict, key is objectIdentifier
            bacnet_id = device_cache[device_addr]
            device_name = object_info_cache[(device_addr, "device", bacnet_id)].get(
                "objectName"
            )

            self._put_result_in_source_queue(device_name, result_values)

        # do something for error/reject/abort
        if iocb.ioError:
            # TODO error logging
            print(str(iocb.ioError))
