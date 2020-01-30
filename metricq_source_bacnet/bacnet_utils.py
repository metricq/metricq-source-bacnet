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

from bacpypes.app import DeviceInfo, DeviceInfoCache
from metricq import get_logger

logger = get_logger(__name__)


class BetterDeviceInfoCache(DeviceInfoCache):
    def update_device_info(self, device_info):
        super(BetterDeviceInfoCache, self).update_device_info(device_info)

        cache_id, cache_address = getattr(device_info, "_cache_keys", (None, None))

        if cache_id is None:
            self.cache[device_info.deviceIdentifier] = device_info
        else:
            if not self.has_device_info(device_info.deviceIdentifier):
                self.cache[device_info.deviceIdentifier] = device_info

        if cache_address is None:
            self.cache[device_info.address] = device_info
        else:
            if not self.has_device_info(device_info.address):
                self.cache[device_info.address] = device_info

    def acquire(self, key):
        if isinstance(key, DeviceInfo):
            logger.debug("Got wrong type of key, fixing this...")
            key = key.deviceIdentifier

        return super(BetterDeviceInfoCache, self).acquire(key)
