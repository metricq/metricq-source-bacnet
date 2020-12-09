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
from typing import Tuple

import click

def _cachekey_str_to_tuple(cache_key_str: str) -> Tuple[str, str, int]:
    device_address_str, object_type, object_instance = cache_key_str.split("-*-", 3)
    return device_address_str, object_type, int(object_instance)

@click.command()
@click.option("--cache-file", type=click.File("r"), required=True)
@click.argument("search-string")
def main(cache_file, search_string):
    object_info_cache = {
        _cachekey_str_to_tuple(key): value
        for key, value in json.load(cache_file).items()
    }

    for cache_entry in object_info_cache:
        object_name = object_info_cache[cache_entry].get("objectName")
        if object_name and search_string in object_name:
            click.echo(f"{cache_entry}: {object_info_cache[cache_entry]}")


if __name__ == "__main__":
    main()
