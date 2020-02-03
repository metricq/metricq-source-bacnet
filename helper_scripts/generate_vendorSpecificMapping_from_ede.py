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
import csv
import json

import click


@click.command()
@click.argument("ede-file", type=click.File("r"))
def main(ede_file):
    ede_reader = csv.reader(ede_file, delimiter=";")

    header_compeleted = False
    vendor_specific_address_column = -1
    object_name_column = -1
    vendor_specific_address_mapping = {}

    for ede_row in ede_reader:
        if len(ede_row) > 0:
            if not header_compeleted:
                if ede_row[0].startswith("#") and "keyname" in ede_row[0]:
                    header_compeleted = True
                    vendor_specific_address_column = ede_row.index(
                        "vendor-specific-address"
                    )
                    object_name_column = ede_row.index("object-name")
            else:
                if ede_row[vendor_specific_address_column].strip():
                    vendor_specific_address_mapping[
                        ede_row[object_name_column]
                    ] = ede_row[vendor_specific_address_column]

    click.echo(json.dumps(vendor_specific_address_mapping))


if __name__ == "__main__":
    main()
