# metricq-source-bacnet
# Copyright (C) 2022 ZIH, Technische Universitaet Dresden, Federal Republic of Germany
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
import click

from bacpypes.basetypes import EngineeringUnits
from metricq_source_bacnet.bacnet.constants import BACNET_TO_SI_UNITS

@click.command()
@click.option("--as-code/--no-as-code", default=False)
@click.option("--all/--no-all", default=False)
def main(all, as_code):
    if not as_code:
        print(f"{'#': <4} | {'BACnet': <32} | {'source-bacnet': <15}")
        print(f"{'-'*4}-+-{'-'*32}-+-{'-'*15}")

    for unit, int_value in EngineeringUnits.enumerations.items():
        if int_value not in BACNET_TO_SI_UNITS:
            if as_code:
                print(f"{int_value}: \"{unit}\",")
            else:
                print(f"{int_value: <4} | {unit: <32} | {'-': <15}")
        elif all:
            if as_code:
                print(f"{int_value}: \"{unit}\",  # {BACNET_TO_SI_UNITS[int_value]}")
            else:
                print(f"{int_value: <4} | {unit: <32} | {BACNET_TO_SI_UNITS[int_value]: <15}")



if __name__ == "__main__":
    main()