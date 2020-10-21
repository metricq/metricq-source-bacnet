from bacpypes.object import (
    AnalogInputObject,
    AnalogValueObject,
    Property,
    register_object_type,
)
from bacpypes.primitivedata import CharacterString


class CustomOptionalProperty(Property):

    """The property is optional and need not be present."""

    def __init__(
        self, identifier, datatype, default=None, optional=True, mutable=False
    ):
        Property.__init__(self, identifier, datatype, default, optional, mutable)


class ExtendedAnalogInputObject(AnalogInputObject):
    properties = [CustomOptionalProperty(3000, CharacterString)]


class ExtendedAnalogValueObject(AnalogValueObject):
    properties = [CustomOptionalProperty(3000, CharacterString)]


def register_extended_object_types():
    register_object_type(ExtendedAnalogInputObject, vendor_id=7)
    register_object_type(ExtendedAnalogValueObject, vendor_id=7)
