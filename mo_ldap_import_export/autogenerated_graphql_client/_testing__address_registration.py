from datetime import datetime

from .base_model import BaseModel


class TestingAddressRegistration(BaseModel):
    addresses: "TestingAddressRegistrationAddresses"


class TestingAddressRegistrationAddresses(BaseModel):
    objects: list["TestingAddressRegistrationAddressesObjects"]


class TestingAddressRegistrationAddressesObjects(BaseModel):
    registrations: list["TestingAddressRegistrationAddressesObjectsRegistrations"]


class TestingAddressRegistrationAddressesObjectsRegistrations(BaseModel):
    start: datetime
    end: datetime | None


TestingAddressRegistration.update_forward_refs()
TestingAddressRegistrationAddresses.update_forward_refs()
TestingAddressRegistrationAddressesObjects.update_forward_refs()
TestingAddressRegistrationAddressesObjectsRegistrations.update_forward_refs()
