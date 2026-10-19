# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID
from uuid import uuid4

from pydantic import BaseModel
from pydantic import Extra
from pydantic import Field

from .maybe import Maybe


class StrictBaseModel(BaseModel):
    """Pydantic BaseModel with strict(er) defaults."""

    class Config:
        extra = Extra.forbid
        frozen = True
        # TODO: do we want this? grandfathered-in from ramodels
        allow_population_by_field_name = True
        # Required to support UNSET
        arbitrary_types_allowed = True


class Validity(StrictBaseModel):
    start: datetime
    end: datetime | None


class OpenValidity(StrictBaseModel):
    start: datetime | None
    end: datetime | None


class Address(StrictBaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    user_key: str = None  # type: ignore[assignment]

    value: str
    value2: str | None
    address_type: UUID
    person: UUID | None
    org_unit: UUID | None
    ituser: UUID | None
    engagement: UUID | None
    visibility: UUID | None
    validity: Validity


class Employee(StrictBaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    user_key: str = None  # type: ignore[assignment]

    given_name: str | None  # TODO: don't allow none (grandfathered-in from ramodels)
    surname: str | None  # TODO: don't allow none (grandfathered-in from ramodels)
    cpr_number: str | None = Field(regex=r"^\d{10}$")
    seniority: datetime | None  # TODO: ensure this field is read from MO, #64576
    nickname_given_name: str = ""
    nickname_surname: str = ""


class OrganisationUnit(StrictBaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    user_key: str

    name: str
    parent: UUID | None = None
    unit_type: UUID
    validity: Validity


# Because we dump to json all over the place, we cannot really invent our own
# custom (proper) types.
UNSET = "LDAP-PLEASE-MAKE-NO-CHANGES (unset sentinel)"


class Engagement(StrictBaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    user_key: str

    org_unit: UUID
    person: UUID
    job_function: UUID
    engagement_type: UUID
    primary: UUID | None
    extension_1: str | None = UNSET
    extension_2: str | None = UNSET
    extension_3: str | None = UNSET
    extension_4: str | None = UNSET
    extension_5: str | None = UNSET
    extension_6: str | None = UNSET
    extension_7: str | None = UNSET
    extension_8: str | None = UNSET
    extension_9: str | None = UNSET
    extension_10: str | None = UNSET
    fraction: int | None
    validity: Validity


@dataclass(frozen=True)
class EngagementChanges:
    """Changes to write to an engagement in MO within the validity.

    Fields which are `None` are left untouched in MO, see `Maybe`.
    """

    uuid: UUID
    validity: Validity
    user_key: Maybe[str] = None
    org_unit: Maybe[UUID] = None
    person: Maybe[UUID] = None
    job_function: Maybe[UUID] = None
    engagement_type: Maybe[UUID] = None
    primary: Maybe[UUID | None] = None
    extension_1: Maybe[str | None] = None
    extension_2: Maybe[str | None] = None
    extension_3: Maybe[str | None] = None
    extension_4: Maybe[str | None] = None
    extension_5: Maybe[str | None] = None
    extension_6: Maybe[str | None] = None
    extension_7: Maybe[str | None] = None
    extension_8: Maybe[str | None] = None
    extension_9: Maybe[str | None] = None
    extension_10: Maybe[str | None] = None


class ITUser(StrictBaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    user_key: str

    external_id: str | None
    binding_type: str | None
    itsystem: UUID
    person: UUID | None
    org_unit: UUID | None
    engagements: list[UUID] = []
    validity: Validity


class ITSystem(StrictBaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    user_key: str

    name: str
    validity: Validity


class Class(StrictBaseModel):
    uuid: UUID = Field(default_factory=uuid4)
    user_key: str

    name: str
    scope: str
    facet: UUID
    it_system: UUID | None

    owner: UUID | None
    parent: UUID | None
    published: str = "Publiceret"

    validity: OpenValidity


MOBase = Address | Employee | Engagement | ITUser | OrganisationUnit | ITSystem | Class


class Termination(StrictBaseModel):
    mo_class: type[MOBase]
    uuid: UUID
    at: datetime
