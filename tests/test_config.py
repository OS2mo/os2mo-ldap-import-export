# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
import json
from functools import partial
from typing import cast

import pytest
from mergedeep import Strategy  # type: ignore
from mergedeep import merge  # type: ignore
from pydantic import ValidationError
from pydantic import parse_obj_as
from pydantic.env_settings import SettingsError

from mo_ldap_import_export.config import ConversionMapping
from mo_ldap_import_export.config import Settings

overlay = partial(merge, strategy=Strategy.TYPESAFE_ADDITIVE)


@pytest.fixture
def address_mapping(minimal_mapping: dict) -> dict:
    new_mapping = overlay(
        minimal_mapping,
        {
            "ldap_to_mo": {
                "EmailEmployee": {
                    "objectClass": "ramodels.mo.details.address.Address",
                    "_import_to_mo_": "true",
                    "value": "{{ldap.mail or NONE}}",
                    "address_type": "{{ dict(uuid=get_employee_address_type_uuid('EmailEmployee')) }}",
                    "person": "{{ dict(uuid=employee_uuid or NONE) }}",
                }
            },
            "mo_to_ldap": {
                "EmailEmployee": {
                    "objectClass": "user",
                    "_export_to_ldap_": "true",
                    "mail": "{{mo_employee_address.value}}",
                    "employeeID": "{{mo_employee.cpr_no}}",
                }
            },
        },
    )
    return cast(dict, new_mapping)


def test_minimal_config(minimal_mapping: dict) -> None:
    """Happy path test for the minimal acceptable mapping."""
    conversion_mapping = parse_obj_as(ConversionMapping, minimal_mapping)
    assert conversion_mapping.dict(exclude_unset=True, by_alias=True) == minimal_mapping


def test_address_config(address_mapping: dict) -> None:
    """Happy path test for the address  mapping."""
    conversion_mapping = parse_obj_as(ConversionMapping, address_mapping)
    assert conversion_mapping.dict(exclude_unset=True, by_alias=True) == address_mapping


def test_cannot_terminate_employee(minimal_mapping: dict) -> None:
    """Test that employees cannot be terminated."""
    invalid_mapping = overlay(
        minimal_mapping,
        {
            "ldap_to_mo": {"Employee": {"_terminate_": "whatever"}},
        },
    )
    with pytest.raises(ValidationError) as exc_info:
        parse_obj_as(ConversionMapping, invalid_mapping)
    assert (
        "Termination not supported for <class 'ramodels.mo.employee.Employee'>"
        in str(exc_info.value)
    )


def test_can_terminate_address(address_mapping: dict) -> None:
    """Test that addresses can be terminated."""
    new_mapping = overlay(
        address_mapping,
        {
            "ldap_to_mo": {"EmailEmployee": {"_terminate_": "whatever"}},
        },
    )
    parse_obj_as(ConversionMapping, new_mapping)


@pytest.mark.usefixtures("minimal_valid_environmental_variables")
def test_discriminator_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings()
    assert settings.discriminator_field is None
    assert settings.discriminator_function is None
    assert settings.discriminator_values == []

    exc_info: pytest.ExceptionInfo

    with monkeypatch.context() as mpc:
        mpc.setenv("DISCRIMINATOR_FIELD", "xBrugertype")
        with pytest.raises(ValidationError) as exc_info:
            Settings()
        assert "DISCRIMINATOR_FUNCTION must be set" in str(exc_info.value)

    with monkeypatch.context() as mpc:
        mpc.setenv("DISCRIMINATOR_FIELD", "xBrugertype")
        mpc.setenv("DISCRIMINATOR_FUNCTION", "include")
        with pytest.raises(ValidationError) as exc_info:
            Settings()
        assert "DISCRIMINATOR_VALUES must be set" in str(exc_info.value)

    with monkeypatch.context() as mpc:
        mpc.setenv("DISCRIMINATOR_FIELD", "xBrugertype")
        mpc.setenv("DISCRIMINATOR_FUNCTION", "__invalid__")
        with pytest.raises(ValidationError) as exc_info:
            Settings()
        assert "unexpected value; permitted: 'exclude'" in str(exc_info.value)

    with monkeypatch.context() as mpc:
        mpc.setenv("DISCRIMINATOR_FIELD", "xBrugertype")
        mpc.setenv("DISCRIMINATOR_FUNCTION", "include")
        mpc.setenv("DISCRIMINATOR_VALUES", "[]")
        with pytest.raises(ValidationError) as exc_info:
            Settings()
        assert "DISCRIMINATOR_VALUES must be set" in str(exc_info.value)

    with monkeypatch.context() as mpc:
        mpc.setenv("DISCRIMINATOR_FIELD", "xBrugertype")
        mpc.setenv("DISCRIMINATOR_FUNCTION", "include")
        mpc.setenv("DISCRIMINATOR_VALUES", "__invalid__")
        with pytest.raises(SettingsError) as exc_info:
            Settings()
        assert 'error parsing env var "discriminator_values"' in str(exc_info.value)

    with monkeypatch.context() as mpc:
        mpc.setenv("DISCRIMINATOR_FIELD", "xBrugertype")
        mpc.setenv("DISCRIMINATOR_FUNCTION", "include")
        mpc.setenv("DISCRIMINATOR_VALUES", '["hello"]')
        settings = Settings()
        assert settings.discriminator_field == "xBrugertype"
        assert settings.discriminator_function == "include"
        assert settings.discriminator_values == ["hello"]


@pytest.mark.usefixtures("minimal_valid_environmental_variables")
def test_dialect_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    settings = Settings()
    assert settings.ldap_dialect == "AD"
    assert settings.ldap_unique_id_field == "objectGUID"

    exc_info: pytest.ExceptionInfo

    with monkeypatch.context() as mpc:
        mpc.setenv("LDAP_DIALECT", "UNKNOWN")
        with pytest.raises(ValidationError) as exc_info:
            Settings()
        assert "unexpected value; permitted: 'Standard', 'AD'" in str(exc_info.value)

    with monkeypatch.context() as mpc:
        mpc.setenv("LDAP_DIALECT", "Standard")
        settings = Settings()
        assert settings.ldap_dialect == "Standard"
        assert settings.ldap_unique_id_field == "entryUUID"

    with monkeypatch.context() as mpc:
        mpc.setenv("LDAP_DIALECT", "AD")
        settings = Settings()
        assert settings.ldap_dialect == "AD"
        assert settings.ldap_unique_id_field == "objectGUID"

    with monkeypatch.context() as mpc:
        mpc.setenv("LDAP_DIALECT", "Standard")
        mpc.setenv("ldap_unique_id_field", "myCustomField")
        settings = Settings()
        assert settings.ldap_dialect == "Standard"
        assert settings.ldap_unique_id_field == "myCustomField"

    with monkeypatch.context() as mpc:
        mpc.setenv("LDAP_DIALECT", "AD")
        mpc.setenv("ldap_unique_id_field", "myCustomField")
        settings = Settings()
        assert settings.ldap_dialect == "AD"
        assert settings.ldap_unique_id_field == "myCustomField"


@pytest.mark.usefixtures("minimal_valid_environmental_variables")
def test_mapper_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that mapper can be set as read as expected."""

    settings = Settings()
    assert settings.conversion_mapping.ldap_to_mo.keys() == {"Employee"}
    employee = settings.conversion_mapping.ldap_to_mo["Employee"]
    assert employee.mapper is None

    mapping_template = "{{ value['user_key'] }}"
    new_mapping = overlay(
        settings.conversion_mapping.dict(exclude_unset=True, by_alias=True),
        {"ldap_to_mo": {"Employee": {"_mapper_": mapping_template}}},
    )
    monkeypatch.setenv("CONVERSION_MAPPING", json.dumps(new_mapping))
    settings = Settings()
    assert settings.conversion_mapping.ldap_to_mo.keys() == {"Employee"}
    employee = settings.conversion_mapping.ldap_to_mo["Employee"]
    assert employee.mapper == mapping_template
