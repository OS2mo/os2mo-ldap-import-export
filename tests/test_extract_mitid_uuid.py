# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import binascii
import random
from typing import Any
from uuid import UUID

import pytest
from hypothesis import given
from hypothesis import strategies as st

from mo_ldap_import_export.environments.main import filter_extract_mitid_uuid
from tests.conftest import construct_mitid_nl3uuid
from tests.conftest import construct_mitid_nl3uuid_str

# These are randomly generated values
VALID_UUID = UUID("f99c7445-e95e-4824-b299-d968033021fb")
VALID_CVR = 29189846
VALID_NONCE = 12345678
VALID_SIGNATURE = "YmFzZTY0"
VALID_STATUS = "ACTIVE"


@given(st.uuids())
def test_extract_mitid_uuid_single_string(mitid_uuid: UUID) -> None:
    input_val = construct_mitid_nl3uuid(mitid_uuid)
    assert filter_extract_mitid_uuid(input_val) == mitid_uuid


@given(st.uuids())
def test_extract_mitid_uuid_success(mitid_uuid: UUID) -> None:
    input_val = [
        "SomeOtherValue",
        construct_mitid_nl3uuid(mitid_uuid),
        "AnotherValue",
    ]
    random.shuffle(input_val)
    assert filter_extract_mitid_uuid(input_val) == mitid_uuid


@given(st.uuids())
def test_extract_mitid_uuid_not_string(mitid_uuid: UUID) -> None:
    input_val = [
        123,
        None,
        construct_mitid_nl3uuid(mitid_uuid),
    ]
    random.shuffle(input_val)
    assert filter_extract_mitid_uuid(input_val) == mitid_uuid


@pytest.mark.parametrize(
    "input_val",
    [
        None,
        "",
        [],
        [""],
        ["SomeOtherValue", "AnotherValue"],
        ["NOT-NL3UUID:1.2.3.4.5"],
    ],
)
def test_extract_mitid_uuid_returns_none(input_val: Any) -> None:
    assert filter_extract_mitid_uuid(input_val) is None


@pytest.mark.parametrize(
    "input_val, expected_exception, expected_message",
    [
        (
            ["NL3UUID-ACTIVE-NSIS:1.2"],
            ValueError,
            "not enough values to unpack",
        ),
        (
            ["NL3UUID-ACTIVE-NSIS:1"],
            ValueError,
            "not enough values to unpack",
        ),
        (
            ["NL3UUIDFOO-ACTIVE-NSIS:1.2.3.4.5"],
            AssertionError,
            "Expected NL3UUID prefix, got NL3UUIDFOO",
        ),
        (
            ["NL3UUID-ACTIVE-NSISFOO:1.2.3.4.5"],
            AssertionError,
            "Expected NSIS suffix, got NSISFOO",
        ),
        (
            ["NL3UUID-BUT-BROKEN:1.2.3.4.5"],
            AssertionError,
            "Status must be ACTIVE or SUSPENDED",
        ),
        (
            ["NL3UUID-INVALID-NSIS:1.2.3.4.5"],
            AssertionError,
            "Status must be ACTIVE or SUSPENDED",
        ),
    ],
)
def test_extract_mitid_uuid_malformed_raises(
    input_val: list[str], expected_exception: type[Exception], expected_message: str
) -> None:
    with pytest.raises(expected_exception) as exc_info:
        filter_extract_mitid_uuid(input_val)
    assert expected_message in str(exc_info.value)


@pytest.mark.parametrize(
    "overrides, expected_exception, expected_message",
    [
        # Invalid Status
        (
            {"status": "INVALID"},
            AssertionError,
            "Status must be ACTIVE or SUSPENDED",
        ),
        # Invalid CVR
        (
            {"cvr": "abc"},
            AssertionError,
            "CVR must be numeric",
        ),
        # Invalid Session UUID
        (
            {"session_uuid": "not-a-uuid"},
            ValueError,
            "badly formed hexadecimal UUID string",
        ),
        # Invalid MitID UUID
        (
            {"mitid_uuid": "not-a-uuid"},
            ValueError,
            "badly formed hexadecimal UUID string",
        ),
        # Invalid Nonce
        (
            {"nonce": "abc"},
            AssertionError,
            "Nonce must be numeric",
        ),
        # Invalid Base64 Signature
        (
            {"signature": "not_base64!"},
            binascii.Error,
            "Only base64 data is allowed",
        ),
    ],
)
def test_extract_mitid_uuid_invalid_types_raises(
    overrides: dict[str, Any],
    expected_exception: type[Exception],
    expected_message: str,
) -> None:
    defaults = {
        "status": VALID_STATUS,
        "cvr": VALID_CVR,
        "session_uuid": VALID_UUID,
        "mitid_uuid": VALID_UUID,
        "nonce": VALID_NONCE,
        "signature": VALID_SIGNATURE,
    }
    input_val = construct_mitid_nl3uuid_str(**{**defaults, **overrides})
    with pytest.raises(expected_exception) as exc_info:
        filter_extract_mitid_uuid(input_val)
    assert expected_message in str(exc_info.value)


def test_extract_mitid_uuid_multiple_matches_raises() -> None:
    input_val = [
        construct_mitid_nl3uuid(VALID_UUID),
        construct_mitid_nl3uuid(VALID_UUID),
    ]
    with pytest.raises(ValueError) as exc_info:
        filter_extract_mitid_uuid(input_val)
    assert "Expected exactly one item in iterable" in str(exc_info.value)
