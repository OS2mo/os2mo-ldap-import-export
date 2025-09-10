# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from fastapi import APIRouter
from fastramqpi.events import Listener

router = APIRouter()


@router.post("/hello-ldap")
async def hello_ldap() -> None:
    print("Hello, ldap!")


@router.post("/hello-mo")
async def hello_mo() -> None:
    print("Hello, mo!")


listeners = [
    Listener(
        namespace="ldap",
        user_key="hello-ldap",
        routing_key="uuid",
        path="/hello-ldap",
    ),
    Listener(
        namespace="mo",
        user_key="hello-mo",
        routing_key="person",
        path="/hello-mo",
    ),
]
