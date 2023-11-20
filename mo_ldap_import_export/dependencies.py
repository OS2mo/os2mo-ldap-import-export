# SPDX-FileCopyrightText: 2019-2020 Magenta ApS
# SPDX-License-Identifier: MPL-2.0
# -*- coding: utf-8 -*-
import re

from .exceptions import InvalidCPR


async def valid_cpr(cpr: str) -> str:
    cpr = cpr.replace("-", "")
    if not re.match(r"^\d{10}$", cpr):
        raise InvalidCPR(f"{cpr} is not a valid cpr-number")

    return cpr
