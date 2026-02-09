# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
from prometheus_client import Gauge

# If the integration exposes a trigger endpoint, it should make sure to call
# `dipex_last_success_timestamp.set_to_current_time()` to instrument when it last
# successfully ran.
dipex_last_success_timestamp = Gauge(
    name="dipex_last_success_timestamp",
    documentation="When the integration last successfully ran.",
    unit="seconds",
)
