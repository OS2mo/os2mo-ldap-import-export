# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json  # pragma: no cover
from typing import Tuple  # pragma: no cover
from uuid import UUID  # pragma: no cover

import click  # pragma: no cover

from fastramqpi.ra_utils.async_to_sync import async_to_sync  # pragma: no cover

from . import AsyncDARClient  # pragma: no cover


@click.command()
@click.option(
    "uuids",
    "--uuid",
    type=click.UUID,
    multiple=True,
    required=True,
    help="DAR UUIDs to lookup",
)
@async_to_sync
async def cli(uuids: Tuple[UUID]) -> None:  # pragma: no cover
    darclient = AsyncDARClient()
    async with darclient:
        if not await darclient.healthcheck():
            raise click.ClickException("Unable to establish connection to DAR")

        results, missing = await darclient.fetch(set(uuids))
        print("Found:", json.dumps(list(results.values()), indent=4))
        print("Missing:", json.dumps(list(map(str, missing)), indent=4))


if __name__ == "__main__":  # pragma: no cover
    cli()
