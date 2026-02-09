# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import json
from functools import lru_cache
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict
from typing import cast

# This path is now used on all (non-K8S) servers
_JSON_SETTINGS_PATH = "/opt/dipex/os2mo-data-import-and-export/settings/settings.json"


@lru_cache(maxsize=None)
def load_settings() -> Dict[str, Any]:
    """Load settings file from `settings/settings.json`.

    *Note: This function is in-memory cached using `lru_cache`, such that the
           underlying file is only read and parsed once, thus if the settings file is
           written to or updated after a program has called this function once, it
           will not return the new values.*

    If a refresh or reload is needed, the cache must first be invalidated:
    ```Python
    load_settings.clear_cache()
    ```

    Returns:
        The parsed `settings.json` file as a dictionary.
    """
    cwd = Path().cwd().absolute()
    settings_path = cwd / "settings" / "settings.json"

    if not Path(settings_path).exists():
        settings_path = Path(_JSON_SETTINGS_PATH)

    with open(str(settings_path), "r") as settings_file:
        return cast(Dict[str, Any], json.load(settings_file))


class Sentinel:
    def __str__(self) -> str:
        return "sentinel"

    def __repr__(self) -> str:
        return "sentinel"


read_setting_sentinel = Sentinel()


def load_setting(
    setting: str, default: Any = read_setting_sentinel
) -> Callable[[], Any]:
    """Load a single setting, defaulting to 'default' if not present

    This function is mainly for use as defaults in `click` or similar, hence why it is
    lazily evaluated.

    Example:
        With `settings.json` containing:
        ```Json
        {"a": 1}
        ```
        ```Python
        lazy_func = load_setting("a")
        assert lazy_func() == 1
        ```

    Example:
        ```Python
        import click
        from fastramqpi.ra_utils.load_settings import load_setting

        @click.command()
        @click.option(
            "--mora-base",
            default=load_setting("mora.base", "http://localhost:5000"),
            help="URL for OS2mo.",
        )
        def main(mora_base: str):
            ...
        ```

    Raises:
        ValueError: If the `setting` is not found and no `default` is set.

    Args:
        setting: The setting to read from `load_settings`.
        default: The default value to return, if `setting` is not found.

    Returns:
        A function evaluating to the value of the setting or the configured default.
    """

    def inner() -> Any:
        value = load_settings().get(setting, default)
        if value == read_setting_sentinel:
            raise ValueError(f"{setting} not in settings file and no default")
        return value

    return inner
