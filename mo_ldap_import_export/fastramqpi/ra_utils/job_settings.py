# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import logging
import sys
from enum import Enum
from enum import auto
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import structlog
from pydantic import BaseSettings
from pydantic import Extra
from pydantic.env_settings import SettingsSourceCallable
from structlog.processors import CallsiteParameter

from .load_settings import load_settings

logger = logging.getLogger(__name__)


def _get_json_settings_source(prefix: str) -> SettingsSourceCallable:
    """Create a Pydantic settings source which reads the DIPEX `settings.json`

    Args:
        prefix (str): Retrieve only the settings in `settings.json` whose key match this
                      prefix.
    """

    def settings_source(base_settings: BaseSettings) -> Dict[str, Any]:
        # The actual settings source callable

        try:
            all_settings = load_settings()
        except FileNotFoundError:
            # `print()` is used instead of `logger.warning()` here, as logging is
            # probably not yet configured at this point.
            print("Could not load 'settings.json', using settings from environment")
            return {}

        # Retrieve all settings matching `prefix`, and convert dots in setting keys to
        # underscores, so they become valid setting names for Pydantic settings.
        settings: Dict[str, Any] = {
            key.replace(".", "_"): val
            for key, val in all_settings.items()
            if key.startswith(prefix)
        }

        # Add log level to settings, if defined
        if "log_level" in all_settings:
            settings["log_level"] = all_settings["log_level"]

        return settings

    return settings_source


class LogLevel(Enum):
    """Represent log levels of the Python `logging` library as an `Enum`.

    This allows us to read the desired log level in `IntegrationSettings`  without
    additional parsing or conversion.
    """

    def _generate_next_value_(name, start, count, last_values):  # type: ignore
        # Generate enum elements whose key and values are identical.
        # See: https://docs.python.org/3.9/library/enum.html#using-automatic-values
        # Must be defined *before* the enum elements to work correctly.
        return name

    NOTSET = auto()
    DEBUG = auto()
    INFO = auto()
    WARNING = auto()
    ERROR = auto()
    CRITICAL = auto()


def _dont_log_graphql_responses(_: Any, __: str, event_dict: dict) -> dict:
    """Drop logs from `BaseHTTPXTransport._decode_response` (in
    `raclients.graph.transport`), which logs *all* GraphQL responses at DEBUG level.
    (https://git.magenta.dk/rammearkitektur/ra-clients/-/blob/master/raclients/graph/transport.py#L117)
    """
    module: str | None = event_dict.get("module")
    func_name: str | None = event_dict.get("func_name")
    if module == "transport" and func_name == "_decode_response":  # pragma: no cover
        raise structlog.DropEvent
    return event_dict


class JobSettings(BaseSettings):
    """Base class for defining the settings of a given OS2MO integration job.

    Each integration should define its settings like this:
    >>> class SqlExportSettings(JobSettings):
    >>>
    >>>     class Config:
    >>>         # Optional: Only use settings from settings.json if they match this
    >>>         # prefix.
    >>>         settings_json_prefix = "exporters.actual_state"

    And then read its settings like this:
    >>> settings = SqlExportSettings()

    And configure logging according to the settings like this:
    >>> settings.start_logging_based_on_settings()
    """

    mora_base = "http://mo:5000"
    client_id: str = "dipex"
    client_secret: Optional[str] = None
    auth_realm: str = "mo"
    auth_server: str = "http://keycloak:8080/auth"

    log_level: LogLevel = LogLevel.DEBUG
    log_format: str = (
        "%(levelname)s %(asctime)s %(filename)s:%(lineno)d:%(name)s: %(message)s"
    )

    sentry_dsn: Optional[str] = None

    class Config:
        # Configuration attributes defined by the Pydantic `Config` class
        extra: Extra = Extra.allow
        env_file_encoding: str = "utf-8"
        use_enum_values: bool = True

        # Additional configuration attributes defined by us.
        settings_json_prefix: str = ""

        @classmethod
        def customise_sources(
            cls,
            init_settings: SettingsSourceCallable,
            env_settings: SettingsSourceCallable,
            file_secret_settings: SettingsSourceCallable,
        ) -> Tuple[SettingsSourceCallable, ...]:
            """Add settings source which reads settings from 'settings.json'"""
            json_settings = _get_json_settings_source(cls.settings_json_prefix)
            return (
                init_settings,
                env_settings,
                json_settings,
                file_secret_settings,
            )

    def start_logging_based_on_settings(self) -> None:
        """Configure Python `logging` library as well as `structlog` logging according
        to the specified log level."""
        self._configure_python_logging()
        self._configure_structlog_logging()

    def _configure_python_logging(self) -> None:
        # Based on https://stackoverflow.com/a/14058475

        # Get root logger and set its log level
        root: logging.Logger = logging.getLogger()
        root.setLevel(self._get_log_level_numeric_value())

        # Create handler logging to stdout, and set its log level
        handler: logging.StreamHandler = logging.StreamHandler(sys.stdout)
        handler.setLevel(self._get_log_level_numeric_value())

        # Set the log format of the handler
        formatter: logging.Formatter = logging.Formatter(self.log_format)
        handler.setFormatter(formatter)

        # Add the handler logging to stdout to the root logger
        root.addHandler(handler)

    def _configure_structlog_logging(self) -> None:
        # Based on: https://www.structlog.org/en/stable/logging-best-practices.html

        shared_processors: List[Any] = [
            structlog.processors.CallsiteParameterAdder(
                [CallsiteParameter.MODULE, CallsiteParameter.FUNC_NAME],
            ),
            _dont_log_graphql_responses,
        ]
        if sys.stderr.isatty():
            # Pretty printing when we run in a terminal session.
            # Automatically prints pretty tracebacks when "rich" is installed
            processors = shared_processors + [structlog.dev.ConsoleRenderer()]
        else:  # pragma: no cover
            # Print JSON when we run, e.g., in a Docker container.
            # Also print structured tracebacks.
            processors = shared_processors + [structlog.processors.JSONRenderer()]

        structlog.configure(
            processors,
            # Only log `structlog` output at the configured log level, or higher
            wrapper_class=structlog.make_filtering_bound_logger(
                self._get_log_level_numeric_value()
            ),
        )

    def _get_log_level_numeric_value(self) -> int:
        reverse_mapping = {value: key for key, value in logging._levelToName.items()}
        return reverse_mapping[str(self.log_level)]
