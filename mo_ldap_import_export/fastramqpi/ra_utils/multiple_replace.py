# SPDX-FileCopyrightText: Magenta ApS <https://magenta.dk>
# SPDX-License-Identifier: MPL-2.0
import re
from typing import Dict
from typing import Iterator
from typing import Pattern
from typing import cast


def multiple_replace_compile(replacement_dict: Dict[str, str]) -> Pattern:
    """Make a regex pattern for finding all keys in replacement dict.

    Calling this directly with `multiple_replace_run` allows one to generate the regex
    only once, but using it multiple times, which is advantageous for performance.

    Args:
        replacement_dict: Dictionary of replacements. Keys are made into a regex.

    Returns:
        Regex matching all keys in replacement dict.
    """
    # Replacing empty string is a mess
    keys = replacement_dict.keys()
    if "" in keys:
        raise ValueError("Cannot replace empty string")

    # Make a regex pattern, which matches all (escaped) keys
    escaped_keys = map(re.escape, keys)  # type: ignore
    pattern = re.compile("|".join(cast(Iterator[str], escaped_keys)))

    return pattern


def multiple_replace_run(
    pattern: Pattern, replacement_dict: Dict[str, str], string: str
) -> str:
    """Run a a regex pattern to replace matches.

    Calling this directly with a regex from `multiple_replace_compile` allows one to
    only generate the regex once, but using it multiple times, which is advantageous
    for performance.

    Args:
        pattern: A regex pattern produced by `multiple_replace_compile`, using the
            same `replacment_dict` provided here.
        replacement_dict: Dictionary of replacements.
            Keys are replaced with their corresponding values.
        string: The string to make replacements in.

    Returns:
        Modified string, with all replacements made.
    """
    # For each match, replace found key with corresponding value
    return cast(
        str, pattern.sub(lambda x: replacement_dict.get(x.group(0), ""), string)
    )


def multiple_replace(replacement_dict: Dict[str, str], string: str) -> str:
    """Make multiple replacements in string.

    *Note: Glues together `multiple_replace_compile` and `multiple_replace_run`.*

    Example:
        ```Python
        result = multiple_replace({"like": "love", "tea": "coffee"}, "I like tea")
        assert result == "I love coffee"

        result = multiple_replace(
            {"I": "love", "love": "eating", "eating": "spam"},
            "I love eating"
        )
        assert result == "love eating spam"
        assert result != "spam spam spam"
        ```

    Args:
        replacement_dict: Dictionary of replacements.
            Keys are replaced with their corresponding values.
        string: The string to make replacements in.

    Returns:
        Modified string, with all replacements made.
    """
    pattern = multiple_replace_compile(replacement_dict)
    return multiple_replace_run(pattern, replacement_dict, string)
