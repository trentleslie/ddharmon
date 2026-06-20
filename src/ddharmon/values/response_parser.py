"""Parse value_encoding_raw strings into ResponseOption lists.

Handles multiple formats found across data dictionaries:

    Parenthesized: (1) Less than once per month|(2) 1-3 times per month|...
    Code, Label:   code_1, Label one | code_2, Label two
    Simple:        Yes/No, Male/Female, 1=Yes|2=No
"""

from __future__ import annotations

import logging
import re

from ddharmon.models.data_dictionary import ResponseOption

logger = logging.getLogger(__name__)


def parse_value_encoding(raw: str) -> list[ResponseOption]:
    """Parse a value_encoding_raw string into ResponseOption objects.

    Tries formats in order of specificity:
    1. Parenthesized code: (1) Label|(2) Label
    2. Code-comma-label:   Code, Label | Code, Label  (REDCap-style)
    3. Code-equals-label:  1=Yes|2=No
    4. Slash-delimited:    Yes/No, Male/Female (2-3 options only)

    Args:
        raw: The raw value encoding string.

    Returns:
        List of ResponseOption objects. Empty list if unparseable.
    """
    raw = raw.strip()
    if not raw:
        return []

    # Try each format
    result = _parse_parenthesized(raw)
    if result:
        return result

    result = _parse_code_equals_label(raw)
    if result:
        return result

    result = _parse_code_comma_label(raw)
    if result:
        return result

    result = _parse_slash_delimited(raw)
    if result:
        return result

    return []


def _parse_parenthesized(raw: str) -> list[ResponseOption] | None:
    """Parse (code) label | (code) label format.

    Examples:
        (1) Less than once per month|(2) 1-3 times per month
        (0) No|(1) Yes

    Only matches when options START with (code), not parentheticals mid-label.
    """
    # Must start with (code) pattern or have |(code) after pipe
    if not re.match(r"\s*\(", raw):
        return None

    pattern = re.compile(r"\(([^)]+)\)\s*([^|]*)")
    matches = pattern.findall(raw)

    if len(matches) < 2:
        return None

    options = []
    for i, (code, label) in enumerate(matches):
        label = label.strip()
        if not label:
            label = code.strip()
        options.append(ResponseOption(code=code.strip(), label=label, order=i))

    return options


def _parse_code_equals_label(raw: str) -> list[ResponseOption] | None:
    """Parse code=label|code=label format.

    Examples:
        1=Yes|2=No
        1=Male|2=Female|3=Other
    """
    if "=" not in raw:
        return None

    parts = re.split(r"\s*\|\s*", raw)
    if len(parts) < 2:
        return None

    options = []
    for i, part in enumerate(parts):
        part = part.strip()
        if "=" not in part:
            return None  # Mixed format, bail
        code, label = part.split("=", 1)
        code = code.strip()
        label = label.strip()
        if not code or not label:
            continue
        options.append(ResponseOption(code=code, label=label, order=i))

    return options if len(options) >= 2 else None


def _parse_code_comma_label(raw: str) -> list[ResponseOption] | None:
    """Parse Code, Label | Code, Label format (REDCap-style).

    Examples:
        code_1, Label one | code_2, Label two
        race_aian, American Indian or Alaska Native | race_asian, Asian
    """
    if "|" not in raw:
        return None

    parts = re.split(r"\s*\|\s*", raw)
    if len(parts) < 2:
        return None

    options = []
    for i, part in enumerate(parts):
        part = part.strip()
        if not part:
            continue
        # Split on first comma only (label may contain commas in parentheticals)
        if "," not in part:
            # No comma = treat whole thing as both code and label
            options.append(ResponseOption(code=part, label=part, order=i))
            continue
        code, label = part.split(",", 1)
        code = code.strip()
        label = label.strip()
        if not code:
            continue
        if not label:
            label = code
        options.append(ResponseOption(code=code, label=label, order=i))

    return options if len(options) >= 2 else None


def _parse_slash_delimited(raw: str) -> list[ResponseOption] | None:
    """Parse simple slash-delimited options (2-3 only).

    Examples:
        Yes/No
        Male/Female
        Yes/No/Unknown
    """
    if "|" in raw or "=" in raw or "(" in raw:
        return None

    parts = raw.split("/")
    if len(parts) < 2 or len(parts) > 3:
        return None

    # Each part should be a single short word/phrase
    options = []
    for i, part in enumerate(parts):
        part = part.strip()
        if not part or len(part) > 30:
            return None  # Too long to be a simple option label
        options.append(ResponseOption(code=str(i), label=part, order=i))

    return options
