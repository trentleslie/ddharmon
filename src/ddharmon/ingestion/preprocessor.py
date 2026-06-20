"""Rule-based preprocessing for data dictionary fields before embedding.

Cleans variable names and descriptions to improve embedding quality by removing
cohort-specific boilerplate, common prefixes, and redundant text. All rules are
cohort-agnostic — they discover patterns from the data rather than hardcoding
per-cohort knowledge.

Usage:
    dd = load_dictionary("data.csv", variable_name="var", description="desc")
    dd = preprocess_dictionary(dd)  # cleaned text now primary; raw preserved
    print(dd.preprocessing_report)  # summary of what changed
    preprocessing_diff(dd)          # per-field before/after for changed fields
    embedded = embed_dictionary(dd)
"""

from __future__ import annotations

import json
import logging
import re
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path

import ftfy

from ddharmon.models.data_dictionary import DataDictionary, Field

logger = logging.getLogger(__name__)

# Default config path (next to this file's package root)
_DEFAULT_STOPWORDS_PATH = Path(__file__).resolve().parent.parent.parent.parent / "config" / "stopwords.json"


@dataclass
class PreprocessingReport:
    """Summary of what preprocessing changed in a DataDictionary."""

    dictionary_name: str
    total_fields: int
    unicode_fixed: int = 0
    placeholders_replaced: int = 0
    placeholder_values: list[str] = field(default_factory=list)  # the placeholder descriptions that were replaced
    prefix_stripped: int = 0
    prefix_value: str | None = None  # the prefix that was detected and stripped
    stopwords_applied: int = 0
    name_deduped: int = 0  # fields where _embed_variable_name was set to False
    whitespace_fixed: int = 0
    names_changed: int = 0  # total fields where variable_name differs from raw
    descriptions_changed: int = 0  # total fields where description differs from raw

    def __str__(self) -> str:
        lines = [f"Preprocessing {self.dictionary_name} ({self.total_fields} fields):"]
        lines.append(f"  Unicode fixes:      {self.unicode_fixed} fields")
        if self.placeholders_replaced:
            for pv in self.placeholder_values:
                lines.append(f"  Placeholder replaced: \"{pv[:60]}\" → variable_name ({self.placeholders_replaced} fields)")
        else:
            lines.append(f"  Placeholders:       none detected")
        if self.prefix_value:
            lines.append(f"  Prefix stripped:    \"{self.prefix_value}\" from {self.prefix_stripped} fields")
        else:
            lines.append(f"  Prefix stripped:    none detected")
        lines.append(f"  Stopwords:          {self.stopwords_applied} fields")
        lines.append(f"  Name in description: {self.name_deduped} fields (embed_variable_name disabled)")
        lines.append(f"  Whitespace:         {self.whitespace_fixed} fields")
        lines.append(f"  ---")
        lines.append(f"  Names changed:      {self.names_changed} / {self.total_fields}")
        lines.append(f"  Descriptions changed: {self.descriptions_changed} / {self.total_fields}")
        return "\n".join(lines)


def preprocessing_diff(dd: DataDictionary) -> list[dict[str, str]]:
    """Return per-field before/after for all fields where preprocessing changed something.

    Each entry has: variable_name, raw_variable_name, raw_description,
    cleaned_variable_name, cleaned_description, embed_variable_name.
    Only includes fields where at least one value differs from raw.
    """
    diffs: list[dict[str, str]] = []
    for f in dd.fields.values():
        name_changed = f.raw_variable_name is not None and f.raw_variable_name != f.variable_name
        desc_changed = f.raw_description is not None and f.raw_description != f.description
        embed_suppressed = not f._embed_variable_name

        if name_changed or desc_changed or embed_suppressed:
            diffs.append({
                "variable_name": f.variable_name,
                "raw_variable_name": f.raw_variable_name or f.variable_name,
                "raw_description": (f.raw_description or f.description)[:80],
                "cleaned_description": f.description[:80],
                "name_changed": name_changed,
                "desc_changed": desc_changed,
                "embed_name_suppressed": embed_suppressed,
            })
    return diffs


def preprocess_dictionary(
    dd: DataDictionary,
    *,
    stopwords: list[str] | None = None,
    stopwords_file: Path | str | None = None,
    prefix_min_length: int = 8,
    prefix_min_ratio: float = 0.5,
    normalize_unicode: bool = True,
    strip_common_prefixes: bool = True,
    dedup_name_in_description: bool = True,
    replace_placeholder_descriptions: bool = True,
    placeholder_min_count: int = 10,
) -> DataDictionary:
    """Preprocess a DataDictionary in-place, cleaning field text for embedding.

    Saves original values to raw_variable_name / raw_description before mutating.
    Cleaned text becomes the primary value used by all downstream code.

    Steps (in order):
        1. Unicode normalization (ftfy) — fix mojibake, curly quotes, encoding artifacts
        2. Placeholder description replacement — if the same description appears in
           >= placeholder_min_count fields, it's uninformative (e.g., "Field description
           available on the registry website"). Replace with the variable name so the
           embedding captures the field identity rather than boilerplate.
        3. Common prefix stripping — detect and remove shared variable name prefixes
        4. Stopword removal — remove user-configured substrings from variable names
        5. Substring deduplication — if variable_name is contained in description, clear it
           from embedding text by setting _embed_variable_name = False
        6. Whitespace normalization — collapse runs, strip leading/trailing

    Args:
        dd: DataDictionary to preprocess (modified in place and returned).
        stopwords: Optional list of substrings to remove from variable names.
            Applied after prefix stripping. Case-insensitive matching.
        stopwords_file: Path to a JSON stopwords config file. If None, looks for
            config/stopwords.json relative to project root. Merged with stopwords arg.
        prefix_min_length: Minimum character length for a detected common prefix
            to be stripped. Shorter prefixes are kept (they're likely meaningful).
        prefix_min_ratio: Minimum fraction of fields that must share a prefix
            for it to be stripped (0.0–1.0). Higher = more conservative.
        normalize_unicode: Whether to run ftfy unicode normalization.
        strip_common_prefixes: Whether to detect and strip common prefixes.
        dedup_name_in_description: Whether to suppress variable_name in embedding
            text when it's a substring of the description.
        replace_placeholder_descriptions: Whether to detect and replace descriptions
            that appear too many times to be informative.
        placeholder_min_count: Minimum number of fields sharing the same description
            for it to be considered a placeholder (default 10).

    Returns:
        The same DataDictionary (modified in place) for chaining.
    """
    fields = list(dd.fields.values())
    if not fields:
        return dd

    # Snapshot raw values before any mutation
    for f in fields:
        f.raw_variable_name = f.variable_name
        f.raw_description = f.description

    report = PreprocessingReport(dictionary_name=dd.name, total_fields=len(fields))

    # Step 1: Unicode normalization
    if normalize_unicode:
        report.unicode_fixed = _normalize_unicode(fields)

    # Step 2: Placeholder description replacement
    if replace_placeholder_descriptions:
        report.placeholders_replaced, report.placeholder_values = _replace_placeholder_descriptions(
            fields, placeholder_min_count
        )

    # Step 3: Common prefix stripping
    if strip_common_prefixes:
        report.prefix_stripped, report.prefix_value = _strip_common_prefixes(fields, prefix_min_length, prefix_min_ratio)

    # Step 4: Stopword removal
    merged_stopwords = _load_stopwords(stopwords, stopwords_file)
    if merged_stopwords:
        report.stopwords_applied = _remove_stopwords(fields, merged_stopwords)

    # Step 5: Substring deduplication
    if dedup_name_in_description:
        report.name_deduped = _dedup_name_in_description(fields)

    # Step 6: Whitespace normalization (always runs, no flag needed)
    report.whitespace_fixed = _normalize_whitespace(fields)

    # Compute aggregate change counts
    for f in fields:
        if f.raw_variable_name is not None and f.raw_variable_name != f.variable_name:
            report.names_changed += 1
        if f.raw_description is not None and f.raw_description != f.description:
            report.descriptions_changed += 1

    # Re-key the dictionary in case variable_name changed
    dd.fields = {f.variable_name: f for f in fields}

    # Attach report to the dictionary for later inspection
    dd.preprocessing_report = report  # type: ignore[attr-defined]

    logger.info(
        "preprocess_dictionary(%s): %d fields preprocessed",
        dd.name,
        len(fields),
    )
    return dd


# ---------------------------------------------------------------------------
# Step 1: Unicode normalization
# ---------------------------------------------------------------------------


def _normalize_unicode(fields: list[Field]) -> int:
    """Fix encoding artifacts using ftfy. Returns count of fields changed."""
    count = 0
    for f in fields:
        fixed_name = ftfy.fix_text(f.variable_name)
        fixed_desc = ftfy.fix_text(f.description)
        if fixed_name != f.variable_name or fixed_desc != f.description:
            count += 1
        f.variable_name = fixed_name
        f.description = fixed_desc
        if f.question_text:
            f.question_text = ftfy.fix_text(f.question_text)
        if f.short_label:
            f.short_label = ftfy.fix_text(f.short_label)
    if count:
        logger.info("Unicode normalization: fixed %d fields", count)
    return count


# ---------------------------------------------------------------------------
# Step 2: Placeholder description replacement
# ---------------------------------------------------------------------------


def _replace_placeholder_descriptions(fields: list[Field], min_count: int) -> tuple[int, list[str]]:
    """Replace high-frequency duplicate descriptions with the variable name.

    If the same exact description appears in >= min_count fields, it's likely a
    placeholder (e.g., "Field description available on the registry website") rather
    than a real definition. Replace it with the variable name so the embedding
    captures field identity instead of boilerplate.

    Returns (count_replaced, list_of_placeholder_strings).
    """
    # Count description frequencies
    desc_counts: Counter[str] = Counter(f.description for f in fields)

    # Find descriptions that appear >= min_count times
    placeholders = {desc for desc, count in desc_counts.items() if count >= min_count}

    if not placeholders:
        return 0, []

    count = 0
    for f in fields:
        if f.description in placeholders:
            # Replace with a readable version of the variable name
            replacement = f.variable_name.replace("_", " ").replace(".", " ").replace("-", " ").strip()
            f.description = replacement
            count += 1

    placeholder_list = sorted(placeholders)
    for p in placeholder_list:
        n = desc_counts[p]
        logger.info("Placeholder description replaced: \"%s\" (%d fields)", p[:80], n)

    return count, placeholder_list


# ---------------------------------------------------------------------------
# Step 3: Common prefix stripping
# ---------------------------------------------------------------------------

# Regex for splitting on word boundaries in identifiers:
# underscores, dots, hyphens, camelCase transitions
_WORD_BOUNDARY_RE = re.compile(r"[_.\-]|(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")


def _split_identifier(name: str) -> list[str]:
    """Split an identifier into tokens at word boundaries.

    Handles snake_case, camelCase, dot.notation, and kebab-case.
    Returns lowercased tokens.

    >>> _split_identifier("assessmentHealthHistory_bmi")
    ['assessment', 'health', 'history', 'bmi']
    >>> _split_identifier("the_basics.birthplace_country")
    ['the', 'basics', 'birthplace', 'country']
    """
    # Insert boundary markers, then split
    spaced = _WORD_BOUNDARY_RE.sub(" ", name)
    return [t.lower() for t in spaced.split() if t]


def _find_common_prefix_tokens(names: list[str], min_ratio: float) -> list[str]:
    """Find the longest common token prefix shared by at least min_ratio of names.

    Works at the token level (word boundaries), not character level.
    Returns the prefix tokens, or empty list if no significant prefix found.
    """
    if not names:
        return []

    tokenized = [_split_identifier(n) for n in names]
    min_count = max(2, int(len(names) * min_ratio))

    # Find the maximum possible prefix length
    max_prefix_len = min(len(t) for t in tokenized) if tokenized else 0

    best_prefix: list[str] = []
    for depth in range(1, max_prefix_len + 1):
        # Count how many names share this prefix at this depth
        prefix_counter: Counter[tuple[str, ...]] = Counter()
        for tokens in tokenized:
            prefix_counter[tuple(tokens[:depth])] += 1

        # Find the most common prefix at this depth
        if not prefix_counter:
            break
        most_common_prefix, count = prefix_counter.most_common(1)[0]
        if count >= min_count:
            best_prefix = list(most_common_prefix)
        else:
            break

    return best_prefix


def _strip_common_prefixes(fields: list[Field], min_length: int, min_ratio: float) -> tuple[int, str | None]:
    """Detect and strip common variable name prefixes at word boundaries.

    Returns (count_stripped, prefix_string) or (0, None) if no prefix found.
    """
    names = [f.variable_name for f in fields]
    prefix_tokens = _find_common_prefix_tokens(names, min_ratio)

    if not prefix_tokens:
        return 0, None

    # Reconstruct the prefix string to check length
    prefix_str = "_".join(prefix_tokens)
    if len(prefix_str) < min_length:
        logger.debug("Common prefix '%s' too short (%d < %d), keeping", prefix_str, len(prefix_str), min_length)
        return 0, None

    logger.info(
        "Detected common prefix: '%s' (shared by %.0f%%+ of %d fields)",
        prefix_str,
        min_ratio * 100,
        len(fields),
    )

    count = 0
    prefix_depth = len(prefix_tokens)
    for f in fields:
        tokens = _split_identifier(f.variable_name)
        if len(tokens) <= prefix_depth:
            continue  # Don't strip if it would leave nothing
        if [t.lower() for t in tokens[:prefix_depth]] == prefix_tokens:
            # Reconstruct the stripped name preserving original delimiters
            stripped = _remove_token_prefix(f.variable_name, prefix_depth)
            if stripped:
                f.variable_name = stripped
                count += 1
    return count, prefix_str


def _remove_token_prefix(name: str, n_tokens: int) -> str:
    """Remove the first n_tokens from an identifier, preserving the original delimiter style.

    >>> _remove_token_prefix("assessment_health_history_bmi", 3)
    'bmi'
    >>> _remove_token_prefix("assessmentHealthHistoryBmi", 3)
    'Bmi'
    >>> _remove_token_prefix("the.basics.birthplace", 2)
    'birthplace'
    """
    # Find split positions using the boundary regex
    boundaries = list(_WORD_BOUNDARY_RE.finditer(name))

    if len(boundaries) < n_tokens:
        # Try camelCase: count actual token boundaries
        tokens = _split_identifier(name)
        if len(tokens) <= n_tokens:
            return ""
        # For camelCase without explicit delimiters, find the character position
        # of the (n_tokens)th token start
        pos = 0
        found = 0
        i = 0
        while i < len(name) and found < n_tokens:
            # Advance past current token
            if i > 0 and (name[i].isupper() or name[i] in "_.-"):
                found += 1
                if found == n_tokens:
                    # Skip delimiter if present
                    if name[i] in "_.-":
                        i += 1
                    pos = i
                    break
            i += 1
        if found == n_tokens:
            return name[pos:]
        return name

    # Use the nth boundary position
    target_boundary = boundaries[n_tokens - 1]
    end = target_boundary.end()
    # Skip trailing delimiter characters
    while end < len(name) and name[end] in "_.-":
        end += 1
    return name[end:]


# ---------------------------------------------------------------------------
# Step 4: Stopword removal
# ---------------------------------------------------------------------------


def _load_stopwords(explicit: list[str] | None, file_path: Path | str | None) -> list[str]:
    """Merge explicit stopwords with those from a config file."""
    result: list[str] = []

    # Load from file
    path = Path(file_path) if file_path else _DEFAULT_STOPWORDS_PATH
    if path.exists():
        try:
            with open(path) as f:
                data = json.load(f)
            if isinstance(data, list):
                result.extend(data)
            elif isinstance(data, dict) and "stopwords" in data:
                result.extend(data["stopwords"])
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Failed to load stopwords from %s: %s", path, e)

    # Merge explicit
    if explicit:
        result.extend(explicit)

    return result


def _remove_stopwords(fields: list[Field], stopwords: list[str]) -> int:
    """Remove stopword substrings from variable names (case-insensitive). Returns count changed."""
    if not stopwords:
        return 0

    # Build a single regex pattern for efficiency
    escaped = [re.escape(sw) for sw in stopwords]
    pattern = re.compile("|".join(escaped), re.IGNORECASE)

    count = 0
    for f in fields:
        cleaned = pattern.sub("", f.variable_name)
        # Clean up leftover consecutive delimiters
        cleaned = re.sub(r"[_.\-]{2,}", lambda m: m.group()[0], cleaned)
        cleaned = cleaned.strip("_.- ")
        if cleaned and cleaned != f.variable_name:
            f.variable_name = cleaned
            count += 1

    if count:
        logger.info("Stopword removal: cleaned %d variable names", count)
    return count


# ---------------------------------------------------------------------------
# Step 5: Substring deduplication
# ---------------------------------------------------------------------------


def _dedup_name_in_description(fields: list[Field]) -> int:
    """Suppress variable_name in embedding text when it's redundant with description.

    If the variable name (after normalizing underscores to spaces) appears as a
    substring of the description, we set _embed_variable_name = False to avoid
    double-counting in embeddings. Returns count of fields suppressed.
    """
    count = 0
    for f in fields:
        name_as_words = f.variable_name.replace("_", " ").replace(".", " ").replace("-", " ").lower().strip()
        desc_lower = f.description.lower()
        if name_as_words and name_as_words in desc_lower:
            f._embed_variable_name = False
            count += 1

    if count:
        logger.info("Substring dedup: suppressed %d variable names redundant with description", count)
    return count


# ---------------------------------------------------------------------------
# Step 6: Whitespace normalization
# ---------------------------------------------------------------------------


def _normalize_whitespace(fields: list[Field]) -> int:
    """Collapse whitespace runs and strip leading/trailing whitespace. Returns count changed."""
    count = 0
    for f in fields:
        new_name = re.sub(r"\s+", " ", f.variable_name).strip()
        new_desc = re.sub(r"\s+", " ", f.description).strip()
        if new_name != f.variable_name or new_desc != f.description:
            count += 1
        f.variable_name = new_name
        f.description = new_desc
        if f.question_text:
            f.question_text = re.sub(r"\s+", " ", f.question_text).strip()
        if f.short_label:
            f.short_label = re.sub(r"\s+", " ", f.short_label).strip()
    return count
