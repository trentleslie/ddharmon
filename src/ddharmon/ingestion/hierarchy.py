"""Hierarchy detection for data dictionary fields via description prefix analysis.

Detects parent/child relationships by analyzing shared description prefixes
split on a delimiter (default backslash). Creates synthetic parent Fields when
no explicit parent row exists.
"""

from __future__ import annotations

import hashlib
import logging
from collections import defaultdict

from ddharmon.models.data_dictionary import Field

logger = logging.getLogger(__name__)


def detect_hierarchy(
    fields: list[Field],
    delimiter: str = "\\",
) -> list[Field]:
    """Detect parent/child hierarchy from description prefix patterns.

    Strategy:
    1. Split descriptions on delimiter, collect prefix -> field list.
    2. For groups with 2+ children sharing a prefix, create a synthetic parent
       if no parent row already exists.
    3. Link children to parent via parent_field_id / children list.
    4. Handle multi-level nesting by processing deepest levels first.

    Args:
        fields: List of Field objects to analyze.
        delimiter: Character(s) used to separate hierarchy levels in descriptions.

    Returns:
        Updated list of fields including any synthetic parents created.
    """
    if not fields:
        return fields

    # Build a lookup by variable_name for existing fields
    existing_names: set[str] = {f.variable_name for f in fields}

    # Collect all fields into a dict for mutation
    field_map: dict[str, Field] = {f.variable_name: f for f in fields}

    # Step 1: Find prefix groups from descriptions containing the delimiter
    prefix_groups: dict[str, list[str]] = defaultdict(list)  # prefix -> list of variable_names
    for f in fields:
        if delimiter in f.description:
            parts = f.description.split(delimiter, 1)
            prefix = parts[0].strip()
            if prefix:
                prefix_groups[prefix].append(f.variable_name)

    # Step 2: Process groups with 2+ members
    synthetic_parents: list[Field] = []
    groups_found = 0
    max_depth = 1

    for prefix, member_names in prefix_groups.items():
        if len(member_names) < 2:
            continue

        groups_found += 1

        # Check if an existing field already serves as parent (description matches prefix exactly)
        existing_parent = None
        for f in field_map.values():
            if f.description.strip() == prefix and not f._synthetic:
                existing_parent = f
                break

        # Create synthetic parent if needed
        if existing_parent is None:
            parent_var_name = _generate_synthetic_name(prefix, existing_names)
            existing_names.add(parent_var_name)

            # Inherit category from first child
            first_child = field_map[member_names[0]]
            parent_field = Field(
                variable_name=parent_var_name,
                description=prefix,
                category=first_child.category,
                data_type=first_child.data_type,
                _synthetic=True,
            )
            field_map[parent_var_name] = parent_field
            synthetic_parents.append(parent_field)
            parent_id = parent_var_name
        else:
            parent_id = existing_parent.variable_name

        # Link children to parent
        parent = field_map[parent_id]
        for child_name in member_names:
            child = field_map[child_name]

            # Preserve original description in metadata
            child.metadata["original_description"] = child.description

            # Update description to suffix only
            parts = child.description.split(delimiter, 1)
            suffix = parts[1].strip() if len(parts) > 1 else child.description
            child.description = suffix

            # Set parent/child links
            child.parent_field_id = parent_id
            if child_name not in parent.children:
                parent.children.append(child_name)

    logger.info(
        "Hierarchy detection: %d groups found, %d synthetic parents created",
        groups_found,
        len(synthetic_parents),
    )

    # Step 3: Handle multi-level nesting by recursing on children whose
    # descriptions still contain the delimiter
    children_with_delimiter = [f for f in field_map.values() if delimiter in f.description and not f._synthetic]
    if children_with_delimiter:
        # Check if any groups of 2+ exist among these children sharing a prefix
        sub_prefix_groups: dict[str, list[str]] = defaultdict(list)
        for f in children_with_delimiter:
            parts = f.description.split(delimiter, 1)
            sub_prefix = parts[0].strip()
            if sub_prefix:
                sub_prefix_groups[sub_prefix].append(f.variable_name)

        has_sub_groups = any(len(members) >= 2 for members in sub_prefix_groups.values())
        if has_sub_groups:
            max_depth += 1
            # Recurse: gather all current fields, re-run hierarchy on the subset
            all_current = list(field_map.values())
            sub_result = _detect_hierarchy_recursive(all_current, delimiter, existing_names, depth=2, max_recursion=5)
            # Merge results back
            field_map = {f.variable_name: f for f in sub_result}

    if max_depth > 1:
        logger.info("Multi-level hierarchy detected, max depth: %d", max_depth)

    return list(field_map.values())


def _detect_hierarchy_recursive(
    fields: list[Field],
    delimiter: str,
    existing_names: set[str],
    depth: int,
    max_recursion: int,
) -> list[Field]:
    """Recursively process multi-level hierarchy.

    Only processes fields that still contain the delimiter in their description
    and have not yet been assigned as synthetic parents.
    """
    if depth > max_recursion:
        return fields

    field_map: dict[str, Field] = {f.variable_name: f for f in fields}

    # Find fields with delimiter that are NOT synthetic (leaf-level candidates)
    candidates = [f for f in fields if delimiter in f.description and not f._synthetic]
    if not candidates:
        return fields

    # Group by prefix
    prefix_groups: dict[str, list[str]] = defaultdict(list)
    for f in candidates:
        parts = f.description.split(delimiter, 1)
        prefix = parts[0].strip()
        if prefix:
            prefix_groups[prefix].append(f.variable_name)

    made_changes = False
    for prefix, member_names in prefix_groups.items():
        if len(member_names) < 2:
            continue

        made_changes = True

        # Find or create parent
        # Look for a field whose description is exactly this prefix and is parent of these children
        existing_parent = None
        for f in field_map.values():
            if f.description.strip() == prefix:
                existing_parent = f
                break

        if existing_parent is None:
            parent_var_name = _generate_synthetic_name(prefix, existing_names)
            existing_names.add(parent_var_name)

            first_child = field_map[member_names[0]]
            # The parent for a sub-group should inherit the parent_field_id of its children
            sub_parent_id = first_child.parent_field_id

            parent_field = Field(
                variable_name=parent_var_name,
                description=prefix,
                category=first_child.category,
                data_type=first_child.data_type,
                parent_field_id=sub_parent_id,
                _synthetic=True,
            )
            field_map[parent_var_name] = parent_field

            # Update the grandparent's children list
            if sub_parent_id and sub_parent_id in field_map:
                grandparent = field_map[sub_parent_id]
                if parent_var_name not in grandparent.children:
                    grandparent.children.append(parent_var_name)
                # Remove direct child links from grandparent to these children
                for child_name in member_names:
                    if child_name in grandparent.children:
                        grandparent.children.remove(child_name)

            parent_id = parent_var_name
        else:
            parent_id = existing_parent.variable_name

        parent = field_map[parent_id]
        for child_name in member_names:
            child = field_map[child_name]

            # Store the current description as original if not already saved
            if "original_description" not in child.metadata:
                child.metadata["original_description"] = child.description

            # Update description to suffix
            parts = child.description.split(delimiter, 1)
            suffix = parts[1].strip() if len(parts) > 1 else child.description
            child.description = suffix

            child.parent_field_id = parent_id
            if child_name not in parent.children:
                parent.children.append(child_name)

    if made_changes:
        # Check if there are still more levels to process
        all_current = list(field_map.values())
        return _detect_hierarchy_recursive(all_current, delimiter, existing_names, depth + 1, max_recursion)

    return list(field_map.values())


def _generate_synthetic_name(prefix: str, existing_names: set[str]) -> str:
    """Generate a deterministic, collision-free synthetic variable name from a prefix.

    Format: _SYNTHETIC_{sha256_hash[:8]}
    If collision occurs, extends hash length until unique.

    Args:
        prefix: The description prefix text.
        existing_names: Set of already-used variable names.

    Returns:
        A unique synthetic variable name.
    """
    full_hash = hashlib.sha256(prefix.encode()).hexdigest()
    hash_len = 8
    while hash_len <= len(full_hash):
        name = f"_SYNTHETIC_{full_hash[:hash_len]}"
        if name not in existing_names:
            return name
        hash_len += 4  # Extend by 4 chars at a time
    # Extremely unlikely fallback
    return f"_SYNTHETIC_{full_hash}"
