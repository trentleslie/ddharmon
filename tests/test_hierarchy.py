"""Tests for hierarchy detection from description prefix patterns.

Tests backslash-delimited parent/child hierarchy detection, synthetic parent
creation, multi-level nesting, and edge cases.
"""

from __future__ import annotations

from ddharmon.ingestion.hierarchy import detect_hierarchy
from ddharmon.models import Field


def _make_field(name: str, desc: str, **kwargs: object) -> Field:
    """Helper to create a Field with minimal required attributes."""
    return Field(variable_name=name, description=desc, **kwargs)  # type: ignore[arg-type]


class TestDetectHierarchy:
    """Tests for detect_hierarchy() function."""

    def test_simple_backslash_hierarchy(self) -> None:
        """Three fields sharing a backslash prefix create 1 synthetic parent + 3 children."""
        fields = [
            _make_field("f1", "Pain \\ Head"),
            _make_field("f2", "Pain \\ Neck"),
            _make_field("f3", "Pain \\ Back"),
        ]

        result = detect_hierarchy(fields)

        # Should have 4 fields: 3 children + 1 synthetic parent
        assert len(result) == 4

        # Find the synthetic parent
        synthetics = [f for f in result if f._synthetic]
        assert len(synthetics) == 1
        parent = synthetics[0]
        assert parent.description == "Pain"
        assert parent.variable_name.startswith("_SYNTHETIC_")
        assert len(parent.children) == 3

        # Check children link to parent
        children = [f for f in result if not f._synthetic]
        for child in children:
            assert child.parent_field_id == parent.variable_name

    def test_no_hierarchy_unchanged(self) -> None:
        """Fields without backslash delimiter are unchanged."""
        fields = [
            _make_field("f1", "Age of participant"),
            _make_field("f2", "Sex of participant"),
            _make_field("f3", "Body mass index"),
        ]

        result = detect_hierarchy(fields)

        assert len(result) == 3
        for f in result:
            assert f.parent_field_id is None
            assert f._synthetic is False

    def test_multi_level_hierarchy(self) -> None:
        """A\\B\\C creates nested hierarchy with grandparent, parent, and children."""
        fields = [
            _make_field("f1", "Pain \\ Head \\ Left"),
            _make_field("f2", "Pain \\ Head \\ Right"),
            _make_field("f3", "Pain \\ Neck \\ Left"),
            _make_field("f4", "Pain \\ Neck \\ Right"),
        ]

        result = detect_hierarchy(fields)

        # Should have synthetic parents for "Pain", "Pain \\ Head", "Pain \\ Neck"
        synthetics = [f for f in result if f._synthetic]
        assert len(synthetics) >= 2  # At least "Pain \\ Head" and "Pain \\ Neck" parents

        # The deepest children should have parent_field_id set
        f1 = next(f for f in result if f.variable_name == "f1")
        assert f1.parent_field_id is not None

    def test_single_member_not_grouped(self) -> None:
        """A group with only 1 member is NOT turned into parent/child."""
        fields = [
            _make_field("f1", "Pain \\ Head"),
            _make_field("f2", "No backslash here"),
            _make_field("f3", "Another field"),
        ]

        result = detect_hierarchy(fields)

        # Only 1 field has backslash, group size < 2, so no synthetic parent
        synthetics = [f for f in result if f._synthetic]
        assert len(synthetics) == 0
        assert len(result) == 3

    def test_synthetic_name_deterministic(self) -> None:
        """Synthetic variable names are deterministic across runs."""
        # Create fresh Field objects for each run (detect_hierarchy mutates descriptions)
        fields1 = [
            _make_field("f1", "Pain \\ Head"),
            _make_field("f2", "Pain \\ Neck"),
        ]
        fields2 = [
            _make_field("f1", "Pain \\ Head"),
            _make_field("f2", "Pain \\ Neck"),
        ]

        result1 = detect_hierarchy(fields1)
        result2 = detect_hierarchy(fields2)

        synthetics1 = [f for f in result1 if f._synthetic]
        synthetics2 = [f for f in result2 if f._synthetic]

        assert len(synthetics1) == 1
        assert len(synthetics2) == 1
        assert synthetics1[0].variable_name == synthetics2[0].variable_name

    def test_no_collision_with_real_names(self) -> None:
        """Synthetic names do not collide with existing real variable names."""
        # Create a scenario where we force a potential collision
        fields = [
            _make_field("f1", "Pain \\ Head"),
            _make_field("f2", "Pain \\ Neck"),
        ]

        result = detect_hierarchy(fields)

        all_names = [f.variable_name for f in result]
        # All names should be unique
        assert len(all_names) == len(set(all_names))

    def test_child_description_is_suffix(self) -> None:
        """After hierarchy detection, child description is the suffix after delimiter."""
        fields = [
            _make_field("f1", "Pain \\ Head"),
            _make_field("f2", "Pain \\ Neck"),
        ]

        result = detect_hierarchy(fields)

        children = [f for f in result if not f._synthetic]
        child_descs = {f.variable_name: f.description for f in children}
        assert child_descs["f1"] == "Head"
        assert child_descs["f2"] == "Neck"

    def test_original_description_preserved_in_metadata(self) -> None:
        """Original full description is preserved in metadata['original_description']."""
        fields = [
            _make_field("f1", "Pain \\ Head"),
            _make_field("f2", "Pain \\ Neck"),
        ]

        result = detect_hierarchy(fields)

        children = [f for f in result if not f._synthetic]
        for child in children:
            assert "original_description" in child.metadata

        f1 = next(f for f in children if f.variable_name == "f1")
        assert f1.metadata["original_description"] == "Pain \\ Head"

    def test_category_inherited_from_first_child(self) -> None:
        """Synthetic parent inherits category from its first child."""
        fields = [
            _make_field("f1", "Pain \\ Head", category="Measurements"),
            _make_field("f2", "Pain \\ Neck", category="Measurements"),
        ]

        result = detect_hierarchy(fields)

        parent = next(f for f in result if f._synthetic)
        assert parent.category == "Measurements"

    def test_mixed_hierarchical_and_flat(self) -> None:
        """Mix of hierarchical and flat fields processes correctly."""
        fields = [
            _make_field("f1", "Pain \\ Head"),
            _make_field("f2", "Pain \\ Neck"),
            _make_field("f3", "Age of participant"),
            _make_field("f4", "BMI measurement"),
        ]

        result = detect_hierarchy(fields)

        # 4 original + 1 synthetic parent = 5
        assert len(result) == 5
        synthetics = [f for f in result if f._synthetic]
        assert len(synthetics) == 1

        # Flat fields unchanged
        f3 = next(f for f in result if f.variable_name == "f3")
        assert f3.parent_field_id is None
        assert f3.description == "Age of participant"
