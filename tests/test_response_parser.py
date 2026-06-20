"""Tests for value_encoding_raw -> ResponseOption parsing."""

from ddharmon.values.response_parser import parse_value_encoding


class TestParenthesizedFormat:
    """Parenthesized format: (code) label|(code) label"""

    def test_ordinal_frequency(self):
        raw = "(1) Less than once per month|(2) 1-3 times per month|(3) Once per week|(4) 2-4 times per week"
        opts = parse_value_encoding(raw)
        assert len(opts) == 4
        assert opts[0].code == "1"
        assert opts[0].label == "Less than once per month"
        assert opts[0].order == 0
        assert opts[3].code == "4"
        assert opts[3].label == "2-4 times per week"

    def test_binary_yes_no(self):
        raw = "(0) No|(1) Yes"
        opts = parse_value_encoding(raw)
        assert len(opts) == 2
        assert opts[0].code == "0"
        assert opts[0].label == "No"
        assert opts[1].code == "1"
        assert opts[1].label == "Yes"


class TestCodeEqualsLabelFormat:
    """Simple format: 1=Yes|2=No"""

    def test_binary(self):
        raw = "1=Yes|2=No"
        opts = parse_value_encoding(raw)
        assert len(opts) == 2
        assert opts[0].code == "1"
        assert opts[0].label == "Yes"

    def test_three_options(self):
        raw = "1=Male|2=Female|3=Other"
        opts = parse_value_encoding(raw)
        assert len(opts) == 3
        assert opts[2].label == "Other"


class TestCodeCommaLabelFormat:
    """REDCap-style format: Code, Label | Code, Label"""

    def test_simple(self):
        raw = "region_usa, USA | region_other, Other"
        opts = parse_value_encoding(raw)
        assert len(opts) == 2
        assert opts[0].code == "region_usa"
        assert opts[0].label == "USA"
        assert opts[1].code == "region_other"
        assert opts[1].label == "Other"

    def test_with_parenthetical_in_label(self):
        raw = (
            "race_aian, American Indian or Alaska Native "
            "(For example: Aztec, Navajo) | "
            "race_asian, Asian (For example: Chinese, Japanese)"
        )
        opts = parse_value_encoding(raw)
        assert len(opts) == 2
        assert opts[0].code == "race_aian"
        assert "American Indian" in opts[0].label
        assert "Aztec" in opts[0].label  # parenthetical preserved

    def test_many_options(self):
        raw = "A, Option A | B, Option B | C, Option C | D, Option D"
        opts = parse_value_encoding(raw)
        assert len(opts) == 4


class TestSlashDelimited:
    """Simple slash format: Yes/No"""

    def test_yes_no(self):
        opts = parse_value_encoding("Yes/No")
        assert len(opts) == 2
        assert opts[0].label == "Yes"
        assert opts[1].label == "No"

    def test_three_options(self):
        opts = parse_value_encoding("Male/Female/Other")
        assert len(opts) == 3

    def test_too_many_rejected(self):
        opts = parse_value_encoding("A/B/C/D")
        assert len(opts) == 0  # >3 options rejected for slash format


class TestEdgeCases:
    def test_empty_string(self):
        assert parse_value_encoding("") == []

    def test_whitespace_only(self):
        assert parse_value_encoding("   ") == []

    def test_single_value_no_parse(self):
        assert parse_value_encoding("Continuous") == []

    def test_coding_reference_no_parse(self):
        """UKBB 'Coding 6332' should not parse into options."""
        assert parse_value_encoding("Coding 6332") == []
