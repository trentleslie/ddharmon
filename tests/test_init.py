"""Basic tests to verify project setup."""


def test_import_ddharmon():
    """Verify ddharmon package can be imported."""
    import ddharmon

    assert ddharmon.__version__ == "0.1.0"


def test_version_string():
    """Verify version string format."""
    import ddharmon

    parts = ddharmon.__version__.split(".")
    assert len(parts) == 3
    assert all(part.isdigit() for part in parts)
