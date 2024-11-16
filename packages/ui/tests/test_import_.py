def test_import():
    """Test import."""
    import ui  # noqa: F401


def test_say_hellos():
    """Test using function."""
    import ui

    ui.say_hello()
