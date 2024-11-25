from electoralyze.common.functools import classproperty


def test_classproperty():
    """Test the classproperty decorator works as intended."""

    class TestClass:
        @classproperty
        def id(cls) -> str:
            return "test"

        @classproperty
        def name(cls) -> str:
            name = f"{cls.id}_name"
            return name

    assert TestClass.id == "test"
    assert TestClass.name == "test_name"
