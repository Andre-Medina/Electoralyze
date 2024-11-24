class classproperty:
    """Class property method decorator."""

    def __init__(self, function):
        """Decorates a method."""
        self.function = function

    def __get__(self, _object, owner):
        """Calls the function."""
        return self.function(owner)
