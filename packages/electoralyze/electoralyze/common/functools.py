class classproperty:
    """Class property method decorator.

    Example
    -------
    >>> class RegionABC:
    >>>     @classproperty
    >>>     def name(cls) -> str:
    >>>         name = f"{cls.id}_name"
    >>>         return name
    >>> RegionABC.name
    'SA2_2021'
    """

    def __init__(self, function):
        """Decorates a method."""
        self.function = function

    def __get__(self, _object, owner):
        """Calls the function."""
        return self.function(owner)
