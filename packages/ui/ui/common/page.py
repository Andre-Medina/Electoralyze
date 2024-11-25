import dash_mantine_components as dmc

NEEDED_PAGE_ATTRIBUTES = ["path", "label"]


class Page(dmc.Stack):
    """Page base class for creating pages.

    To create a page you must:
    - Create a file for you page in `ui/pages` with the name of the page
    - Create a child class of "Page"
    - Overwrite the following methods and attributes, more info can be found in each of their doc strings.
      - `label`, Name of the page. In both buttons and in the tab header.
      - `path`, path to the page will be placed from root.
      - `icon`, Icon to place next to the navigation button.
      - `__init__`, Creates the page, must call super().__init__()
    - Refer to the child class in `ui/pages/__init__.py`
    - Add the page to the App in `ui/app.py`

    Example
    -------
    Adding a page named `archive`. First adding the class in `ui/pages/archive.py`
    ```python
        from dash import html

        from ui.common import Page, icon


        class Archive(Page):
            \"\"\"Archive page.\"\"\"

            path = "/archive"
            label = "Archive"
            icon = icon.archive

            def __init__(self):
                \"\"\"Archive page.\"\"\"
                layout = html.Div(
                    [
                        html.H1("This is our Archive page"),
                        html.Div("This is our Archive page content."),
                    ]
                )

                super().__init__(layout)
    ```

    And reference it in `ui/pages/__init__.py`
    ```python
        from .archive import Archive
        from .home import Home

        __all__ = ["Archive", "Home"]
    ```

    Add to `create_app` in `ui/app.py`

    ```python
        from ui.common import create_app
        from ui.pages import Analytics, Archive, Home, Map

        app = create_app(
            ...
            pages=[Archive, Home],
        )
    ```

    Now test the page is working by running the UI and navigating to the page.
    """

    path: str
    label: str
    icon: str | None = None
    wrap_with_container: bool = True

    def __init__(self, children, *args, **kwargs):
        """Creates the layout of the base page.

        Child classes must call this method and pass their layouts through to `children`.

        Example
        -------
        ```python
            def __init__(self):
                \"\"\"Archive page.\"\"\"
                layout = html.Div(
                    [
                        html.H1("This is our Archive page"),
                        html.Div("This is our Archive page content."),
                    ]
                )

                super().__init__(layout)
        ```
        """
        if any((getattr(self, attribute) is None) for attribute in NEEDED_PAGE_ATTRIBUTES):
            raise RuntimeError(f"Please set all page attributes: {NEEDED_PAGE_ATTRIBUTES}")

        if self.wrap_with_container:
            children = dmc.Container(children)

        super().__init__(
            *args,
            children=children,
            **kwargs,
        )
