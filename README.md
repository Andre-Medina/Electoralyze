# Project
Electorate x Analyze: Elec-tora-lyze

## Install

Installing is easy with [pixi](https://pixi.sh/latest/). Once installed use the following commands:

```sh
pixi install
pixi global install pre-commit
pre-commit install
```

You can get the `python.exe` file by:
```sh
pixi r which python
```


## Testing

pytests will run with pre-commit, to run manually use:
```sh
pixi run tests
```

## API summary

There are two packages within this repo:
- UI: Creates the UI
- Electoralyze: API for Australian census, regions and election data.

### UI

To start the UI locally, use the debugger in Visual Studio Code and run `Electoralyze UI`. It should automatically run `ui/app.py` which is the main file which creates the application.

To create a new page. Refer to the example and instructions in `ui/common/page.py`.

### Electoralyze
