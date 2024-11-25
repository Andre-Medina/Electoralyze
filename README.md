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
Can also run the integration tests using
```sh
pixi run integration
```

## API summary

There are two packages within this repo:
- UI: Creates the UI
- Electoralyze: API for Australian census, regions and election data.

### UI



### Electoralyze
