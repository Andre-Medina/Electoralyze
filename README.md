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

## Contributing

- Fork this repo.
- Create a branch on your fork.
- Create a pull request from your new branch on your fork to main in the main repo.


## API summary

There are two packages within this repo:
- UI: Creates the UI
- Electoralyze: API for Australian census, regions and election data.
- Electoralive: API for viewing live election results on election night.

### UI


To start the UI locally, use the debugger in Visual Studio Code and run `Electoralyze UI`. It should automatically run `ui/app.py` which is the main file which creates the application.

To create a new page. Refer to the example and instructions in `ui/common/page.py`.

### Electoralyze


Has three main APIs:

- `electoralyze.region`: API for Australia regions like SA1 and LGA as well as redistributing between them.
- `electoralyze.census`: API for Australian Census data, linking to regions.
- `electoralyze.election`: API for Australian Election data, linking to regions.

#### `region`

API for processing and reading geometries for different Australian regions.

Accessed by
```python
>>> from electoralyze import region
>>> region.SA2_2021.geometry
shape: (2_472, 2)
┌───────────┬─────────────────────────────────┐
│ SA2_2021  ┆ geometry                        │
│ ---       ┆ ---                             │
│ i64       ┆ binary                          │
╞═══════════╪═════════════════════════════════╡
│ 305031128 ┆ POLYGON ((153.040413 -27.44932… │
│ 511041289 ┆ POLYGON ((114.921946 -29.27032… │
│ 211051285 ┆ POLYGON ((145.411636 -37.79203… │
│ 110011188 ┆ POLYGON ((151.135773 -30.28397… │
│ 121011687 ┆ POLYGON ((151.209539 -33.80446… │
│ …         ┆ …                               │
│ 212031458 ┆ POLYGON ((145.279819 -38.06734… │
│ 307011178 ┆ POLYGON ((150.400255 -26.97881… │
│ 316071546 ┆ POLYGON ((152.976155 -26.61877… │
│ 202021026 ┆ POLYGON ((144.421959 -36.81470… │
│ 208021181 ┆ POLYGON ((145.078188 -37.88713… │
└───────────┴─────────────────────────────────┘
```

main properties and methods include

- `region.<REGION>.id`, The column name for the `region_ids`.
- `region.<REGION>.name`, The column name for the region names.
- `region.<REGION>.geometry`, GeoDataFrame linking the `region_ids` to polygons.
- `region.<REGION>.metadata`, DataFrame linking `region_ids` to metadata about each geometry.
- `region.<REGION>.process_raw()`, Extracts the raw data and saves it so `region.<REGION>.geometry` and `region.<REGION>.metadata` have something to read.

#### `census`

#### `election`


### Electoralive

"Elec-tora-live"
