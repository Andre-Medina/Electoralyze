# Electoralyze

### `Electorate x Analyze`: *Elec-tora-lyze*

Welcome! This is a passion project for displaying election and census data in an easy to use UI. It's open source so feel free to contribute 😊.

## Installing

#### *⚠️NOTE⚠️*
- *This package was developed on Debian Linux WSL using Visual Studio Code, it might not be 100% compatible with your OS and IDE.*
- *This project contains several alpha packages, don't expect everything to work like magic.*

After this repo is [cloned](https://docs.github.com/en/repositories/creating-and-managing-repositories/cloning-a-repository). You must use pixi to install the environment.

Thankfully, installing is made easy with [pixi](https://pixi.sh/latest/). Once the program itself is installed, use the following commands from within the cloned repo:

```sh
pixi install
pixi global install pre-commit
pre-commit install
```

To set the python interpreter for your IDE to the current environment; You can get the `python.exe` file by:
```sh
>>> pixi r which python
/home/user/git/electoralyze/.pixi/envs/default/bin/python
```


## Testing

Once the package is all set up. Please test it by running the following.

To check pixi install correctly:
```sh
pixi run test
```
To check the the local pytests:
```sh
pixi run tests
```
To run the integration tests use:
```sh
pixi run integration
```
And finally you can try launching the UI by navigating to the "run and debug menu" and launching the UI.
![Debugger button](/data/readme_images/debugger_play_button.png)

## Contributing

Information for contributing to a public repository can be found [here on the github website](https://docs.github.com/en/get-started/exploring-projects-on-github/contributing-to-a-project). But the general steps are:

- Fork this repo.
- Create a branch on your fork.
- Create a pull request from your new branch on your fork to main in the main repo.

#### *⚠️NOTE⚠️*
- *There are several tests to check the names of issue, PRs and branches conform to the naming convention. You should receive helpful errors if your chose name doesn't comply.*
- *This is a passion project, but maintainable code is important. Expect reviews when contributing to this project to ask for improvements to documentation and readability.*

# Experiments and Modelling

There are two folders used to organise [jupyter notebooks](https://ipython.org/notebook.html)

- [experiments](/experiments/): For developing functions and testing them once they are created.
- [modelling](/modelling/): For showcasing completed script to process data and create models.

Attempt to clear the outputs of these notebooks before committing them to main to avoid merge conflicts.

### Experiments

Sandbox of sorts to store experiments for later reference. Use a numbering system and keep experiments organized. Old experiments will likely be culled as the repo grows.

### Modelling

Where to place final pieces of work which generate nice plots or were part of a in-depth investigation. Keep these files neat and tidy and easy for others to follow. Files here will unlikely be culled.

# API summary

Packages are kept under [packages](/packages/)
There are three packages within this repo:
- UI: Creates the UI
- Electoralyze: API for Australian census, regions and election data.
- Electoralive: API for viewing live election results on election night.

## UI


To start the UI locally, use the debugger in Visual Studio Code and run `Electoralyze UI`. It should automatically run `ui/app.py` which is the main file which creates the application.

To create a new page. Refer to the example and instructions in `ui/common/page.py`.

## Electoralyze


Has three main APIs:

- `electoralyze.region`: API for Australia regions like SA1 and LGA as well as redistributing between them.
- `electoralyze.census`: API for Australian Census data, linking to regions.
- `electoralyze.election`: API for Australian Election data, linking to regions.

### `region`

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

### `census`

WIP

### `election`

WIP


## Electoralive

### `Electorate x Live`: *Elec-tora-live*

Separate package for adding a live view of data on election night to the UI.

WIP
