import logging
from functools import cached_property
from typing import Callable, Literal

import polars as pl
from electoralyze.common.files import create_path
from electoralyze.region.region_abc import RegionABC
from pydantic import BaseModel, ConfigDict, model_validator
from typing_extensions import Self

METRIC_DATA_TYPES = Literal["categorical", "ordinal", "numeric", "single"]


class MetricRegion(BaseModel):
    """Hold data for a single region.

    Can either be a primary region which should have `process_raw` to read and return the raw data. Or a secondary
    region which should have `redistribute_from` to redistribute data from another region.

    Parameters
    ----------
    region: RegionABC, region to get data for.
    process_raw: Callable[[], pl.DataFrame] | None = None, function to read and return the raw data.
    process_raw_kwargs: dict | None = None, kwargs to pass to `process_raw`.
    redistribute_from: RegionABC | None = None, region to redistribute data from.
    redistribute_kwargs: dict | None = None, kwargs to pass to `redistribute_from.redistribute`.
    """

    region: type[RegionABC]
    redistribute_from: type[RegionABC] | None = None
    redistribute_kwargs: dict | None = None
    process_raw: Callable[[], pl.DataFrame] | None = None
    process_raw_kwargs: dict | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def validate(self) -> Self:
        """Validate that either `process_raw` or `redistribute_from` is specified."""
        if (self.process_raw is None) and (self.redistribute_from is None):
            raise ValueError("Either `process_raw` or `redistribute_from` must be specified.")
        return self


class Metric(BaseModel):
    """Hold data in several regions for a single metric and unify the API.

    Parameters
    ----------
    allowed_regions: list[MetricRegion], list of `MetricRegion` to specify which regions are allowed.
    name: str, name of the metric.
    name_prefix: str | None = None, optional prefix for the name, can be accessed by `full_name`.
    data_type: METRIC_DATA_TYPES, type of the data, one of `categorical`, `ordinal`, `numeric`, `single`.
    processed_path: str, path to the processed data, must be formattable with `{region}`.
    category_column: str, column name for the category.
    value_column: str, column to use for the metric.
    schema_getter: Callable[type(RegionABC), pl.Schema], function to get the schema for a given region.

    Example
    -------
    Basic usage to create a single metric:
    ```python
    >>> from electoralyze.common.metric import Metric, MetricRegion
    >>> from electoralyze import region
    >>> Metric(
    ...     allowed_regions=[
    ...         MetricRegion(region.SA1_2021, process_raw = process_raw_population),
    ...         MetricRegion(region.LGA_2021, redistribute_from=region.SA1_2021),
    ...     ],
    ...     name="my_metric",
    ...     processed_path="/home/user/.../data/my_metric/{region}.parquet",
    ...     data_type="ordinal",
    ...     category_column="year",
    ...     value_column="population",
    ...     schema_getter = lambda region: pl.Schema({
    ...         region.id: pl.String,
    ...         "year": pl.Int32,
    ...         "population": pl.Float32,
    ...     }),
    ... )
    ```

    Creating a subclass used for several similar metrics:
    ```python
    >>> from electoralyze.common.metric import Metric, MetricRegion
    >>> from electoralyze import region
    >>> class Census2021(Metric):
    ...     name_prefix: str = "census_2021"
    ...     processed_path: None = None
    ...     data_type: METRIC_DATA_TYPES = "numeric"
    ...     schema_getter = lambda region: pl.Schema({
    ...         region.id: pl.String,
    ...         "year": pl.Int32,
    ...         "population": pl.Float32,
    ...     }),
    ...
    ...
    ...     def get_processed_path(self) -> str:
    ...         processed_path = f"/home/user/.../data/{self.name_prefix}/{{region_id}}.parquet"
    ...         return processed_path
    ...
    >>> population = Census2021(
    ...     name="population",
    ...     allowed_regions=[
    ...         MetricRegion(region.SA1_2021, process_raw = process_raw_population),
    ...         MetricRegion(region.LGA_2021, redistribute_from=region.SA1_2021),
    ...     ],
    >>> income = Census2021(
    ...     name="population",
    ...     allowed_regions=[
    ...         MetricRegion(region.SA1_2021, process_raw = process_raw_income),
    ...         MetricRegion(region.LGA_2021, redistribute_from=region.SA1_2021),
    ...     ],
    """

    allowed_regions: list[MetricRegion]
    name: str
    name_prefix: str | None = None
    data_type: METRIC_DATA_TYPES
    processed_path: str
    category_column: str
    value_column: str
    schema_getter: Callable[[type[RegionABC]], pl.Schema]

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def validate(self) -> Self:
        """Validate basic info about the Metric."""

        class _FakeRegion:
            id = "fake_region_id"

        fake_schema: pl.Schema = self.schema_getter(_FakeRegion)
        schema_columns = set(fake_schema.keys())
        if schema_columns != {_FakeRegion.id, self.category_column, self.value_column}:
            raise ValueError("Schema must have columns `region_id`, `category_column`, and `value_column`.")

        if len(self.allowed_regions) == 0:
            raise ValueError("At least one `MetricRegion` must be specified.")

        if not self.processed_path.endswith(".parquet"):
            raise ValueError("Path must be a parquet file.")

        if "{region_id}" not in self.processed_path:
            raise ValueError("Path must contain {region_id} in its path.")

        self._validate_allowed_regions()

        return self

    def _validate_allowed_regions(self):
        """Validate the `allowed_regions` make sense."""
        regions_with_processors: list[str] = []
        regions_to_redistribute: dict[str, str] = {}
        errors = []

        for metric_region in self.allowed_regions:
            if metric_region.redistribute_from is not None:
                regions_to_redistribute[metric_region.region.id] = metric_region.redistribute_from.id
            if metric_region.process_raw is not None:
                regions_with_processors.append(metric_region.region.id)

        if len(regions_with_processors) == 0:
            errors.append("There are no regions with `process_raw` functions")

        for region_id, redistribute_from_id in regions_to_redistribute.items():
            if redistribute_from_id not in regions_with_processors:
                errors.append(f"Redistribution from region {region_id} must have a `process_raw` function")

        if len(errors) > 0:
            raise ValueError("\n".join(errors))

    @cached_property
    def full_name(self):
        """Return the full name of the given metric."""
        full_name = f"{self.name_prefix}_{self.name}" if self.name_prefix else f"{self.name}"
        return full_name

    @cached_property
    def allowed_regions_map(self) -> dict[str, MetricRegion]:
        """Extracts the `allowed_regions` into a dictionary lookup."""
        allowed_regions = {}
        for metric_region in self.allowed_regions:
            allowed_regions[metric_region.region.id] = metric_region

        return allowed_regions

    def get_processed_path(self) -> str:
        """Return the path to the clean data, just needs to be formatted with `region_id`.

        Can be overwritten by child classes to specify different formatting method.

        Example
        -------
        With this function as is:
        ```python
        >>> my_metric = Metric(processed_path = "/home/user/.../data/my_metric/{region_id}.parquet", ...)
        >>> my_metric.get_processed_path()
        /home/user/.../data/my_metric/{region_id}.parquet
        ```

        Overwriting this function to use other attributes:
        ```python
        >>> class SubMetric(Metric):
        >>>    ...
        >>>    def get_processed_path(self) -> str:
        >>>        processed_path = f"/home/user/.../data/{self.name}/{{region_id}}.parquet"
        >>>        return processed_path
        >>>
        >>> my_metric = SubMetric(name="test_new_name", ...)
        >>> my_metric.get_processed_path()
        /home/user/.../data/test_new_name/{region_id}.parquet
        ```
        """
        processed_path = self.processed_path
        return processed_path

    #### Get and set data #####

    def process_raw(self):
        """Process raw data for all allowed regions."""
        for metric_region in self.allowed_regions:
            if metric_region.process_raw is not None:
                logging.info(f"Processing raw data for {metric_region.region.id!r}")
                kwargs = metric_region.process_raw_kwargs or {}
                processed_data = metric_region.process_raw(
                    parent_metric=self,
                    **kwargs,
                )

                processed_file = self.get_processed_path().format(region_id=metric_region.region.id)
                create_path(processed_file)

                processed_data.write_parquet(processed_file)

                # Validate data is available
                self.by(region=metric_region.region)

    def by(self, region: RegionABC) -> pl.DataFrame:
        """Get data stored for a given region.

        Parameters
        ----------
        region : RegionABC, region to get data for, must be in `allowed_regions`.

        Returns
        -------
        pl.DataFrame, data for the given metric

        Example
        ```
        >>> my_metric = Metric(...)
        >>> my_metric.by(region=region.SA1_2021)
        ```
        """
        if region.id not in self.allowed_regions_map:
            raise KeyError(f"Region {region.id!r} not found for metric: {self.full_name!r}")

        region_metric = self.allowed_regions_map[region.id]

        if region_metric.redistribute_from:
            metric_data = self._get_redistributed_data(region)
        else:
            metric_data = self._get_stored_data(region)

        if metric_data.schema != self.schema_getter(region):
            raise ValueError(
                f"Schema mismatch for metric: {self.full_name!r}. "
                f"Expected: {self.schema_getter(region)}, Got: {metric_data.schema}"
            )

        return metric_data

    def _get_stored_data(self, region: RegionABC) -> pl.DataFrame:
        """Get data stored from an existing processed_file."""
        processed_file = self.get_processed_path().format(region_id=region.id)
        metric_data = pl.read_parquet(processed_file)
        return metric_data

    def _get_redistributed_data(self, region: RegionABC) -> pl.DataFrame:
        """Get data by redistributing from another region."""
        raise NotImplementedError("Redistributing from another region is not implemented yet.")
