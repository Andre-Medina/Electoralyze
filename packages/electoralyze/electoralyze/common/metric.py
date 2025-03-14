import logging
import os
from functools import cached_property
from typing import Callable, Literal, get_type_hints

import polars as pl
from electoralyze.common.files import create_path
from electoralyze.region.redistribute import redistribute
from electoralyze.region.region_abc import RegionABC
from pydantic import BaseModel, ConfigDict, computed_field, model_validator
from typing_extensions import Self

METRIC_DATA_TYPES = Literal["categorical", "ordinal", "numeric", "integer"]


class MetricRegion(BaseModel):
    """Hold data for a single region.

    Can either be a primary region which should have `process_raw` to read and return the raw data. Or a secondary
    region which should have `redistribute_from` to redistribute data from another region.

    Parameters
    ----------
    region: RegionABC
        region to get data for.
    process_raw: Callable[[], pl.DataFrame] | None = None
        function to read and return the raw data,
        View Metric for schema information.
    process_raw_kwargs: dict | None = None
        kwargs to pass to `process_raw`.
    redistribute_from: RegionABC | None = None
        region to redistribute data from.
    redistribute_kwargs: dict | None = None
        kwargs to pass to `redistribute_from.redistribute`.

    Example
    -------
    Creating a primary region

    >>> from electoralyze.common.metric import MetricRegion
    >>> from electoralyze import region
    >>> MetricRegion(
    ...     region=region.SA1_2021,
    ...     process_raw = process_raw_population,
    ... )

    Creating a secondary region

    >>> from electoralyze.common.metric import MetricRegion
    >>> from electoralyze import region
    >>> MetricRegion(
    ...     region=region.SA2_2021,
    ...     redistribute_from=region.SA1_2021,
    ... )

    """

    region: type[RegionABC]
    redistribute_from: type[RegionABC] | None = None
    redistribute_kwargs: dict | None = None
    process_raw: Callable[[], pl.DataFrame] | None = None
    process_raw_kwargs: dict | None = None

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @computed_field
    @property
    def is_primary(self) -> bool:
        """Whether the region is a primary region."""
        is_primary_ = self.process_raw is not None
        return is_primary_

    ## Validation

    @model_validator(mode="after")
    def validate(self) -> Self:
        """Validate the given values fit with a primary or secondary metric."""
        if self.is_primary:
            self_ = self._validate_primary()
        else:
            self_ = self._validate_secondary()

        return self_

    def _validate_primary(self) -> Self:
        """Validate metric region as if its a primary source."""
        if self.redistribute_from is not None:
            raise ValueError("If the metric region is primary, then `redistribute_from` should be None.")
        if self.redistribute_kwargs is not None:
            raise ValueError("If the metric region is primary, then `redistribute_kwargs` should be None.")

        if not callable(self.process_raw):
            raise ValueError("If the metric region is primary, then `process_raw` must be a function.")
        self.process_raw_kwargs = self.process_raw_kwargs or {}
        errors_with_process_raw = []

        kwarg_to_type = {kwarg_name: type(value) for kwarg_name, value in self.process_raw_kwargs.items()} | {
            "parent_metric": Metric,
            "region": RegionABC,
            "return": pl.DataFrame,
            "download": bool,
            "force_new": bool,
            "_kwargs": dict,
        }

        for kwarg_name, passed_type in kwarg_to_type.items():
            expected_type = get_type_hints(self.process_raw).get(kwarg_name)
            if passed_type is not expected_type:
                errors_with_process_raw.append(
                    f"`process_raw` kwargs/ returns must align for parameter name: `{kwarg_name}` ."
                    f"types: expected type: `{passed_type}` and function type: `{expected_type}`."
                )

        expected_kwarg_names = list(get_type_hints(self.process_raw).keys())
        passed_kwarg_names = list(kwarg_to_type.keys())
        if set(expected_kwarg_names) != set(passed_kwarg_names):
            errors_with_process_raw.append(
                f"`process_raw` & `process_raw_kwargs` kwargs list must align: "
                f"`{expected_kwarg_names}` != `{passed_kwarg_names}`."
            )

        if len(errors_with_process_raw):
            raise ValueError(
                "Errors with `process_raw` & possibly `process_raw_kwargs`:\n\n" + "\n".join(errors_with_process_raw)
            )

        return self

    def _validate_secondary(self) -> Self:
        """Validate metric region as if its a secondary source."""
        if self.process_raw_kwargs is not None:
            raise ValueError("If the metric region is secondary, then `process_raw_kwargs` should be None.")

        if self.redistribute_from is None:
            raise ValueError("If the metric region is secondary, then `redistribute_from` must be set.")
        self.redistribute_kwargs = self.redistribute_kwargs or {}

        expected_redistribute_kwarg_names = set(get_type_hints(redistribute).keys()) - {
            "data_by_from",
            "region_from",
            "region_to",
        }
        passed_redistribute_kwarg_names = set(self.redistribute_kwargs.keys())
        if passed_redistribute_kwarg_names - expected_redistribute_kwarg_names:
            raise ValueError(
                f"Bad `redistribute_kwargs` passed. Allowed: `{expected_redistribute_kwarg_names!r}` ."
                f"extras received: `{(passed_redistribute_kwarg_names - expected_redistribute_kwarg_names)!r}`"
            )

        return self


class Metric(BaseModel):
    """Hold data in several regions for a single metric and unify the API.

    Parameters
    ----------
    allowed_regions: list[MetricRegion]
        A list of `MetricRegion` to specify which regions are allowed.
    name: str
        the name of the metric.
    name_prefix: str | None, default = None
        optional prefix for the name, can be accessed by `full_name`.
    data_type: METRIC_DATA_TYPES, default = "numeric"
        the type of the data, one of `categorical`, `ordinal`, `numeric`, `integer`.
    processed_path: str
        path to the processed data, must be formattable with `{region}`.
    category_column: str, default = "category"
        The column name for the category.
    value_column: str, default = "value"
        The column to use for the metric.
    schema: pl.Schema
        `default = pl.Schema({"region_id": pl.String, "category": pl.Int32, "value": pl.Float32})`
        the schema for a given region.

    Example
    -------
    simplistic usage to create a single metric

    >>> from electoralyze.common.metric import Metric, MetricRegion
    >>> from electoralyze import region
    >>> Metric(
    ...     allowed_regions=[
    ...         MetricRegion(region.SA1_2021, process_raw = process_raw_population),
    ...     ],
    ...     name="my_metric",
    ...     processed_path="/home/user/.../data/my_metric/{region}.parquet",
    ... )

    Basic usage to create a single metric with some custom logic

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
    ...     schema = pl.Schema({
    ...         "region_id": pl.String,
    ...         "year": pl.Int32,
    ...         "value": pl.Float32,
    ...     }),
    ... )

    Creating a subclass used for several similar metrics (view tests for more examples):

    >>> from electoralyze.common.metric import Metric, MetricRegion
    >>> from electoralyze import region
    >>> class Census2021(Metric):
    ...     name_prefix: str = "census_2021"
    ...     processed_path: None = None
    ...     data_type: METRIC_DATA_TYPES = "integer"
    ...     category_column="year",
    ...     schema = pl.Schema({
    ...         "region_id": pl.String,
    ...         "year": pl.Int32,
    ...         "value": pl.Int32,
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
    ... )
    >>> income = Census2021(
    ...     name="income",
    ...     allowed_regions=[
    ...         MetricRegion(region.SA1_2021, process_raw = process_raw_income),
    ...         MetricRegion(region.LGA_2021, redistribute_from=region.SA1_2021),
    ...     ],
    ... )
    """

    allowed_regions: list[MetricRegion]
    name: str
    name_prefix: str | None = None
    processed_path: str
    data_type: METRIC_DATA_TYPES = "numeric"
    category_column: str = "category"
    value_column: str = "value"
    schema: pl.Schema = pl.Schema(
        {
            "region_id": pl.String,
            "category": pl.Int32,
            "value": pl.Float32,
        }
    )

    model_config = ConfigDict(arbitrary_types_allowed=True)

    @model_validator(mode="after")
    def validate(self) -> Self:
        """Validate basic info about the Metric."""

        class _FakeRegion:
            id = "fake_region_id"

        fake_schema: pl.Schema = self._get_schema(_FakeRegion)
        schema_columns = set(fake_schema.keys())
        if schema_columns != {"region_id", self.category_column, self.value_column}:
            raise ValueError(
                f"Schema must have columns `region_id`, `{self.category_column}`, and `{self.value_column}`."
            )

        if len(self.allowed_regions) == 0:
            raise ValueError("At least one `MetricRegion` must be specified.")

        if self.processed_path is not None:
            if not self.processed_path.endswith(".parquet"):
                raise ValueError("Path must be a parquet file.")

            if "{region_id}" not in self.processed_path:
                raise ValueError("Path must contain {region_id} in its path.")

        self._validate_allowed_regions()

        return self

    def _validate_allowed_regions(self):
        """Validate the `allowed_regions` make sense."""
        primary_regions: list[str] = []
        secondary_regions: dict[str, str] = {}
        errors = []

        for metric_region in self.allowed_regions:
            if metric_region.is_primary:
                primary_regions.append(metric_region.region.id)
            else:
                secondary_regions[metric_region.region.id] = metric_region.redistribute_from.id

        if len(primary_regions) == 0:
            errors.append("There are no regions with `process_raw` functions")

        for region_id, redistribute_from_id in secondary_regions.items():
            if redistribute_from_id not in primary_regions:
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

        >>> my_metric = Metric(processed_path = "/home/user/.../data/my_metric/{region_id}.parquet", ...)
        >>> my_metric.get_processed_path()
        /home/user/.../data/my_metric/{region_id}.parquet


        Overwriting this function to use other attributes:

        >>> class SubMetric(Metric):
        ...    ...
        ...    def get_processed_path(self) -> str:
        ...        processed_path = f"/home/user/.../data/{self.name}/{{region_id}}.parquet"
        ...        return processed_path
        ...
        >>> my_metric = SubMetric(name="test_new_name", ...)
        >>> my_metric.get_processed_path()
        /home/user/.../data/test_new_name/{region_id}.parquet

        """
        processed_path = self.processed_path
        return processed_path

    #### Get and set data #####

    def process_raw(self, *, force_new: bool = False, download: bool = True, **kwargs: dict):
        """Process raw data for all allowed regions.

        Parameters
        ----------
        force_new (bool, optional): Defaults to False
            If True, will force a new download of the raw data.
        download (bool, optional): Defaults to True
            If True, will download the raw data.
        """
        for metric_region in self.allowed_regions:
            if metric_region.is_primary:
                processed_file = self.get_processed_path().format(region_id=metric_region.region.id)
                create_path(processed_file)
                if (not force_new) and (os.path.exists(processed_file)):
                    logging.info(f"Skipping processing raw data for {metric_region.region.id!r}")
                    continue

                logging.info(f"Processing raw data for {metric_region.region.id!r}")
                default_kwargs = metric_region.process_raw_kwargs or {}
                processed_data = metric_region.process_raw(
                    parent_metric=self,
                    region=metric_region.region,
                    force_new=force_new,
                    download=download,
                    **(default_kwargs | kwargs),
                )

                processed_data.write_parquet(processed_file)

                # Validate data is available
                reread_data = self.by(region=metric_region.region)
                regions_in_data = reread_data["region_id"].unique()
                regions_allowed = metric_region.region.geometry[metric_region.region.id].unique()
                if len(set(regions_in_data) - set(regions_allowed)) > 0:
                    raise ValueError(
                        f"Data for {metric_region.region.id!r} not found in processed data. "
                        f"Allowed: {regions_allowed}, Got: {regions_in_data}"
                    )

    def by(self, region: RegionABC) -> pl.DataFrame:
        """Get data stored for a given region.

        Parameters
        ----------
        region : RegionABC
            The region to get data for, must be in `allowed_regions`.

        Returns
        -------
        pl.DataFrame, data for the given metric

        Example
        -------

        >>> my_metric = Metric(...)
        >>> my_metric.by(region=region.SA1_2021)

        """
        if region.id not in self.allowed_regions_map:
            raise KeyError(f"Region {region.id!r} not found for metric: {self.full_name!r}")

        region_metric = self.allowed_regions_map[region.id]

        if region_metric.is_primary:
            metric_data = self._get_stored_data(region)
        else:
            metric_data = self._get_redistributed_data(region)

        if metric_data.schema != self._get_schema(region):
            raise ValueError(
                f"Schema mismatch for metric: {self.full_name!r}. "
                f"Expected: {self._get_schema(region)}, Got: {metric_data.schema}"
            )

        return metric_data

    def _get_schema(self, region: RegionABC) -> pl.Schema:  # noqa: ARG002
        """Overrideable function to change kwargs in schema getter."""
        return self.schema

    def _get_stored_data(self, region: RegionABC) -> pl.DataFrame:
        """Get data stored from an existing processed_file."""
        processed_file = self.get_processed_path().format(region_id=region.id)
        metric_data = pl.read_parquet(processed_file)
        return metric_data

    def _get_redistributed_data(self, region: RegionABC) -> pl.DataFrame:
        """Get data by redistributing from another region."""
        raise NotImplementedError("Redistributing from another region is not implemented yet.")
