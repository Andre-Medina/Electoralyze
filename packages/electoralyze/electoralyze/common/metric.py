import os
from functools import cached_property
from typing import Callable, Literal

import polars as pl
from electoralyze.region.region_abc import RegionABC
from pydantic import BaseModel, ConfigDict

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


class Metric(BaseModel):
    """Hold data in several regions for a single metric and unify the API.

    Paramters
    ---------
    allowed_regions: list[MetricRegion], list of `MetricRegion` to specify which regions are allowed.
    name: str
    name_prefix: str | None = None
    data_type: METRIC_DATA_TYPES
    file: str
    category_column: str
    data_column: str
    schema: Callable[type(RegionABC), pl.Schema]

    Example
    -------
    ```python
    >>> from electoralyze.common.metric import Metric, MetricRegion
    >>> from electoralyze import region
    >>> Metric(
    ...     allowed_regions=[
    ...         MetricRegion(region.SA1_2021, process_raw = process_raw_population),
    ...         MetricRegion(region.LGA_2021, redistribute_from=region.SA1_2021),
    ...     ],
    ...     name="my_metric",
    ...     file="/home/user/.../data/my_metric/{region}.parquet",
    ...     data_type="ordinal",
    ...     category_column="year",
    ...     data_column="population",
    ...     schema = lambda region: pl.Schema({
    ...         region.id: pl.String,
    ...         "year": pl.Int32,
    ...         "population": pl.Float32,
    ...     }),
    ... )
    ```
    """

    allowed_regions: list[MetricRegion]
    name: str
    name_prefix: str | None = None
    data_type: METRIC_DATA_TYPES
    file: str
    category_column: str
    data_column: str
    schema: Callable[[type[RegionABC]], pl.Schema]

    model_config = ConfigDict(arbitrary_types_allowed=True)

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

    def get_file(self) -> str:
        """Return the path to the clean data, just needs to be formatted with `region`.

        Can be overwritten by child classes to specify different formatting method.

        Example
        -------
        With this function as is:
        ```python
        >>> my_metric = Metric(file = "/home/user/.../data/my_metric/{region}.parquet", ...)
        >>> my_metric.get_file()
        /home/user/.../data/my_metric/{region}.parquet
        ```

        Overwriting this function to use other attributes:
        ```python
        >>> class SubMetric(Metric):
        >>>    ...
        >>>    def get_file(self) -> str:
        >>>        file = f"/home/user/.../data/{self.name}/{{region}}.parquet"
        >>>        return file
        >>>
        >>> my_metric = SubMetric(name="test_new_name", ...)
        >>> my_metric.get_file()
        /home/user/.../data/test_new_name/{region}.parquet
        ```
        """
        file = self.file
        return file

    #### Get and set data #####

    def process_raw(self):
        """Process raw data for all allowed regions."""
        for metric_region in self.allowed_regions:
            if metric_region.process_raw is not None:
                kwargs = metric_region.process_raw_kwargs or {}
                processed_data = metric_region.process_raw(
                    parent_metric=self,
                    **kwargs,
                )

                processed_data_file = self.get_file().format(region=metric_region.region.id)
                os.makedirs(processed_data_file.rsplit("/", maxsplit=1)[0], exist_ok=True)

                processed_data.write_parquet(processed_data_file)

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

        if metric_data.schema != self.schema(region):
            raise ValueError(
                f"Schema mismatch for metric: {self.full_name!r}. "
                f"Expected: {self.schema(region)}, Got: {metric_data.schema}"
            )

        return metric_data

    def _get_stored_data(self, region: RegionABC) -> pl.DataFrame:
        """Get data stored from an existing file."""
        file = self.get_file().format(region=region.id)
        metric_data = pl.read_parquet(file)
        return metric_data

    def _get_redistributed_data(self, region: RegionABC) -> pl.DataFrame:
        """Get data by redistributing from another region."""
        raise NotImplementedError("Redistributing from another region is not implemented yet.")
