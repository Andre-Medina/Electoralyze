from logging import warning
from typing import Literal

import polars as pl
from polars.exceptions import ColumnNotFoundError

from ..region_abc import RegionABC
from .mapping import get_region_mapping_base
from .utils import AGGREGATION_OPTIONS, MAPPING_OPTIONS, WEIGHT_OPTIONS

DEFAULT_RATIO_TOLERANCE = 0.0001


def redistribute(
    data_by_from: pl.DataFrame,
    *,
    region_from: RegionABC,
    region_to: RegionABC,
    index_columns: list[str] | None = None,
    region_via: RegionABC | None = None,
    weights: WEIGHT_OPTIONS | None = None,
    mapping: MAPPING_OPTIONS | pl.DataFrame = "intersection_area",
    aggregation: AGGREGATION_OPTIONS = "sum",
    redistribute_with_full: bool | None = None,
    errors: Literal["raise", "warning"] = "raise",
) -> pl.DataFrame:
    """Redistribute data from one region to another.

    Parameters
    ----------
    data_by_from: pl.DataFrame,
    index_columns: list[str] | None, Index columns in the input dataframe to keep.
    region_from: RegionABC, From region to redistribute, should be a column in the dataframe.
    region_to: RegionABC, To region to redistribute, Will output data with this column.
    region_via: RegionABC | None, If given, will convert region_from -> region_via -> region_to. Useful to
    weights: Literal["population"] | None, weighting to use to redistribute data.
        - None: redistribute by pure `mapping` as the weight.
        - "population", Will use population as a weight.
    mapping: Literal["intersection_area", "centroid_distance"] | pl.DataFrame, mapping to
        geometrically redistribute data.
        - "intersection_area": Will use the intersection area of each regions.
        - "centroid_distance": Will use the centroid distance of each regions.
        - pl.DataFrame: Will use the custom mapping, with columns `region_from`, `region_to`, and `mapping`.
    aggregation: Literal["sum", "mean", "count", "max", "min"], mapping to aggregate the redistributed data.
        - "sum": Will take the proportional sum the sub regions.
        - "mean": Will take the proportional mean of sub regions.
        - "count": Will take the proportional count of sub regions.
        - "max": Will take the absolute max of sub regions.
        - "min": Will take the absolute min of sub regions.
    redistribute_with_full: bool | None = None,
        - None = only use existing static redistribution map files.
        - False = Will create redistribution maps using simplified geometries for each region.
        - True = Will create redistribution maps using full geometries for each region (EXPENSIVE!).
    errors: Literal["raise", "warning"] = "raise",
        - "raise": Will raise an error if the redistribution fails.
        - "warning": Will print a warning if the redistribution fails.


    Examples
    --------
    ```python
    >>> region.quadrants.geometry
    shape: (4, 2)
    ┌──────────┬─────────────────────────────────┐
    │ quadrant ┆ geometry                        │
    │ ---      ┆ ---                             │
    │ str      ┆ binary                          │
    ╞══════════╪═════════════════════════════════╡
    │ M        ┆ POLYGON ((0 0, -4 0, -4 4, 0 ...|
    │ N        ┆ POLYGON ((0 0, 0 4, 4 4, 4 0,...|
    │ O        ┆ POLYGON ((0 0, 0 -4, -4 -4, -...|
    │ P        ┆ POLYGON ((0 0, 4 0, 4 -4, 0 -...|
    └──────────┴─────────────────────────────────┘
    >>> region.square.geometry
    shape: (1, 2)
    ┌────────┬─────────────────────────────────┐
    │ square ┆ geometry                        │
    │ ---    ┆ ---                             │
    │ str    ┆ binary                          │
    ╞════════╪═════════════════════════════════╡
    │ main   ┆ POLYGON ((-4 -4, -4 4, 4 4, 4...|
    └────────┴─────────────────────────────────┘
    >>> region.triangles.geometry
    shape: (3, 2)
    ┌──────────┬─────────────────────────────────┐
    │ triangle ┆ geometry                        │
    │ ---      ┆ ---                             │
    │ str      ┆ binary                          │
    ╞══════════╪═════════════════════════════════╡
    │ A        ┆ POLYGON ((-4 -4, 0 4, 4 -4, -...|
    │ B        ┆ POLYGON ((-4 -4, -8 4, 0 4, -...|
    │ C        ┆ POLYGON ((4 -4, 0 4, 8 4, 4 -...|
    └──────────┴─────────────────────────────────┘
    >>> data_by_from
    shape: (1, 2)
    ┌────────┬──────┐
    │ square ┆ data │
    │ ---    ┆ ---  │
    │ str    ┆ f64  │
    ╞════════╪══════╡
    │ main   ┆ 64.0 │
    └────────┴──────┘
    >>> region.redistribute(
    ...     data_by_from=data_by_from,
    ...     region_from=region.square,
    ...     region_via=region.triangles,
    ...     region_to=region.quadrants,
    ...     mapping="intersection_area",
    ...     aggregation="sum",
    ... )
    shape: (5, 2)
    ┌──────────┬──────┐
    │ quadrant ┆ data │
    │ ---      ┆ ---  │
    │ str      ┆ f64  │
    ╞══════════╪══════╡
    │ N        ┆ 10.0 │
    │ O        ┆ 14.0 │
    │ P        ┆ 14.0 │
    │ M        ┆ 10.0 │
    │ null     ┆ 16.0 │
    └──────────┴──────┘
    ```
    """
    mapping_method = mapping
    mapping_weights = weights
    aggregation_method = aggregation
    index_columns = index_columns or []

    if region_from.id not in data_by_from.columns:
        raise ValueError(f"From region column `{region_from.id}` not found in data_by_from.")

    if region_from.id == region_to.id:
        raise ValueError(f"`from` and `to` region cannot be the same. Both were {region_from.id!r}")

    data_columns = list(set(data_by_from.columns) - set(index_columns) - {region_from.id, region_to.id})

    if not data_columns:
        raise ValueError("No data columns found in data_by_from.")

    if region_via:
        data_by_through = redistribute(
            data_by_from,
            index_columns=index_columns,
            region_from=region_from,
            region_to=region_via,
            weights=weights,
            mapping=mapping,
            aggregation=aggregation,
            redistribute_with_full=redistribute_with_full,
        )
        data_by_from = data_by_through
        region_from = region_via

    region_ratios = _get_region_to_region_ratio(
        region_from=region_from,
        region_to=region_to,
        mapping_method=mapping_method,
        mapping_weights=mapping_weights,
        redistribute_with_full=redistribute_with_full,
    )

    data_distributed = _combine(
        data_by_from=data_by_from,
        region_from=region_from,
        region_ratios=region_ratios,
        data_columns=data_columns,
    )
    data_by_to = _aggregate(
        data_distributed=data_distributed,
        region_to=region_to,
        aggregation_method=aggregation_method,
        index_columns=index_columns,
        data_columns=data_columns,
    )
    _validate(data_by_from, data_by_to, data_columns, errors=errors)

    return data_by_to


def _get_region_to_region_ratio(
    *,
    region_from: RegionABC,
    region_to: RegionABC,
    mapping_method: MAPPING_OPTIONS | pl.DataFrame,
    mapping_weights: WEIGHT_OPTIONS | None,  # noqa: ARG001
    redistribute_with_full: bool | None = None,
) -> pl.DataFrame:
    """Get the ratio of how much to distribute on region to another.

    Returns
    -------
    pl.DataFrame, with columns `region_from.id`, `region_to.id` and `ratio`

    Example
    -------
    ```python
    >>> _get_region_to_region_ratio(region_from=region.region_a, region_to=region_b, mapping_method="intersection_area")
    shape: (4, 3)
    ┌────────┬──────────┬─────────┐
    │ regn_a ┆ region_b ┆ ratio   │
    │ ---    ┆ ---      ┆ ---     │
    │ str    ┆ str      ┆ f64     │
    ╞════════╪══════════╪═════════╡
    │ main   ┆ M        ┆ 0.25    │
    │ main   ┆ N        ┆ 0.25    │
    │ main   ┆ O        ┆ 0.25    │
    │ main   ┆ P        ┆ 0.25    │
    └────────┴──────────┴─────────┘
    ```
    """
    region_mapping_all: pl.DataFrame

    if isinstance(mapping_method, pl.DataFrame):
        region_mapping_all = mapping_method
    else:
        region_mapping_all = get_region_mapping_base(
            region_from=region_from,
            region_to=region_to,
            mapping_method=mapping_method,
            redistribute_with_full=redistribute_with_full,
        )

    try:
        region_mapping = region_mapping_all.select(region_from.id, region_to.id, "mapping")
    except ColumnNotFoundError:
        raise ColumnNotFoundError(
            f"Mapping should have at least columns `{region_from.id}`, `{region_to.id}`, and "
            f"`{"mapping"}`, It has {region_mapping_all.columns}"
        ) from None

    if mapping_weights is None:
        region_ratios = _distribute(region_mapping, region_id=region_from.id, mapping_column="mapping")
    else:
        weights_per_region = _get_weights(region_from=region_from, mapping_weights=mapping_weights)

        mapping_with_weights = region_mapping.join(weights_per_region, on=region_from.id)

        mapping_with_distributed_weights = mapping_with_weights.pipe(
            _distribute, region_id=region_from.id, mapping_column="mapping"
        ).select(
            region_from.id, region_to.id, pl.col(mapping_weights).mul(pl.col("ratio")).alias("distributed_weights")
        )

        region_ratios = _distribute(
            mapping_with_distributed_weights, region_id=region_to.id, mapping_column="distributed_weights"
        )

    return region_ratios


def _distribute(region_ratios: pl.DataFrame, *, region_id: str, mapping_column: str) -> pl.DataFrame:
    """Distributes mapping over regions."""
    region_ratios = region_ratios.select(
        pl.exclude(mapping_column),
        pl.col(mapping_column).truediv(pl.col(mapping_column).sum()).over(region_id).alias("ratio"),
    )

    return region_ratios


def _get_weights(*, region_from: RegionABC, mapping_weights: WEIGHT_OPTIONS) -> pl.DataFrame:  # noqa: ARG001
    """Get data to use as weights for the given region."""
    match mapping_weights:
        case "population":
            raise NotImplementedError("Not implemented yet.")
        case _:
            raise ValueError(f"Unknown weight `{mapping_weights}`.")


def _combine(
    *,
    data_by_from: pl.DataFrame,
    region_from: RegionABC,
    region_ratios: pl.DataFrame,
    data_columns: list[str],
):
    """Combines mapping and distributed data."""
    data_distributed = data_by_from.join(region_ratios, on=region_from.id).select(
        pl.exclude(data_columns),
        *[pl.col(data_column).mul(pl.col("ratio")) for data_column in data_columns],
    )

    return data_distributed


def _aggregate(
    data_distributed: pl.DataFrame,
    region_to: RegionABC,
    aggregation_method: AGGREGATION_OPTIONS,
    index_columns: list[str] | None,
    data_columns: list[str],
) -> pl.DataFrame:
    """Aggregates the fully distributed data to the final region."""
    match aggregation_method:
        case "sum":
            aggregation_expressions = [pl.col(data_column).sum() for data_column in data_columns]
        case "mean":
            aggregation_expressions = [pl.col(data_column).mean() for data_column in data_columns]
            raise NotImplementedError("This aggregation method is not implemented yet.")
        case "count":
            aggregation_expressions = [pl.col(data_column).count() for data_column in data_columns]
            raise NotImplementedError("This aggregation method is not implemented yet.")
        case "max":
            aggregation_expressions = [pl.col(data_column).max() for data_column in data_columns]
            raise NotImplementedError("This aggregation method is not implemented yet.")
        case "min":
            aggregation_expressions = [pl.col(data_column).min() for data_column in data_columns]
            raise NotImplementedError("This aggregation method is not implemented yet.")
        case _:
            raise ValueError(f"Unknown aggregation method `{aggregation_method}`.")

    if index_columns:
        data_by_to = data_distributed.group_by(region_to.id, index_columns).agg(*aggregation_expressions)
    else:
        data_by_to = data_distributed.group_by(region_to.id).agg(*aggregation_expressions)

    return data_by_to


def _validate(
    data_by_from: pl.DataFrame,
    data_by_to: pl.DataFrame,
    data_columns: list[str],
    errors: Literal["raise", "warning"],
    ratio_tolerance: float = DEFAULT_RATIO_TOLERANCE,
):
    """Validate that data_by_from and data_by_to have the same amount of data."""
    bad_data_transformations = []
    for data_column in data_columns:
        from_total = data_by_from[data_column].sum()
        to_total = data_by_to[data_column].sum()
        ratio = to_total / from_total
        if (ratio > 1 + ratio_tolerance) or (ratio < 1 - ratio_tolerance):
            bad_data_transformations.append(
                f"Miss match in data for column: {data_column!r}. From: {from_total!r} -> To: {to_total!r}"
            )

    if bad_data_transformations:
        error_message = "Found differences in input and output data while redistributing.\n" + "\n".join(
            bad_data_transformations
        )

        if errors == "raise":
            raise ValueError(error_message)
        if errors == "warning":
            warning(error_message)
