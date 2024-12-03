import polars as pl

from ..region_abc import RegionABC
from .mapping import get_region_mapping_base
from .utils import AGGREGATION_OPTIONS, MAPPING_OPTIONS, WEIGHT_OPTIONS


def redistribute(
    data_by_from: pl.DataFrame,
    *,
    region_from: RegionABC,
    region_to: RegionABC,
    index_columns: list[str] | None = None,
    region_through: RegionABC | None = None,
    weights: WEIGHT_OPTIONS | None = None,
    mapping: MAPPING_OPTIONS | pl.DataFrame = "intersection_area",
    aggregation: AGGREGATION_OPTIONS = "sum",
    redistribute_with_full: bool | None = None,
) -> pl.DataFrame:
    """Redistribute data from one region to another.

    Parameters
    ----------
    data_by_from: pl.DataFrame,
    index_columns: list[str] | None, Index columns in the input dataframe to keep.
    region_from: RegionABC, From region to redistribute, should be a column in the dataframe.
    region_to: RegionABC, To region to redistribute, Will output data with this column.
    region_through: RegionABC | None, If given, will convert region_from -> region_through -> region_to. Useful to
    weights: Literal["population"] | None, weighting to use to redistribute data.
        - None: redistribute by pure `mapping` as the weight.
        - "population", Will use population as a weight.
    mapping: Literal["intersection_area", "centroid_distance"] | pl.DataFrame, mapping to
        geometrically redistribute data.
        - "intersection_area": Will use the intersection area of each regions.
        - "centroid_distance": Will use the centroid distance of each regions.
        - pl.DataFrame: Will use the custom mapping, with columns `region_from`, `region_to`, and `value`.
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
    """
    mapping_method = mapping
    mapping_weights = weights
    aggregation_method = aggregation
    index_columns = index_columns or []

    if region_from.id not in data_by_from.columns:
        raise ValueError(f"From region column `{region_from.id}` not found in data_by_from.")

    data_columns = list(set(data_by_from.columns) - set(index_columns) - {region_from.id, region_to.id})

    if not data_columns:
        raise ValueError("No data columns found in data_by_from.")

    if region_through:
        data_by_through = redistribute(
            data_by_from,
            index_columns=index_columns,
            region_from=region_from,
            region_to=region_through,
            weights=weights,
            mapping=mapping,
            aggregation=aggregation,
            redistribute_with_full=redistribute_with_full,
        )
        data_by_from = data_by_through
        region_from = region_through

    region_mapping = _get_region_mapping(
        region_from=region_from,
        region_to=region_to,
        mapping_method=mapping_method,
        mapping_weights=mapping_weights,
        redistribute_with_full=redistribute_with_full,
    )

    data_distributed = _combine(
        data_by_from=data_by_from,
        region_from=region_from,
        region_mapping=region_mapping,
        data_columns=data_columns,
    )
    data_by_to = _aggregate(
        data_distributed=data_distributed,
        region_to=region_to,
        aggregation_method=aggregation_method,
        index_columns=index_columns,
        data_columns=data_columns,
    )

    return data_by_to


def _get_region_mapping(
    *,
    region_from: RegionABC,
    region_to: RegionABC,
    mapping_method: MAPPING_OPTIONS | pl.DataFrame,
    mapping_weights: WEIGHT_OPTIONS | None,  # noqa: ARG001
    redistribute_with_full: bool | None = None,
) -> pl.DataFrame:
    """_summary_.

    Parameters
    ----------
    region_from : RegionABC, _description_
    region_to : RegionABC, _description_
    mapping_method : MAPPING_OPTIONS | pl.DataFrame, _description_
    mapping_weights : WEIGHT_OPTIONS | None, _description_

    Returns
    -------
    pl.DataFrame, with columns `region_from.id`, `region_to.id` and `ratio`
    E.g.
    ```python
    ```

    Raises
    ------
    ValueError, _description_
    """
    mapping_base: pl.DataFrame

    if isinstance(mapping_method, pl.DataFrame):
        mapping_base = mapping_method
    else:
        mapping_base = get_region_mapping_base(
            region_from=region_from,
            region_to=region_to,
            mapping_method=mapping_method,
            redistribute_with_full=redistribute_with_full,
        )

    if set(mapping_base.columns) != {region_from.id, region_to.id, "mapping"}:
        raise ValueError(f"Mapping should have columns `{region_from.id}`, `{region_to.id}`, and `{"mapping"}`.")

    if mapping_weights is None:
        region_mapping = _distribute(mapping_base, region_id=region_from.id, mapping_column="mapping")
    else:
        weights_per_region = _get_weights(region_from=region_from, mapping_weights=mapping_weights)

        mapping_with_weights = mapping_base.join(weights_per_region, on=region_from.id)

        mapping_with_distributed_weights = mapping_with_weights.pipe(
            _distribute, region_id=region_from.id, mapping_column="mapping"
        ).select(
            region_from.id, region_to.id, pl.col(mapping_weights).mul(pl.col("ratio")).alias("distributed_weights")
        )

        region_mapping = _distribute(
            mapping_with_distributed_weights, region_id=region_to.id, mapping_column="distributed_weights"
        )

    return region_mapping


def _distribute(region_mapping: pl.DataFrame, *, region_id: str, mapping_column: str) -> pl.DataFrame:
    """Distributes mapping over regions."""
    region_ratios = region_mapping.select(
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
    region_mapping: pl.DataFrame,
    data_columns: list[str],
):
    """Combines mapping and distributed data."""
    data_distributed = data_by_from.join(region_mapping, on=region_from.id).select(
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
            print(Warning("Untested!"))
        case "count":
            aggregation_expressions = [pl.col(data_column).count() for data_column in data_columns]
            print(Warning("Untested!"))
        case "max":
            aggregation_expressions = [pl.col(data_column).max() for data_column in data_columns]
            print(Warning("Untested!"))
        case "min":
            aggregation_expressions = [pl.col(data_column).min() for data_column in data_columns]
            print(Warning("Untested!"))
        case _:
            raise ValueError(f"Unknown aggregation method `{aggregation_method}`.")

    if index_columns:
        data_by_to = data_distributed.group_by(region_to.id, index_columns).agg(*aggregation_expressions)
    else:
        data_by_to = data_distributed.group_by(region_to.id).agg(*aggregation_expressions)

    return data_by_to
