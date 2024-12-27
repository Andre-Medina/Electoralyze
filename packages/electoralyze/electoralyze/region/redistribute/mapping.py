import logging
import os

import polars as pl
import polars_st as st

from electoralyze.common.files import create_path

from ..region_abc import RegionABC
from .utils import MAPPING_OPTIONS


def get_region_mapping_base(
    region_from: RegionABC,
    region_to: RegionABC,
    *,
    mapping_method: MAPPING_OPTIONS,
    redistribute_with_full: bool | None = None,
    save_data: bool = False,
    force_new: bool = False,
) -> pl.DataFrame:
    """Get region mapping base.

    Parameters
    ----------
    region_from: RegionABC, From region to redistribute, should be a column in the dataframe.
    region_to: RegionABC, To region to redistribute, Will output data with this column.
    mapping_method: Literal["intersection_area", "centroid_distance"], mapping method, refer to `redistribute`.
    redistribute_with_full: bool | None = None,
        - None = only use existing static redistribution map files.
        - False = Will create redistribution maps using simplified geometries for each region.
        - True = Will create redistribution maps using full geometries for each region (EXPENSIVE!).
    save_data: bool = False, save data locally if True.
    force_new: bool = False, force new mapping file, even if one already exists.

    Returns
    -------
    pl.DataFrame, mapping from region_from to region_to.
    E.g.
    ```
    >>> get_region_mapping_base(region.region_a, region.region_b, mapping_method="intersection_area")
    shape: (10, 3)
    ┌──────────┬──────────┬──────────┐
    │ region_a ┆ region_b ┆ mapping  │
    │ ---      ┆ ---      ┆ ---      │
    │ str      ┆ str      ┆ f64      │
    ╞══════════╪══════════╪══════════╡
    │ M        ┆ A        ┆ 0.25     │
    │ M        ┆ B        ┆ 0.75     │
    │ N        ┆ A        ┆ 0.25     │
    │ N        ┆ C        ┆ 0.75     │
    │ O        ┆ A        ┆ 0.75     │
    │ O        ┆ B        ┆ 0.25     │
    │ P        ┆ A        ┆ 0.75     │
    │ P        ┆ C        ┆ 0.25     │
    │ null     ┆ C        ┆ 1.0      │
    │ null     ┆ B        ┆ 1.0      │
    └──────────┴──────────┴──────────┘
    ```
    """
    mapping_file = _get_region_mapping_file(region_from, region_to, mapping=mapping_method)

    if (redistribute_with_full is None) and (not os.path.exists(mapping_file)):
        raise FileNotFoundError(
            f"Mapping file not found for `{region_from.id}` -> `{region_to.id}` under mapping `{mapping_method}`. "
            "Consider passing `redistribute_with_full=True/False`."
        )
    if (force_new is True) and (redistribute_with_full is None):
        raise ValueError("Cannot force new mapping file if `redistribute_with_full = None`")
    if (save_data is True) and redistribute_with_full is not True:
        raise ValueError("Cannot save data with simplified regions. set `redistribute_with_full = True` to save")

    if force_new or (not os.path.exists(mapping_file)):
        logging.info("Generating region mapping.")

        if redistribute_with_full:
            geometry_from: st.GeoDataFrame = region_from.get_raw_geometry()
            geometry_to: st.GeoDataFrame = region_to.get_raw_geometry()
        else:
            geometry_from: st.GeoDataFrame = region_from.geometry
            geometry_to: st.GeoDataFrame = region_to.geometry

        match mapping_method:
            case "intersection_area":
                region_mapping = _create_intersection_area_mapping(geometry_from, geometry_to)
            case "centroid_distance":
                region_mapping = _create_centroid_distance_mapping(geometry_from, geometry_to)
            case _:
                raise ValueError(f"Unknown mapping method `{mapping_method}`")

        if save_data:
            create_path(mapping_file)
            region_mapping.write_parquet(mapping_file)
    else:
        logging.info("Reading region mapping.")
        region_mapping = pl.read_parquet(mapping_file)

    return region_mapping


def _create_intersection_area_mapping(
    geometry_from: st.GeoDataFrame,
    geometry_to: st.GeoDataFrame,
) -> pl.DataFrame:
    """Create mapping from one region to another based on intersection area.

    Returns
    -------
    pl.DataFrame:
    E.g.
    ```
    shape: (10, 3)
    ┌──────────┬──────────┬──────────┐
    │ region_a ┆ region_b ┆ mapping  │
    │ ---      ┆ ---      ┆ ---      │
    │ str      ┆ str      ┆ f64      │
    ╞══════════╪══════════╪══════════╡
    │ M        ┆ A        ┆ 0.25     │
    │ M        ┆ B        ┆ 0.75     │
    │ N        ┆ A        ┆ 0.25     │
    │ N        ┆ C        ┆ 0.75     │
    │ O        ┆ A        ┆ 0.75     │
    │ O        ┆ B        ┆ 0.25     │
    │ P        ┆ A        ┆ 0.75     │
    │ P        ┆ C        ┆ 0.25     │
    │ null     ┆ C        ┆ 1.0      │
    │ null     ┆ B        ┆ 1.0      │
    └──────────┴──────────┴──────────┘
    ```
    """
    logging.info("Joining geometries and finding intersection area.")

    geometry_combined = geometry_from.rename({"geometry": "geometry_from"}).join(
        geometry_to.rename({"geometry": "geometry_to"}), how="cross"
    )
    intersection_area = geometry_combined.select(
        pl.exclude("geometry_from", "geometry_to"),
        st.geom("geometry_from").st.intersection(st.geom("geometry_to")).st.area().alias("intersection_area"),
    )

    logging.info("Finding remaining areas.")
    remaining_area_for_from = _get_remaining_area(
        region_id=list(set(geometry_from.columns) - {"geometry"})[0],
        geometry=geometry_from,
        intersection_area=intersection_area,
    )
    remaining_area_for_to = _get_remaining_area(
        region_id=list(set(geometry_to.columns) - {"geometry"})[0],
        geometry=geometry_to,
        intersection_area=intersection_area,
    )

    intersection_area_complete = (
        pl.concat([intersection_area, remaining_area_for_from, remaining_area_for_to])
        .filter(pl.col("intersection_area") != 0)
        .rename({"intersection_area": "mapping"})
    )

    return intersection_area_complete


def _get_remaining_area(
    region_id: str,
    geometry: st.GeoDataFrame,
    intersection_area: pl.DataFrame,
) -> pl.DataFrame:
    """Find remaining area which hasnt been assigned to another region."""
    alt_region_id = list(set(intersection_area.columns) - {region_id, "intersection_area"})[0]

    intersected_area = intersection_area.group_by(region_id).agg(
        pl.col("intersection_area").sum().alias("intersected_area")
    )
    total_area = geometry.with_columns(
        st.geom("geometry").st.area().alias("total_area"),
        pl.lit(None).cast(intersection_area[alt_region_id].dtype).alias(alt_region_id),
    )
    remaining_area = (
        total_area.join(
            intersected_area,
            on=region_id,
        )
        .with_columns(
            pl.col("total_area").sub(pl.col("intersected_area")).clip(lower_bound=0).alias("intersection_area"),
        )
        .select(intersection_area.columns)
    )
    return remaining_area


def _create_centroid_distance_mapping(
    geometry_from: st.GeoDataFrame,
    geometry_to: st.GeoDataFrame,
) -> pl.DataFrame:
    """Create mapping from one region to another based on centroid distance."""
    raise NotImplementedError("This method is not implemented yet.")


def _get_region_mapping_file(
    region_from: RegionABC,
    region_to: RegionABC,
    *,
    mapping: MAPPING_OPTIONS,
) -> str:
    """Returns the path to the mapping file for the given region."""
    if region_from.id == region_to.id:
        regions = [region_from.id] * 2
    else:
        regions = list({region_from.id, region_to.id})

    mapping_file = region_from.redistribute_file.format(
        mapping=mapping,
        region_a=regions[0],
        region_b=regions[1],
    )
    return mapping_file
