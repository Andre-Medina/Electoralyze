"""GeoDataFrame specific utility functions."""
import polars_st as st
import geopandas as gpd

from .constants import COORDINATE_REFERENCE_SYSTEM

def to_st_gdf(gdp_gdf: gpd.GeoDataFrame) -> st.GeoDataFrame:
    """Convert a GeoPandas dataframe into a Polars ST dataframe."""
    st_gdf = (
        st.from_geopandas(gdp_gdf.to_crs(COORDINATE_REFERENCE_SYSTEM))
        .with_columns(st.geom("geometry").st.set_srid(COORDINATE_REFERENCE_SYSTEM))
    )
    return st_gdf

def to_gpd_gdf(st_gdf: st.GeoDataFrame) -> gpd.GeoDataFrame:
    """Convert a Polars ST dataframe into a GeoPandas dataframe."""
    gpd_gdf = st_gdf.st.to_geopandas().set_crs(COORDINATE_REFERENCE_SYSTEM)
    return gpd_gdf