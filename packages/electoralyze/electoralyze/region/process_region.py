import polars_st as st
from abc import ABC, abstractmethod
import pyogrio

from electoralyze.common.geometry import to_st_gdf

class ProcessRegion(ABC):


    @classmethod
    def extract(cls, raw_geometry_file: str) -> st.GeoDataFrame:
        """Extract raw data from the raw shape file."""
        extracted_shape_gpd = pyogrio.read_dataframe(raw_geometry_file)
        extracted_shape_st = to_st_gdf(extracted_shape_gpd)
        return extracted_shape_st

    
    @abstractmethod
    @classmethod
    def transform(cls, extracted_shape: st.GeoDataFrame) -> st.GeoDataFrame:
        """Transform data from raw shape.
        
        Returns 
        -------

        """
        pass

    @classmethod
    def save(
        cls,
        transformed_shape: st.GeoDataFrame, 
        *, 
        metadata_file: str, 
        geometry_file: str,
    ):
        """Saves the processed data locally."""
        pass