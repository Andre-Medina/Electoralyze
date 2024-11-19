import os
import importlib
from enum import Enum
from electoralyze.common.constants import ELECTORALYZE_DIR, ROOT_DIR

from typing import Callable

REDISTRIBUTE_FILE = os.path.join(ELECTORALYZE_DIR, "region/regions/{from}/redistribute/{to}.parquet")
GEOMETRY_SHAPE_FILE = os.path.join(ELECTORALYZE_DIR, "region/regions/{region}/geometry/{region}.shp")
METADATA_FILE = os.path.join(ELECTORALYZE_DIR, "region/regions/{region}/metadata.parquet")

REGION_MODULE = "electoralyze.region.regions.{region}"

class Region(str, Enum):
    """Regions."""

    SA1_2021 = "SA1_2021"

    @property
    def name(self) -> str:
        """Returns the corresponding name column.
        
        Example
        -------
        `data.select(Region.SA1_2021.name)`        
        """
        name_column = f"{self.value}_name"
        return name_column

    @property
    def raw_data_file(self) -> str:
        """Get the path to the raw data shapefile."""
        raw_data_file = RAW_DATA_FILE[self]
        return raw_data_file
    
    def process_raw(self):

        region_module = importlib.import_module(REGION_MODULE.format(region = self.value))
        ProcessRegion: Callable = getattr(region_module, self.value) 

        # ProcessRegion





RAW_DATA_FILE = {
    Region.SA1_2021: os.path.join(ROOT_DIR, "data/raw/ASGA/2021/SA1/SA1_2021_AUST_GDA2020.shp"),
}
