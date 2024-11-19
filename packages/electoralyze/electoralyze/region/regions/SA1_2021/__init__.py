from electoralyze.region.process_region import ProcessRegion
import polars_st as st

class SA1_2021(ProcessRegion):

    @classmethod
    def transform(cls, extracted_shape: st.GeoDataFrame) -> st.GeoDataFrame:
        pass