import tempfile

import polars as pl
import pytest
from electoralyze import region
from electoralyze.region.redistribute.mapping import MAPPING_OPTIONS, get_region_mapping_base
from electoralyze.region.redistribute.redistribute import redistribute
from electoralyze.region.region_abc import RegionABC
from polars import testing  # noqa: F401


@pytest.mark.parametrize(
    "region_from, region_to, mapping_method",
    [
        (region.Federal2022, region.SA1_2021, "intersection_area"),
        (region.SA2_2021, region.SA1_2021, "intersection_area"),
    ],
)
def test_get_region_mapping_base(region_from: RegionABC, region_to: RegionABC, mapping_method: MAPPING_OPTIONS):
    """Test recreating region mapping and compares it to that stored."""
    with tempfile.TemporaryDirectory(delete=False) as temp_dir:
        #### Getting existing data ####

        mapping_original = get_region_mapping_base(
            region_from=region_from,
            region_to=region_to,
            mapping_method=mapping_method,
            redistribute_with_full=None,
            save_data=False,
            force_new=False,
        )

        root_dir_original = region_from._root_dir

        region_from.cache_clear()
        region_to.cache_clear()

        #### Making temp folders #####

        region_from._root_dir = temp_dir
        region_to._root_dir = temp_dir

        #### Re processing data ####

        mapping_new_generated = get_region_mapping_base(
            region_from=region_from,
            region_to=region_to,
            mapping_method=mapping_method,
            save_data=True,
            force_new=True,
            redistribute_with_full=True,
        )

        mapping_new_stored = get_region_mapping_base(
            region_from=region_from,
            region_to=region_to,
            mapping_method=mapping_method,
            save_data=False,
            force_new=False,
            redistribute_with_full=None,
        )

        #### Comparing ####

        pl.testing.assert_frame_equal(
            mapping_new_generated,
            mapping_new_stored,
            check_row_order=False,
            check_column_order=False,
        )

        pl.testing.assert_frame_equal(
            mapping_original,
            mapping_new_stored,
            check_row_order=False,
            check_column_order=False,
        )

        #### Resetting ####

        region_from._root_dir = root_dir_original
        region_to._root_dir = root_dir_original


@pytest.mark.parametrize(
    "sa_region, sa_subregion",
    [
        (region.SA2_2021, region.SA1_2021),
        # (region.SA3_2021, region.SA2_2021), FIXME: Add more SA 2021 regions, issue #21
        # (region.SA4_2021, region.SA3_2021), FIXME: Add more SA 2021 regions, issue #21
    ],
)
def test_SA_regions_are_subsets(sa_region: RegionABC, sa_subregion: RegionABC):
    """Test that all SA regions are subsets of their parent region."""
    mapping = get_region_mapping_base(
        region_from=sa_subregion,
        region_to=sa_region,
        mapping_method="intersection_area",
        redistribute_with_full=None,
    )
    non_subsets = mapping.drop_nulls().filter(
        ~pl.col(sa_subregion.id).cast(str).str.starts_with(pl.col(sa_region.id).cast(str))
    )
    assert len(non_subsets) == 0, "Mapping is not a subset."


@pytest.mark.parametrize(
    "_name, test_case",
    [
        (
            "SA1 to Federal: 1 pair, ",
            dict(
                region_from=region.SA1_2021,
                region_to=region.Federal2022,
                mapping_method="intersection_area",
                data=pl.DataFrame(
                    {
                        region.SA1_2021.id: [40106102213, 21003153801],
                        region.Federal2022.id: ["adelaide", "wills"],
                        "value": 1000.0,
                    },
                    schema=pl.Schema(
                        {
                            region.SA1_2021.id: pl.Int64,
                            region.Federal2022.id: pl.String,
                            "value": pl.Float64,
                        }
                    ),
                ),
            ),
        ),
        (
            "SA1 to Federal: 1 value per Federal, ",
            dict(
                region_from=region.SA1_2021,
                region_to=region.Federal2022,
                mapping_method="intersection_area",
                data=pl.DataFrame(
                    [
                        {region.Federal2022.id: "adelaide", region.SA1_2021.id: 40101100101, "value": 0.0},
                        {region.Federal2022.id: "aston", region.SA1_2021.id: 21101125104, "value": 1.0},
                        {region.Federal2022.id: "ballarat", region.SA1_2021.id: 20101100101, "value": 2.0},
                        {region.Federal2022.id: "banks", region.SA1_2021.id: 11901135802, "value": 3.0},
                        {region.Federal2022.id: "barker", region.SA1_2021.id: 40501111003, "value": 4.0},
                        {region.Federal2022.id: "barton", region.SA1_2021.id: 11702132808, "value": 5.0},
                        {region.Federal2022.id: "bass", region.SA1_2021.id: 60201103605, "value": 6.0},
                        {region.Federal2022.id: "bean", region.SA1_2021.id: 80103111302, "value": 7.0},
                        {region.Federal2022.id: "bendigo", region.SA1_2021.id: 20201101801, "value": 8.0},
                        {region.Federal2022.id: "bennelong", region.SA1_2021.id: 12502147709, "value": 9.0},
                        {region.Federal2022.id: "berowra", region.SA1_2021.id: 11501129401, "value": 10.0},
                        {region.Federal2022.id: "blair", region.SA1_2021.id: 31002127801, "value": 11.0},
                        {region.Federal2022.id: "blaxland", region.SA1_2021.id: 11901135401, "value": 12.0},
                        {region.Federal2022.id: "bonner", region.SA1_2021.id: 30101100204, "value": 13.0},
                        {region.Federal2022.id: "boothby", region.SA1_2021.id: 40107102331, "value": 14.0},
                        {region.Federal2022.id: "bowman", region.SA1_2021.id: 30101100102, "value": 15.0},
                        {region.Federal2022.id: "braddon", region.SA1_2021.id: 60401107501, "value": 16.0},
                        {region.Federal2022.id: "bradfield", region.SA1_2021.id: 12101139906, "value": 17.0},
                        {region.Federal2022.id: "brand", region.SA1_2021.id: 50703116904, "value": 18.0},
                        {region.Federal2022.id: "brisbane", region.SA1_2021.id: 30202103128, "value": 19.0},
                        {region.Federal2022.id: "bruce", region.SA1_2021.id: 21202129301, "value": 20.0},
                        {region.Federal2022.id: "burt", region.SA1_2021.id: 50601111003, "value": 21.0},
                        {region.Federal2022.id: "calare", region.SA1_2021.id: 10301105901, "value": 22.0},
                        {region.Federal2022.id: "calwell", region.SA1_2021.id: 21005124201, "value": 23.0},
                        {region.Federal2022.id: "canberra", region.SA1_2021.id: 80101100102, "value": 24.0},
                        {region.Federal2022.id: "canning", region.SA1_2021.id: 50102101601, "value": 25.0},
                        {region.Federal2022.id: "capricornia", region.SA1_2021.id: 30803120501, "value": 26.0},
                        {region.Federal2022.id: "casey", region.SA1_2021.id: 21102126220, "value": 27.0},
                        {region.Federal2022.id: "chifley", region.SA1_2021.id: 11601130406, "value": 28.0},
                        {region.Federal2022.id: "chisholm", region.SA1_2021.id: 20703116117, "value": 29.0},
                        {region.Federal2022.id: "clark", region.SA1_2021.id: 60103101306, "value": 30.0},
                        {region.Federal2022.id: "cook", region.SA1_2021.id: 11903137431, "value": 31.0},
                        {region.Federal2022.id: "cooper", region.SA1_2021.id: 20602111002, "value": 32.0},
                        {region.Federal2022.id: "corangamite", region.SA1_2021.id: 20301103403, "value": 33.0},
                        {region.Federal2022.id: "corio", region.SA1_2021.id: 20302103701, "value": 34.0},
                        {region.Federal2022.id: "cowan", region.SA1_2021.id: 50401104701, "value": 35.0},
                        {region.Federal2022.id: "cowper", region.SA1_2021.id: 10402108301, "value": 36.0},
                        {region.Federal2022.id: "cunningham", region.SA1_2021.id: 10701113402, "value": 37.0},
                        {region.Federal2022.id: "curtin", region.SA1_2021.id: 50301103003, "value": 38.0},
                        {region.Federal2022.id: "dawson", region.SA1_2021.id: 31201133703, "value": 39.0},
                        {region.Federal2022.id: "deakin", region.SA1_2021.id: 20703116123, "value": 40.0},
                        {region.Federal2022.id: "dickson", region.SA1_2021.id: 30201102304, "value": 41.0},
                        {region.Federal2022.id: "dobell", region.SA1_2021.id: 10201103114, "value": 42.0},
                        {region.Federal2022.id: "dunkley", region.SA1_2021.id: 21401137002, "value": 43.0},
                        {region.Federal2022.id: "durack", region.SA1_2021.id: 50403105801, "value": 44.0},
                        {region.Federal2022.id: "eden_monaro", region.SA1_2021.id: 10102100702, "value": 45.0},
                        {region.Federal2022.id: "fadden", region.SA1_2021.id: 30903123502, "value": 46.0},
                        {region.Federal2022.id: "fairfax", region.SA1_2021.id: 31601141304, "value": 47.0},
                        {region.Federal2022.id: "farrer", region.SA1_2021.id: 10901117201, "value": 48.0},
                        {region.Federal2022.id: "fenner", region.SA1_2021.id: 80101100201, "value": 49.0},
                        {region.Federal2022.id: "fisher", region.SA1_2021.id: 31601141604, "value": 50.0},
                        {region.Federal2022.id: "flinders", region.SA1_2021.id: 21402137702, "value": 51.0},
                        {region.Federal2022.id: "flynn", region.SA1_2021.id: 30801119001, "value": 52.0},
                        {region.Federal2022.id: "forde", region.SA1_2021.id: 30907155301, "value": 53.0},
                        {region.Federal2022.id: "forrest", region.SA1_2021.id: 50101100101, "value": 54.0},
                        {region.Federal2022.id: "fowler", region.SA1_2021.id: 12503148001, "value": 55.0},
                        {region.Federal2022.id: "franklin", region.SA1_2021.id: 60102100403, "value": 56.0},
                        {region.Federal2022.id: "fraser", region.SA1_2021.id: 21301132801, "value": 57.0},
                        {region.Federal2022.id: "fremantle", region.SA1_2021.id: 50701114806, "value": 58.0},
                        {region.Federal2022.id: "gellibrand", region.SA1_2021.id: 21302134101, "value": 59.0},
                        {region.Federal2022.id: "gilmore", region.SA1_2021.id: 10104101707, "value": 60.0},
                        {region.Federal2022.id: "gippsland", region.SA1_2021.id: 20502108109, "value": 61.0},
                        {region.Federal2022.id: "goldstein", region.SA1_2021.id: 20801116801, "value": 62.0},
                        {region.Federal2022.id: "gorton", region.SA1_2021.id: 21001122803, "value": 63.0},
                        {region.Federal2022.id: "grayndler", region.SA1_2021.id: 11702132701, "value": 64.0},
                        {region.Federal2022.id: "greenway", region.SA1_2021.id: 11601130302, "value": 65.0},
                        {region.Federal2022.id: "grey", region.SA1_2021.id: 40201102701, "value": 66.0},
                        {region.Federal2022.id: "griffith", region.SA1_2021.id: 30103101703, "value": 67.0},
                        {region.Federal2022.id: "groom", region.SA1_2021.id: 30702117916, "value": 68.0},
                        {region.Federal2022.id: "hasluck", region.SA1_2021.id: 50402104902, "value": 69.0},
                        {region.Federal2022.id: "hawke", region.SA1_2021.id: 20102100901, "value": 70.0},
                        {region.Federal2022.id: "herbert", region.SA1_2021.id: 31801146511, "value": 71.0},
                        {region.Federal2022.id: "higgins", region.SA1_2021.id: 20606113501, "value": 72.0},
                        {region.Federal2022.id: "hindmarsh", region.SA1_2021.id: 40401109002, "value": 73.0},
                        {region.Federal2022.id: "hinkler", region.SA1_2021.id: 31901149201, "value": 74.0},
                        {region.Federal2022.id: "holt", region.SA1_2021.id: 21203130002, "value": 75.0},
                        {region.Federal2022.id: "hotham", region.SA1_2021.id: 20802118001, "value": 76.0},
                        {region.Federal2022.id: "hughes", region.SA1_2021.id: 12703152305, "value": 77.0},
                        {region.Federal2022.id: "hume", region.SA1_2021.id: 10105153901, "value": 78.0},
                        {region.Federal2022.id: "hunter", region.SA1_2021.id: 10601110702, "value": 79.0},
                        {region.Federal2022.id: "indi", region.SA1_2021.id: 20401105403, "value": 80.0},
                        {region.Federal2022.id: "isaacs", region.SA1_2021.id: 20803118302, "value": 81.0},
                        {region.Federal2022.id: "jagajaga", region.SA1_2021.id: 20901119601, "value": 82.0},
                        {region.Federal2022.id: "kennedy", region.SA1_2021.id: 30602114402, "value": 83.0},
                        {region.Federal2022.id: "kingsford_smith", region.SA1_2021.id: 11701132003, "value": 84.0},
                        {region.Federal2022.id: "kingston", region.SA1_2021.id: 40302105904, "value": 85.0},
                        {region.Federal2022.id: "kooyong", region.SA1_2021.id: 20701114701, "value": 86.0},
                        {region.Federal2022.id: "la_trobe", region.SA1_2021.id: 21201128904, "value": 87.0},
                        {region.Federal2022.id: "lalor", region.SA1_2021.id: 21305136101, "value": 88.0},
                        {region.Federal2022.id: "leichhardt", region.SA1_2021.id: 30601113801, "value": 89.0},
                        {region.Federal2022.id: "lilley", region.SA1_2021.id: 30201102303, "value": 90.0},
                        {region.Federal2022.id: "lindsay", region.SA1_2021.id: 12403145701, "value": 91.0},
                        {region.Federal2022.id: "lingiari", region.SA1_2021.id: 70103103101, "value": 92.0},
                        {region.Federal2022.id: "longman", region.SA1_2021.id: 31301136202, "value": 93.0},
                        {region.Federal2022.id: "lyne", region.SA1_2021.id: 10601111002, "value": 94.0},
                        {region.Federal2022.id: "lyons", region.SA1_2021.id: 60101100111, "value": 95.0},
                        {region.Federal2022.id: "macarthur", region.SA1_2021.id: 12301169801, "value": 96.0},
                        {region.Federal2022.id: "mackellar", region.SA1_2021.id: 12202142008, "value": 97.0},
                        {region.Federal2022.id: "macnamara", region.SA1_2021.id: 20604111801, "value": 98.0},
                        {region.Federal2022.id: "macquarie", region.SA1_2021.id: 11503129901, "value": 99.0},
                        {region.Federal2022.id: "makin", region.SA1_2021.id: 40203103648, "value": 100.0},
                        {region.Federal2022.id: "mallee", region.SA1_2021.id: 20103101302, "value": 101.0},
                        {region.Federal2022.id: "maranoa", region.SA1_2021.id: 30701117102, "value": 102.0},
                        {region.Federal2022.id: "maribyrnong", region.SA1_2021.id: 20603111302, "value": 103.0},
                        {region.Federal2022.id: "mayo", region.SA1_2021.id: 40102100301, "value": 104.0},
                        {region.Federal2022.id: "mcewen", region.SA1_2021.id: 20202103101, "value": 105.0},
                        {region.Federal2022.id: "mcmahon", region.SA1_2021.id: 11603131804, "value": 106.0},
                        {region.Federal2022.id: "mcpherson", region.SA1_2021.id: 30901122501, "value": 107.0},
                        {region.Federal2022.id: "melbourne", region.SA1_2021.id: 20601110620, "value": 108.0},
                        {region.Federal2022.id: "menzies", region.SA1_2021.id: 20702115601, "value": 109.0},
                        {region.Federal2022.id: "mitchell", region.SA1_2021.id: 11501129001, "value": 110.0},
                        {region.Federal2022.id: "monash", region.SA1_2021.id: 20501107608, "value": 111.0},
                        {region.Federal2022.id: "moncrieff", region.SA1_2021.id: 30901122402, "value": 112.0},
                        {region.Federal2022.id: "moore", region.SA1_2021.id: 50501107001, "value": 113.0},
                        {region.Federal2022.id: "moreton", region.SA1_2021.id: 30302105206, "value": 114.0},
                        {region.Federal2022.id: "new_england", region.SA1_2021.id: 10604112805, "value": 115.0},
                        {region.Federal2022.id: "newcastle", region.SA1_2021.id: 11101120942, "value": 116.0},
                        {region.Federal2022.id: "nicholls", region.SA1_2021.id: 20401105606, "value": 117.0},
                        {region.Federal2022.id: "north_sydney", region.SA1_2021.id: 12101139901, "value": 118.0},
                        {region.Federal2022.id: "o_connor", region.SA1_2021.id: 50102100901, "value": 119.0},
                        {region.Federal2022.id: "oxley", region.SA1_2021.id: 30305107408, "value": 120.0},
                        {region.Federal2022.id: "page", region.SA1_2021.id: 10401108001, "value": 121.0},
                        {region.Federal2022.id: "parkes", region.SA1_2021.id: 10302106202, "value": 122.0},
                        {region.Federal2022.id: "parramatta", region.SA1_2021.id: 12502147705, "value": 123.0},
                        {region.Federal2022.id: "paterson", region.SA1_2021.id: 10601110912, "value": 124.0},
                        {region.Federal2022.id: "pearce", region.SA1_2021.id: 50503109901, "value": 125.0},
                        {region.Federal2022.id: "perth", region.SA1_2021.id: 50302103801, "value": 126.0},
                        {region.Federal2022.id: "petrie", region.SA1_2021.id: 30201102206, "value": 127.0},
                        {region.Federal2022.id: "rankin", region.SA1_2021.id: 30305107202, "value": 128.0},
                        {region.Federal2022.id: "reid", region.SA1_2021.id: 12001138302, "value": 129.0},
                        {region.Federal2022.id: "richmond", region.SA1_2021.id: 11201123603, "value": 130.0},
                        {region.Federal2022.id: "riverina", region.SA1_2021.id: 10106154301, "value": 131.0},
                        {region.Federal2022.id: "robertson", region.SA1_2021.id: 10201102802, "value": 132.0},
                        {region.Federal2022.id: "ryan", region.SA1_2021.id: 30402108603, "value": 133.0},
                        {region.Federal2022.id: "scullin", region.SA1_2021.id: 20904121601, "value": 134.0},
                        {region.Federal2022.id: "shortland", region.SA1_2021.id: 10202104506, "value": 135.0},
                        {region.Federal2022.id: "solomon", region.SA1_2021.id: 70101100101, "value": 136.0},
                        {region.Federal2022.id: "spence", region.SA1_2021.id: 40201102506, "value": 137.0},
                        {region.Federal2022.id: "sturt", region.SA1_2021.id: 40103101101, "value": 138.0},
                        {region.Federal2022.id: "swan", region.SA1_2021.id: 50602111801, "value": 139.0},
                        {region.Federal2022.id: "sydney", region.SA1_2021.id: 11703132901, "value": 140.0},
                        {region.Federal2022.id: "tangney", region.SA1_2021.id: 50603112401, "value": 141.0},
                        {region.Federal2022.id: "wannon", region.SA1_2021.id: 20103101402, "value": 142.0},
                        {region.Federal2022.id: "warringah", region.SA1_2021.id: 12104141305, "value": 143.0},
                        {region.Federal2022.id: "watson", region.SA1_2021.id: 11901157106, "value": 144.0},
                        {region.Federal2022.id: "wentworth", region.SA1_2021.id: 11703133305, "value": 145.0},
                        {region.Federal2022.id: "werriwa", region.SA1_2021.id: 12302170301, "value": 146.0},
                        {region.Federal2022.id: "whitlam", region.SA1_2021.id: 10701113103, "value": 147.0},
                        {region.Federal2022.id: "wide_bay", region.SA1_2021.id: 31605143401, "value": 148.0},
                        {region.Federal2022.id: "wills", region.SA1_2021.id: 20601110609, "value": 149.0},
                        {region.Federal2022.id: "wright", region.SA1_2021.id: 30904124101, "value": 150.0},
                    ],
                    schema=pl.Schema(
                        {
                            region.SA1_2021.id: pl.Int64,
                            region.Federal2022.id: pl.String,
                            "value": pl.Float64,
                        }
                    ),
                ),
            ),
        ),
    ],
)
def test_redistribute_with_data(_name: str, test_case: dict):
    """Test that all SA regions are subsets of their parent region."""
    redistributed_data = redistribute(
        test_case["data"].select(test_case["region_from"].id, "value"),
        region_from=test_case["region_from"],
        region_to=test_case["region_to"],
        mapping=test_case["mapping_method"],
    )

    pl.testing.assert_frame_equal(
        redistributed_data,
        test_case["data"].select(test_case["region_to"].id, "value"),
        check_row_order=False,
        check_column_order=False,
    )
