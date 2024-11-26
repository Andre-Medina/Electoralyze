import pytest
from electoralyze import region


def test_from_id_valid():
    """Test valid region IDs."""
    assert region.from_id("SA1_2021") is region.SA1_2021
    assert region.from_id("SA2_2021") is region.SA2_2021

    with pytest.raises(ValueError):
        region.from_id("INVALID_REGION")
