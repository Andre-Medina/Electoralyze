import os
import tempfile
import zipfile

import polars as pl
import pytest
from electoralyze.common.files import create_path, download_file, pl_scan_csv_zip
from polars import testing  # noqa: F401


@pytest.mark.parametrize(
    "_name, file_path",
    [
        ("Simple file, ", "file.txt"),
        ("Single directory, ", "dir1/file.txt"),
        ("Nested directories, ", "dir1/dir2/dir3/file.txt"),
        ("Path with dots, ", "version.1.0/file.txt"),
        ("Special characters, ", "special@dir/test#file.txt"),
        ("Spaces, ", "dir dir/test file.txt"),
        ("Everything, ", "dir1/dir2 . daf/dir3_dir3/dir4##/dir5/deep_file.txt"),
        ("No file, ", "dir1/dir2 . daf/dir3_dir3/dir4##/dir5/"),
    ],
)
def test_create_path(_name, file_path):
    """Test path creation with different path patterns."""
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, file_path)
        dir_path = os.path.dirname(file_path)

        create_path(file_path)

        assert os.path.exists(dir_path)
        assert os.path.isdir(dir_path)

        if not file_path.endswith("/"):
            assert not os.path.exists(file_path)

        # Test with multiple calls
        create_path(file_path)
        create_path(file_path)

        assert os.path.exists(dir_path)


@pytest.mark.parametrize(
    "invalid_input,expected_error",
    [
        (123, ValueError),
        ("", ValueError),
        ("test_dir/file.txt", ValueError),
        (None, ValueError),
    ],
)
def test_invalid_inputs(invalid_input, expected_error):
    """Test error handling for invalid inputs."""
    with pytest.raises(expected_error):
        create_path(invalid_input)


def test_download_file():
    """Test downloading a file from a URL."""
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, "test_file.geojson")
        url = "https://raw.githubusercontent.com/datasets/geo-boundaries-world-110m/master/countries.geojson"

        assert not os.path.exists(file_path), "file should not exist yet."

        download_file(url, file_path)
        assert os.path.exists(file_path), "file was not downloaded."

        # Testing force new

        time_initial = os.path.getmtime(file_path)

        download_file(url, file_path, force_new=False)
        time_redownload = os.path.getmtime(file_path)
        assert time_initial == time_redownload, "The data should not have changed."

        download_file(url, file_path, force_new=True)
        time_force_new = os.path.getmtime(file_path)
        assert time_initial != time_force_new, "The data should have changed."


def test_pl_scan_csv_zip():
    """Test several cases for `pl_scan_csv_zip` function."""
    schema = pl.Schema({"col1": pl.Int64, "col2": pl.Utf8})

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create valid CSVs and write to zip
        zip_path = os.path.join(temp_dir, "test.zip")
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file1.csv", "col1,col2\n1,a\n2,b")
            zf.writestr("file2.csv", "col1,col2\n3,c\n4,d")

        # Read without file_name column
        result = pl_scan_csv_zip(csv_zip_url=zip_path, schema=schema)
        read_data = result.collect()

        expected = pl.DataFrame(
            {
                "col1": [1, 2, 3, 4],
                "col2": ["a", "b", "c", "d"],
            }
        )
        pl.testing.assert_frame_equal(read_data, expected)

        # Read with file_name column
        result_with_name = pl_scan_csv_zip(csv_zip_url=zip_path, schema=schema, add_file_name=True)
        read_data_with_name = result_with_name.collect()

        expected_with_name = pl.DataFrame(
            {
                "col1": [1, 2, 3, 4],
                "col2": ["a", "b", "c", "d"],
                "file_name": ["file1.csv", "file1.csv", "file2.csv", "file2.csv"],
            }
        )
        pl.testing.assert_frame_equal(read_data_with_name, expected_with_name)

        # Add a non-CSV file and expect ValueError
        with zipfile.ZipFile(zip_path, "a") as zf:
            zf.writestr("not_a_csv.txt", "some text content")

        with pytest.raises(ValueError, match="Found non-CSV file in zip."):
            _ = pl_scan_csv_zip(csv_zip_url=zip_path, schema=schema)
