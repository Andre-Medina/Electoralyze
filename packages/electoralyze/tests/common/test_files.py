import os
import tempfile

import pytest
from electoralyze.common.files import create_path, download_file


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

        download_file(url, file_path)

        assert os.path.exists(file_path)
