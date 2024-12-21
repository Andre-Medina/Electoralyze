import os
import tempfile

import pytest
from electoralyze.common.files import create_path


@pytest.mark.parametrize(
    "file_path",
    [
        "file.txt",  # Simple file
        "dir1/file.txt",  # Single directory
        "dir1/dir2/dir3/file.txt",  # Nested directories
        "version.1.0/file.txt",  # Path with dots
        "special@dir/test#file.txt",  # Special characters
        "dir1/dir2/dir3/dir4/dir5/deep_file.txt",  # Very deep nesting
    ],
)
def test_create_path(file_path):
    """Test path creation with different path patterns."""
    with tempfile.TemporaryDirectory() as temp_dir:
        full_path = os.path.join(temp_dir, file_path)
        dir_path = os.path.dirname(full_path)

        create_path(full_path)

        assert os.path.exists(dir_path)
        assert os.path.isdir(dir_path)

        # Test with multiple calls
        create_path(full_path)
        create_path(full_path)

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
