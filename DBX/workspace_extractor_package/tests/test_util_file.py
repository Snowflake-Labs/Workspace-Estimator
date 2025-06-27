from datetime import datetime
import os
import shutil
import tempfile
from typing import Dict, Generator, List
from unittest.mock import patch
from unittest.mock import mock_open, patch
import zipfile

import pytest
import pytest

from workspace_extractor.utils.util_file import UtilFile

class TestGetFileName:
    @pytest.mark.parametrize(
        "workspace_input, expected_prefix",
        [
            ("MyWorkspace", "MyWorkspace"),
            ("My Workspace", "MyWorkspace"),
            ("My_Workspace", "MyWorkspace"),
            ("Workspace@123!", "Workspace123"),
            ("", "Default_wkp"),
            ("  LeadingTrailingSpaces  ", "LeadingTrailingSpaces"),
            ("!@#$%^&*()", "Default_wkp"),
            ("ValidName123", "ValidName123"),
        ],
    )
    @patch("workspace_extractor.utils.util_file.datetime")
    def test_get_file_name(self, mock_datetime, workspace_input: str, expected_prefix: str) -> None:
        mock_now = datetime(2023, 10, 26)
        mock_datetime.now.return_value = mock_now
        expected_date_part = mock_now.strftime("%m%d")

        result = UtilFile.get_file_name(workspace_input)

        assert result == f"{expected_prefix}_{expected_date_part}"
        mock_datetime.now.assert_called_once()

    @patch("workspace_extractor.utils.util_file.datetime")
    def test_get_file_name_empty_after_cleaning(self, mock_datetime) -> None:
        mock_now = datetime(2023, 1, 5)
        mock_datetime.now.return_value = mock_now
        expected_date_part = mock_now.strftime("%m%d")
        workspace_input = "!@#$"

        result = UtilFile.get_file_name(workspace_input)

        assert result == f"Default_wkp_{expected_date_part}"
        mock_datetime.now.assert_called_once()

    @patch("workspace_extractor.utils.util_file.datetime")
    def test_get_file_name_with_internal_spaces_and_special_chars(self, mock_datetime) -> None:
        mock_now = datetime(2024, 12, 31)
        mock_datetime.now.return_value = mock_now
        expected_date_part = mock_now.strftime("%m%d")
        workspace_input = "My Test Workspace @ V2!"

        result = UtilFile.get_file_name(workspace_input)

        assert result == f"MyTestWorkspaceV2_{expected_date_part}"
        mock_datetime.now.assert_called_once()

class TestZipFileOperations:
    """Test suite for zip file operations in UtilFile class."""

    @pytest.fixture
    def temp_dir(self) -> Generator[str, None, None]:
        """Create a temporary directory for testing."""
        temp_dir = tempfile.mkdtemp()
        yield temp_dir
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)

    @pytest.fixture
    def sample_folder(self, temp_dir: str) -> str:
        """Create a sample folder with test files."""
        folder_path = self._create_sub_dir(temp_dir)
        
        test_files = {
            "file1.txt": "This is test file 1 content.",
            "file2.json": '{"key": "value", "number": 42}',
            "subdir/nested_file.txt": "Nested file content for testing.",
            "empty_file.txt": "",
            "large_file.txt": "x" * 1000  # 1KB file
        }
        
        return self._fill_temp_files(test_files, folder_path)

    @pytest.fixture
    def sample_large_folder(self, temp_dir: str) -> str:
        """Create a sample folder with large test files."""
        folder_path = self._create_sub_dir(temp_dir)

        import random
        import string
        
        # Generate random data that compresses poorly
        def generate_random_data(size: int) -> str:
            return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation, k=size))
        
        # Generate pseudo-random binary-like data
        def generate_binary_like_data(size: int) -> str:
            return ''.join([chr(random.randint(0, 255)) for _ in range(size)])
        
        test_files = {
            "random_file1.txt": generate_random_data(800000),
            "random_file2.txt": generate_random_data(600000),
            "random_file3.txt": generate_random_data(500000),
            
            "binary_file1.dat": generate_binary_like_data(400000),
            "binary_file2.dat": generate_binary_like_data(300000),
            
            "large_json.json": '{"data": [' + ','.join([f'{{"id": {i}, "value": "{generate_random_data(50)}"}}' for i in range(5000)]) + ']}',
            
            "large_csv.csv": '\n'.join([f"{i},{generate_random_data(100)},{random.randint(1, 1000)}" for i in range(10000)]),
            
            "subdir/nested_random1.txt": generate_random_data(400000),
            "subdir/nested_random2.txt": generate_random_data(300000),
            "subdir/deep/nested_random3.txt": generate_random_data(200000),
            
            "empty_file.txt": "",
            "small_file.txt": "Small file content for testing.",
        }
        
        return self._fill_temp_files(test_files, folder_path)

    @pytest.fixture
    def sample_zip_file(self, temp_dir: str) -> str:
        """Create a sample zip file for testing."""
        zip_path = os.path.join(temp_dir, "test_archive.zip")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            zipf.writestr("test1.txt", "Content of test file 1" * 100)
            zipf.writestr("test2.txt", "Content of test file 2" * 200)
            zipf.writestr("folder/nested.txt", "Nested content" * 50)
        return zip_path
    
    def _fill_temp_files(self, test_files: dict, folder_path: str) -> str:
        """Fill the temp folder with the test files."""
        for file_path, content in test_files.items():
            full_path = os.path.join(folder_path, file_path)
            os.makedirs(os.path.dirname(full_path), exist_ok=True)
            with open(full_path, "w") as f:
                f.write(content)
        
        return folder_path

    def _create_sub_dir(self, temp_dir: str) -> str:
        folder_path = os.path.join(temp_dir, "test_folder")
        os.makedirs(folder_path)

        sub_dir = os.path.join(folder_path, "subdir")
        os.makedirs(sub_dir)

        return folder_path


class TestGetPathSeparated(TestZipFileOperations):
    """Tests for get_path_separated method."""

    @pytest.mark.parametrize(
        "file_path, expected",
        [
            (
                "/home/user/documents/file.txt",
                ("/home/user/documents", "file.txt", "file", ".txt")
            ),
            (
                "simple_file.json",
                ("", "simple_file.json", "simple_file", ".json")
            ),
            (
                "/path/to/file_without_extension",
                ("/path/to", "file_without_extension", "file_without_extension", "")
            ),
            (
                "/path/to/.hidden_file",
                ("/path/to", ".hidden_file", ".hidden_file", "")
            ),
            (
                "file.with.multiple.dots.txt",
                ("", "file.with.multiple.dots.txt", "file.with.multiple.dots", ".txt")
            ),
            (
                "/root/",
                ("/root", "", "", "")
            ),
        ],
    )
    def test_get_path_separated_various_paths(self, file_path: str, expected: tuple) -> None:
        """Test get_path_separated with various file path formats."""
        result = UtilFile.get_path_separated(file_path)
        assert result == expected

    def test_get_path_separated_empty_string(self) -> None:
        """Test get_path_separated with empty string."""
        result = UtilFile.get_path_separated("")
        assert result == ("", "", "", "")

    def test_get_path_separated_return_type(self) -> None:
        """Test that get_path_separated returns correct tuple type."""
        result = UtilFile.get_path_separated("/path/to/file.txt")
        assert isinstance(result, tuple)
        assert len(result) == 4
        assert all(isinstance(item, str) for item in result)


class TestCompressFolderToZip(TestZipFileOperations):
    """Tests for compress_folder_to_zip method."""

    def test_compress_folder_to_zip_basic(self, sample_folder: str, temp_dir: str) -> None:
        """Test basic folder compression functionality."""
        output_path = os.path.join(temp_dir, "compressed_archive")
        
        result = UtilFile.compress_folder_to_zip(sample_folder, output_path)
        
        assert result is not None
        assert result.endswith(".zip")
        assert os.path.exists(result)
        
        with zipfile.ZipFile(result, "r") as zipf:
            file_list = zipf.namelist()
            assert "file1.txt" in file_list
            assert "file2.json" in file_list
            assert "subdir/nested_file.txt" in file_list
            assert "empty_file.txt" in file_list
            assert "large_file.txt" in file_list

    def test_compress_folder_to_zip_custom_extension(self, sample_folder: str, temp_dir: str) -> None:
        """Test folder compression with custom extension."""
        output_path = os.path.join(temp_dir, "custom_archive")
        
        result = UtilFile.compress_folder_to_zip(
            sample_folder, output_path, extension="custom"
        )
        
        assert result is not None
        assert result.endswith(".custom")
        assert os.path.exists(result)

    def test_compress_folder_to_zip_nonexistent_folder(self, temp_dir: str) -> None:
        """Test compression of non-existent folder."""
        nonexistent_folder = os.path.join(temp_dir, "nonexistent")
        output_path = os.path.join(temp_dir, "output")
        
        result = UtilFile.compress_folder_to_zip(nonexistent_folder, output_path)
        
        assert result is None

    @patch('workspace_extractor.utils.util_file.UtilFile.split_zip_file')
    def test_compress_folder_to_zip_triggers_split(self, mock_split, sample_large_folder: str, temp_dir: str) -> None:
        """Test that large zip files trigger splitting."""
        output_path = os.path.join(temp_dir, "large_archive")
        mock_split.return_value = "split_result.zip"
        
        result = UtilFile.compress_folder_to_zip(
            sample_large_folder, output_path, split_size_mb=1
        )
        
        mock_split.assert_called_once()
        assert result == "split_result.zip"

    @patch('builtins.print')
    @patch('workspace_extractor.utils.util_file.zipfile.ZipFile')
    def test_compress_folder_to_zip_handles_exception(self, mock_zipfile, mock_print, sample_folder: str, temp_dir: str) -> None:
        """Test error handling in compress_folder_to_zip."""
        mock_zipfile.side_effect = Exception("Compression error")
        output_path = os.path.join(temp_dir, "error_archive")
        
        result = UtilFile.compress_folder_to_zip(sample_folder, output_path)
        
        assert result is None
        mock_print.assert_called_once()
        assert "Error:" in mock_print.call_args[0][0]


class TestSplitZipFile(TestZipFileOperations):
    """Tests for split_zip_file method."""

    def test_split_zip_file_basic(self, temp_dir: str) -> None:
        """Test basic zip file splitting functionality."""
        large_zip_path = os.path.join(temp_dir, "large_archive.zip")
        with zipfile.ZipFile(large_zip_path, "w", zipfile.ZIP_DEFLATED) as zipf:
            for i in range(10):
                zipf.writestr(f"large_file_{i}.txt", "x" * 50000)
        
        result = UtilFile.split_zip_file(large_zip_path, part_size_mb=1)
        
        assert result is not None
        assert result.startswith(os.path.dirname(large_zip_path))
        assert "parts.zip" in result
        assert os.path.exists(result)

    def test_split_zip_file_invalid_extension(self, temp_dir: str) -> None:
        """Test split_zip_file with non-zip file."""
        non_zip_file = os.path.join(temp_dir, "not_a_zip.txt")
        with open(non_zip_file, "w") as f:
            f.write("This is not a zip file")
        
        with pytest.raises(ValueError, match="is not a zip file"):
            UtilFile.split_zip_file(non_zip_file)

    def test_split_zip_file_case_insensitive_extension(self, temp_dir: str) -> None:
        """Test that split_zip_file accepts .ZIP extension (case insensitive)."""
        zip_file = os.path.join(temp_dir, "test.ZIP")
        with zipfile.ZipFile(zip_file, "w") as zipf:
            zipf.writestr("test.txt", "content")
        
        result = UtilFile.split_zip_file(zip_file)
        assert result is not None

    @patch('workspace_extractor.utils.util_file.UtilFile.rezip_zip_parts')
    @patch('builtins.open', side_effect=IOError("Cannot read file"))
    def test_split_zip_file_handles_exception(self, mock_open, mock_rezip, sample_zip_file: str) -> None:
        """Test error handling in split_zip_file."""
        result = UtilFile.split_zip_file(sample_zip_file)
        
        assert result is None
        mock_rezip.assert_not_called()


class TestRezipZipParts(TestZipFileOperations):
    """Tests for rezip_zip_parts method."""

    def create_zip_parts(self, base_zip_path: str, num_parts: int) -> List[str]:
        """Helper method to create zip parts for testing."""
        base_dir = os.path.dirname(base_zip_path)
        base_name = os.path.basename(base_zip_path)
        
        part_files = []
        for i in range(1, num_parts + 1):
            part_path = f"{base_zip_path}.{i:03d}"
            with open(part_path, "wb") as f:
                f.write(f"Part {i} content".encode() * 100)
            part_files.append(part_path)
        
        return part_files

    def test_rezip_zip_parts_basic(self, temp_dir: str) -> None:
        """Test basic rezip_zip_parts functionality."""
        base_zip_path = os.path.join(temp_dir, "archive.zip")
        num_parts = 3
        
        part_files = self.create_zip_parts(base_zip_path, num_parts)
        
        result = UtilFile.rezip_zip_parts(base_zip_path, num_parts + 1)
        
        assert result is not None
        assert os.path.exists(result)
        assert "we_archive_data_4_parts.zip" in result
        
        for part_file in part_files:
            assert not os.path.exists(part_file)
        
        with zipfile.ZipFile(result, "r") as zipf:
            file_list = zipf.namelist()
            assert len(file_list) == num_parts

    def test_rezip_zip_parts_with_subdirectory(self, temp_dir: str) -> None:
        """Test rezip_zip_parts with zip file in subdirectory."""
        sub_dir = os.path.join(temp_dir, "subdir")
        os.makedirs(sub_dir)
        base_zip_path = os.path.join(sub_dir, "nested_archive.zip")
        num_parts = 2
        
        self.create_zip_parts(base_zip_path, num_parts)
        
        result = UtilFile.rezip_zip_parts(base_zip_path, num_parts + 1)
        
        assert result is not None
        assert os.path.exists(result)
        assert "we_nested_archive_data_3_parts.zip" in result
        assert result.startswith(sub_dir)

    def test_rezip_zip_parts_no_matching_parts(self, temp_dir: str) -> None:
        """Test rezip_zip_parts when no matching parts exist."""
        base_zip_path = os.path.join(temp_dir, "nonexistent.zip")
        
        result = UtilFile.rezip_zip_parts(base_zip_path, 3)
        
        assert result is not None
        assert os.path.exists(result)
        
        with zipfile.ZipFile(result, "r") as zipf:
            assert len(zipf.namelist()) == 0

    def test_rezip_zip_parts_filename_with_dots(self, temp_dir: str) -> None:
        """Test rezip_zip_parts with filename containing dots."""
        base_zip_path = os.path.join(temp_dir, "file.with.dots.zip")
        num_parts = 2
        
        self.create_zip_parts(base_zip_path, num_parts)
        
        result = UtilFile.rezip_zip_parts(base_zip_path, num_parts + 1)
        
        assert result is not None
        assert "we_file.with.dots_data_3_parts.zip" in result


class TestIntegrationZipOperations(TestZipFileOperations):
    """Integration tests for zip operations working together."""

    def test_full_zip_workflow(self, sample_large_folder: str, temp_dir: str) -> None:
        """Test complete workflow: compress -> split -> rezip."""
        output_path = os.path.join(temp_dir, "workflow_test")
        

        result1 = UtilFile.compress_folder_to_zip(
            sample_large_folder, output_path, split_size_mb=1
        )
        
        assert result1 is not None
        assert os.path.exists(result1)
        assert "parts.zip" in result1

    def test_compress_and_verify_contents(self, sample_folder: str, temp_dir: str) -> None:
        """Test that compressed files maintain their content integrity."""
        output_path = os.path.join(temp_dir, "integrity_test")
        
        result = UtilFile.compress_folder_to_zip(sample_folder, output_path)
        
        assert result is not None
        
        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir)
        
        with zipfile.ZipFile(result, "r") as zipf:
            zipf.extractall(extract_dir)
        
        with open(os.path.join(extract_dir, "file1.txt"), "r") as f:
            content = f.read()
            assert content == "This is test file 1 content."
        
        with open(os.path.join(extract_dir, "subdir", "nested_file.txt"), "r") as f:
            content = f.read()
            assert content == "Nested file content for testing."

    @pytest.mark.parametrize("split_size_mb", [0.001, 0.01, 0.1])
    def test_different_split_sizes(self, sample_folder: str, temp_dir: str, split_size_mb: float) -> None:
        """Test compression with different split sizes."""
        output_path = os.path.join(temp_dir, f"split_test_{split_size_mb}")
        
        result = UtilFile.compress_folder_to_zip(
            sample_folder, output_path, split_size_mb=split_size_mb
        )
        
        assert result is not None
        assert os.path.exists(result) 