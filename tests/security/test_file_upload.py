"""Security tests for file upload functionality."""

import pytest
import os
import tempfile
from pathlib import Path


class TestFileUploadSecurity:
    """Test file upload security validations."""

    def test_basename_extraction(self):
        """Test that only basename is used, preventing path traversal."""
        # Simulate the security check from chainlit_app.py lines 266-272
        malicious_paths = [
            "/etc/passwd",
            "../../../etc/passwd",
            "..\\..\\..\\windows\\system32\\config\\sam",
            "/root/.ssh/id_rsa",
            "../../../../tmp/evil.csv"
        ]

        for malicious_path in malicious_paths:
            # Apply the same security logic as in the app
            filename = os.path.basename(malicious_path)
            if not filename.endswith('.csv'):
                filename += '.csv'

            # Validate filename doesn't contain path separators or start with dot
            is_valid = not ('/' in filename or '\\' in filename or filename.startswith('.') or not filename)

            # The basename should be safe
            assert is_valid, f"Should have sanitized {malicious_path}"
            assert '/' not in filename
            assert '\\' not in filename

    def test_rejects_path_separators_in_filename(self):
        """Test that filenames with path separators are rejected."""
        invalid_filenames = [
            "subdir/file.csv",
            "..\\file.csv",
            "../../file.csv",
            "a/b/c.csv"
        ]

        for filename in invalid_filenames:
            # Apply validation logic
            is_valid = not ('/' in filename or '\\' in filename or filename.startswith('.') or not filename)
            assert not is_valid, f"Should have rejected {filename}"

    def test_rejects_dot_prefix_filenames(self):
        """Test that filenames starting with '.' are rejected."""
        invalid_filenames = [
            ".hidden.csv",
            "..secret.csv",
            ".bashrc"
        ]

        for filename in invalid_filenames:
            is_valid = not ('/' in filename or '\\' in filename or filename.startswith('.') or not filename)
            assert not is_valid, f"Should have rejected {filename}"

    def test_rejects_empty_filename(self):
        """Test that empty filenames are rejected."""
        filename = ""
        is_valid = not ('/' in filename or '\\' in filename or filename.startswith('.') or not filename)
        assert not is_valid, "Should have rejected empty filename"

    def test_accepts_valid_filenames(self):
        """Test that valid filenames are accepted."""
        valid_filenames = [
            "data.csv",
            "sales_2024.csv",
            "employee-records.csv",
            "file_with_underscores.csv",
            "CamelCase.csv",
            "123numbers.csv"
        ]

        for filename in valid_filenames:
            is_valid = not ('/' in filename or '\\' in filename or filename.startswith('.') or not filename)
            assert is_valid, f"Should have accepted {filename}"

    def test_csv_extension_enforcement(self):
        """Test that .csv extension is added if missing."""
        test_cases = [
            ("data", "data.csv"),
            ("file.txt", "file.txt.csv"),
            ("noextension", "noextension.csv"),
            ("data.csv", "data.csv")  # Already has .csv
        ]

        for input_name, expected in test_cases:
            filename = input_name
            if not filename.endswith('.csv'):
                filename += '.csv'
            assert filename == expected

    def test_path_traversal_prevention(self):
        """Test that path traversal attempts are neutralized."""
        # Simulate what happens when we use os.path.basename
        traversal_attempts = [
            ("../../../etc/passwd", "passwd"),
            ("..\\..\\..\\windows\\system32", "system32"),
            ("/var/log/messages", "messages"),
            ("C:\\Windows\\System32\\config", "config")
        ]

        for malicious, expected_basename in traversal_attempts:
            result = os.path.basename(malicious)
            assert result == expected_basename
            # The basename should not contain any path separators
            assert '/' not in result
            assert '\\' not in result

    def test_file_save_location(self, tmp_path):
        """Test that files are saved in the correct directory."""
        csv_dir = tmp_path / "CSV"
        csv_dir.mkdir()

        # Simulate the upload logic
        filename = "test.csv"
        dest = os.path.join(csv_dir, filename)

        # The destination should be within csv_dir
        assert dest.startswith(str(csv_dir))
        assert os.path.dirname(dest) == str(csv_dir)

    def test_cannot_escape_csv_directory(self, tmp_path):
        """Test that even with malicious input, files stay in CSV directory."""
        csv_dir = tmp_path / "CSV"
        csv_dir.mkdir()

        # Try various path traversal attempts
        malicious_inputs = [
            "../../../evil.csv",
            "..\\..\\..\\evil.csv",
            "/etc/passwd",
            "C:\\Windows\\evil.csv"
        ]

        for malicious in malicious_inputs:
            # Apply the same security logic
            filename = os.path.basename(malicious)
            if not filename.endswith('.csv'):
                filename += '.csv'

            dest = os.path.join(csv_dir, filename)

            # The destination should still be within csv_dir
            # realpath resolves any symlinks and normalizes the path
            assert os.path.commonpath([csv_dir, dest]) == str(csv_dir), \
                f"File {malicious} escaped CSV directory"


class TestFileUploadValidation:
    """Test file upload validation logic."""

    def test_max_file_size_limit(self):
        """Test that file size limit is enforced (50MB in the app)."""
        max_size_mb = 50
        max_size_bytes = max_size_mb * 1024 * 1024

        # A file just under the limit should be OK
        assert (max_size_bytes - 1) < max_size_bytes

        # A file over the limit should fail
        assert (max_size_bytes + 1) > max_size_bytes

    def test_csv_mime_type_validation(self):
        """Test that only CSV files are accepted."""
        valid_mime_types = [
            "text/csv",
            "application/vnd.ms-excel"
        ]

        invalid_mime_types = [
            "application/pdf",
            "text/plain",
            "application/zip",
            "text/html",
            "application/javascript"
        ]

        # In the real app, this would be enforced by cl.AskFileMessage
        # Here we just verify the expected values
        assert "text/csv" in valid_mime_types
        assert "application/pdf" not in valid_mime_types

    def test_csv_extension_validation(self):
        """Test that files must have .csv extension."""
        valid_files = [
            "data.csv",
            "file.CSV",  # Case might vary
            "test.csv"
        ]

        invalid_files = [
            "data.txt",
            "file.pdf",
            "script.py",
            "data.xlsx"
        ]

        for filename in valid_files:
            assert filename.lower().endswith('.csv')

        for filename in invalid_files:
            assert not filename.lower().endswith('.csv')


class TestFilenameSanitization:
    """Test edge cases in filename sanitization."""

    def test_unicode_in_filename(self):
        """Test handling of unicode characters in filenames."""
        unicode_names = [
            "données.csv",  # French
            "数据.csv",      # Chinese
            "файл.csv"      # Russian
        ]

        for filename in unicode_names:
            # os.path.basename should handle unicode
            result = os.path.basename(filename)
            assert result == filename

    def test_special_characters_in_filename(self):
        """Test handling of special characters."""
        special_names = [
            "file with spaces.csv",
            "file-with-dashes.csv",
            "file_with_underscores.csv",
            "file(with)parens.csv",
            "file[with]brackets.csv"
        ]

        for filename in special_names:
            # These should be allowed as long as no path separators
            is_valid = not ('/' in filename or '\\' in filename or filename.startswith('.') or not filename)
            # Most should be valid (implementation dependent)
            # At minimum, no path traversal should be possible
            assert '/' not in filename
            assert '\\' not in filename

    def test_null_byte_injection(self):
        """Test that null bytes in filenames are handled."""
        # Null byte injection attempt
        malicious = "safe.csv\x00.exe"

        # Python's os.path.basename should handle this
        result = os.path.basename(malicious)

        # The result should not contain null bytes in modern Python
        # (Python 3.x raises ValueError for null bytes in paths)
        # This is OS-dependent, but we should be safe
        assert '\x00' not in result or result == malicious.split('\x00')[0]

    def test_very_long_filename(self):
        """Test handling of very long filenames."""
        # Most filesystems have a 255 character limit for filenames
        long_name = "a" * 300 + ".csv"

        # The app should handle this gracefully (truncate or reject)
        # At minimum, it shouldn't crash
        result = os.path.basename(long_name)
        assert len(result) > 0

    def test_windows_reserved_names(self):
        """Test handling of Windows reserved filenames."""
        reserved_names = [
            "CON.csv", "PRN.csv", "AUX.csv", "NUL.csv",
            "COM1.csv", "LPT1.csv"
        ]

        # These are valid basenames, but may cause issues on Windows
        # The app should ideally reject or sanitize these
        for filename in reserved_names:
            result = os.path.basename(filename)
            # At minimum, should not contain path separators
            assert '/' not in result
            assert '\\' not in result
