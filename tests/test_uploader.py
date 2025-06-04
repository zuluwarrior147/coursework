"""
Tests for scripts.uploader module

This module contains tests for the GCSUploader class and upload_all_datasets 
functionality with mocked Google Cloud Storage operations.
"""
import pytest
import os
import tempfile
import shutil
from unittest.mock import Mock, patch, MagicMock
from google.cloud.exceptions import GoogleCloudError

from scripts.uploader import GCSUploader, upload_all_datasets


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_files(temp_dir):
    """Create sample dataset files for testing"""
    files = {}
    dataset_files = ["directors.csv", "crew.csv", "basic_titles.csv", "ratings.csv"]
    
    for filename in dataset_files:
        file_path = os.path.join(temp_dir, filename)
        with open(file_path, 'w') as f:
            f.write(f"test_data_for_{filename}\n")
        files[filename] = file_path
    
    return files


@pytest.fixture
def mock_gcs_client():
    """Create a mock GCS client"""
    with patch('scripts.uploader.storage.Client') as mock_client_class:
        mock_client = Mock()
        mock_bucket = Mock()
        mock_blob = Mock()
        
        mock_client_class.return_value = mock_client
        mock_client.bucket.return_value = mock_bucket
        mock_bucket.blob.return_value = mock_blob
        mock_client.list_blobs.return_value = [Mock(name="test-file.csv")]
        
        yield {
            'client_class': mock_client_class,
            'client': mock_client,
            'bucket': mock_bucket,
            'blob': mock_blob
        }


class TestGCSUploader:
    """Test cases for GCSUploader class"""
    
    def test_initialization(self):
        """Test GCSUploader initialization"""
        uploader = GCSUploader("test-bucket")
        
        assert uploader.bucket_name == "test-bucket"
        assert uploader.client is None
        assert uploader.bucket is None
    
    def test_init_client_success(self, mock_gcs_client):
        """Test successful GCS client initialization"""
        uploader = GCSUploader("test-bucket")
        uploader._init_client()
        
        assert uploader.client is not None
        assert uploader.bucket is not None
        mock_gcs_client['client_class'].assert_called_once()
        mock_gcs_client['client'].bucket.assert_called_once_with("test-bucket")
    
    def test_init_client_failure(self):
        """Test GCS client initialization failure"""
        with patch('scripts.uploader.storage.Client') as mock_client:
            mock_client.side_effect = Exception("Auth failed")
            
            uploader = GCSUploader("test-bucket")
            
            with pytest.raises(GoogleCloudError, match="Failed to initialize GCS client"):
                uploader._init_client()
    
    def test_upload_file_success(self, mock_gcs_client, temp_dir):
        """Test successful file upload"""
        # Create test file
        test_file = os.path.join(temp_dir, "test.csv")
        with open(test_file, 'w') as f:
            f.write("test,data\n")
        
        uploader = GCSUploader("test-bucket")
        result = uploader.upload_file(test_file, "uploaded.csv")
        
        # Verify client initialization
        mock_gcs_client['client_class'].assert_called_once()
        
        # Verify blob operations
        mock_gcs_client['bucket'].blob.assert_called_once_with("uploaded.csv")
        mock_gcs_client['blob'].upload_from_filename.assert_called_once_with(test_file)
        
        # Verify return value
        assert result == "gs://test-bucket/uploaded.csv"
    
    def test_upload_file_default_filename(self, mock_gcs_client, temp_dir):
        """Test file upload with default remote filename"""
        test_file = os.path.join(temp_dir, "test.csv")
        with open(test_file, 'w') as f:
            f.write("test,data\n")
        
        uploader = GCSUploader("test-bucket")
        uploader.upload_file(test_file)
        
        mock_gcs_client['bucket'].blob.assert_called_once_with("test.csv")
    
    def test_upload_file_not_found(self, mock_gcs_client):
        """Test upload with non-existent file"""
        uploader = GCSUploader("test-bucket")
        
        with pytest.raises(FileNotFoundError, match="Local file not found"):
            uploader.upload_file("nonexistent.csv")
    
    def test_upload_file_gcs_error(self, mock_gcs_client, temp_dir):
        """Test upload failure due to GCS error"""
        test_file = os.path.join(temp_dir, "test.csv")
        with open(test_file, 'w') as f:
            f.write("test,data\n")
        
        mock_gcs_client['blob'].upload_from_filename.side_effect = Exception("Upload failed")
        
        uploader = GCSUploader("test-bucket")
        
        with pytest.raises(GoogleCloudError, match="Failed to upload"):
            uploader.upload_file(test_file)
    
    def test_upload_multiple_files_success(self, mock_gcs_client, sample_files):
        """Test uploading multiple files"""
        uploader = GCSUploader("test-bucket")
        
        file_mappings = [
            (sample_files["directors.csv"], "dir.csv"),
            (sample_files["crew.csv"], "crew.csv")
        ]
        
        results = uploader.upload_multiple_files(file_mappings)
        
        assert len(results) == 2
        assert "gs://test-bucket/dir.csv" in results
        assert "gs://test-bucket/crew.csv" in results
        assert mock_gcs_client['blob'].upload_from_filename.call_count == 2
    
    def test_upload_multiple_files_partial_failure(self, mock_gcs_client, sample_files):
        """Test uploading multiple files with partial failures"""
        uploader = GCSUploader("test-bucket")
        
        # Mock one file to fail
        mock_gcs_client['blob'].upload_from_filename.side_effect = [
            None,  # First upload succeeds
            Exception("Upload failed")  # Second upload fails
        ]
        
        file_mappings = [
            (sample_files["directors.csv"], "dir.csv"),
            (sample_files["crew.csv"], "crew.csv")
        ]
        
        results = uploader.upload_multiple_files(file_mappings)
        
        # Only successful upload should be in results
        assert len(results) == 1
        assert "gs://test-bucket/dir.csv" in results
    
    def test_delete_file_success(self, mock_gcs_client):
        """Test successful file deletion"""
        uploader = GCSUploader("test-bucket")
        
        result = uploader.delete_file("test.csv")
        
        mock_gcs_client['bucket'].blob.assert_called_once_with("test.csv")
        mock_gcs_client['blob'].delete.assert_called_once()
        assert result is True
    
    def test_delete_file_failure(self, mock_gcs_client):
        """Test file deletion failure"""
        uploader = GCSUploader("test-bucket")
        mock_gcs_client['blob'].delete.side_effect = Exception("Delete failed")
        
        result = uploader.delete_file("test.csv")
        
        assert result is False
    
    def test_format_size(self):
        """Test file size formatting"""
        assert GCSUploader._format_size(512) == "512.0 B"
        assert GCSUploader._format_size(1024) == "1.0 KB"
        assert GCSUploader._format_size(1024 * 1024) == "1.0 MB"
        assert GCSUploader._format_size(1024 * 1024 * 1024) == "1.0 GB"


class TestUploadAllDatasets:
    """Test cases for upload_all_datasets function"""
    
    @patch.object(GCSUploader, 'upload_file')
    def test_upload_all_existing_files(self, mock_upload, sample_files, temp_dir):
        """Test uploading all datasets when all files exist"""
        upload_all_datasets("test-bucket", temp_dir)
        
        # Should attempt to upload all 4 files
        assert mock_upload.call_count == 4
        
        # Verify file names passed to upload
        uploaded_files = [call[0][1] for call in mock_upload.call_args_list]
        expected_files = ["directors.csv", "crew.csv", "basic_titles.csv", "ratings.csv"]
        assert set(uploaded_files) == set(expected_files)
    
    @patch.object(GCSUploader, 'upload_file')
    def test_upload_missing_files(self, mock_upload, temp_dir):
        """Test uploading when some files are missing"""
        # Create only 2 out of 4 files
        for filename in ["directors.csv", "basic_titles.csv"]:
            file_path = os.path.join(temp_dir, filename)
            with open(file_path, 'w') as f:
                f.write("test data\n")
        
        upload_all_datasets("test-bucket", temp_dir)
        
        # Should only attempt to upload existing files
        assert mock_upload.call_count == 2
        
        uploaded_files = [call[0][1] for call in mock_upload.call_args_list]
        assert "directors.csv" in uploaded_files
        assert "basic_titles.csv" in uploaded_files
    
    @patch.object(GCSUploader, 'upload_file')
    def test_upload_no_files(self, mock_upload, temp_dir):
        """Test uploading when no files exist"""
        upload_all_datasets("test-bucket", temp_dir)
        
        # Should not attempt any uploads
        mock_upload.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 