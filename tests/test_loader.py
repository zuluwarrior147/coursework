"""
Tests for scripts.loader module

This module contains comprehensive tests for the DatasetLoader, BatchProcessor,
and load_all_datasets functionality with mocked HTTP requests and file operations.
"""
import pytest
import pandas as pd
import os
import tempfile
import shutil
import gzip
from unittest.mock import Mock, patch, mock_open
from io import StringIO
import requests

from scripts.loader import DatasetLoader, BatchProcessor, load_all_datasets


# Test fixtures and constants
@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_data():
    """Sample IMDB data for testing"""
    return {
        'tconst': ['tt0000001', 'tt0000002', 'tt0000003'],
        'titleType': ['short', 'short', 'short'],
        'primaryTitle': ['Carmencita', 'Le clown et ses chiens', 'Pauvre Pierrot'],
        'startYear': [1894, 1892, 1892],
        'genres': ['Documentary,Short', 'Animation,Short', 'Animation,Comedy,Romance']
    }


@pytest.fixture
def sample_csv_file(temp_dir, sample_data):
    """Create a sample CSV file for testing"""
    file_path = os.path.join(temp_dir, "test.csv")
    df = pd.DataFrame(sample_data)
    df.to_csv(file_path, index=False)
    return file_path


@pytest.fixture
def sample_tsv_file(temp_dir, sample_data):
    """Create a sample TSV file for testing"""
    file_path = os.path.join(temp_dir, "test.tsv")
    df = pd.DataFrame(sample_data)
    df.to_csv(file_path, sep='\t', index=False)
    return file_path


class TestDatasetLoader:
    """Test cases for DatasetLoader class"""
    
    @pytest.fixture
    def loader(self, temp_dir):
        """Create a DatasetLoader instance for testing"""
        return DatasetLoader(
            url="https://example.com/test.tsv.gz",
            columns=['tconst', 'titleType', 'primaryTitle'],
            output_file=os.path.join(temp_dir, "output.csv")
        )
    
    def test_initialization(self, loader):
        """Test DatasetLoader initialization"""
        assert loader.url == "https://example.com/test.tsv.gz"
        assert loader.columns == ['tconst', 'titleType', 'primaryTitle']
        assert loader.download_path == "test.tsv.gz"
        assert loader.extract_path == "extracted_temp"
        assert loader.extracted_file is None
    
    @patch('requests.get')
    def test_download_success(self, mock_get, loader):
        """Test successful file download"""
        mock_response = Mock()
        mock_response.iter_content.return_value = [b'test data chunk']
        mock_response.raise_for_status.return_value = None
        mock_get.return_value.__enter__.return_value = mock_response
        
        with patch('builtins.open', mock_open()) as mock_file:
            loader.download()
            
        mock_get.assert_called_once_with(loader.url, stream=True)
        mock_file.assert_called_once_with(loader.download_path, 'wb')
    
    @patch('requests.get')
    def test_download_http_error(self, mock_get, loader):
        """Test download failure handling"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        mock_get.return_value.__enter__.return_value = mock_response
        
        with pytest.raises(requests.HTTPError):
            loader.download()
    
    @patch('gzip.open')
    @patch('builtins.open')
    @patch('os.makedirs')
    @patch('shutil.copyfileobj')
    def test_extract(self, mock_copyfileobj, mock_makedirs, mock_open_builtin, mock_gzip_open, loader):
        """Test file extraction from gzip"""
        loader.extract()
        
        mock_makedirs.assert_called_once_with(loader.extract_path, exist_ok=True)
        mock_gzip_open.assert_called_once_with(loader.download_path, 'rb')
        assert loader.extracted_file.endswith('test.tsv')
    
    def test_find_data_file_existing(self, loader, temp_dir):
        """Test finding data file when extracted_file exists"""
        test_file = os.path.join(temp_dir, "test.tsv")
        with open(test_file, 'w') as f:
            f.write("test data")
        
        loader.extracted_file = test_file
        result = loader.find_data_file()
        assert result == test_file
    
    @patch('os.walk')
    def test_find_data_file_by_search(self, mock_walk, loader):
        """Test finding data file by directory search"""
        loader.extracted_file = None
        mock_walk.return_value = [('extracted_temp', [], ['test.tsv', 'readme.txt'])]
        
        result = loader.find_data_file()
        assert result == os.path.join('extracted_temp', 'test.tsv')
    
    @patch('os.walk')
    def test_find_data_file_not_found(self, mock_walk, loader):
        """Test error when no data file is found"""
        loader.extracted_file = None
        mock_walk.return_value = [('extracted_temp', [], ['readme.txt'])]
        
        with pytest.raises(FileNotFoundError):
            loader.find_data_file()
    
    @patch('os.path.exists')
    @patch('os.remove')
    @patch('shutil.rmtree')
    def test_cleanup(self, mock_rmtree, mock_remove, mock_exists, loader):
        """Test cleanup of temporary files"""
        mock_exists.return_value = True
        loader.extracted_file = "test_extracted.tsv"
        
        loader.cleanup()
        
        assert mock_remove.call_count == 2  # download_path and extracted_file
        mock_rmtree.assert_called_once_with(loader.extract_path)
    
    @patch.object(DatasetLoader, 'download')
    @patch.object(DatasetLoader, 'extract')
    @patch.object(DatasetLoader, 'find_data_file')
    @patch.object(DatasetLoader, 'cleanup')
    @patch.object(BatchProcessor, 'process')
    def test_process_workflow(self, mock_batch_process, mock_cleanup, mock_find_data, 
                             mock_extract, mock_download, loader):
        """Test complete processing workflow"""
        mock_find_data.return_value = "test_data.tsv"
        
        loader.process()
        
        # Verify workflow order
        mock_download.assert_called_once()
        mock_extract.assert_called_once()
        mock_find_data.assert_called_once()
        mock_batch_process.assert_called_once()
        mock_cleanup.assert_called_once()
    
    @patch.object(DatasetLoader, 'download')
    @patch.object(DatasetLoader, 'cleanup')
    def test_process_exception_cleanup(self, mock_cleanup, mock_download, loader):
        """Test cleanup is called even when exceptions occur"""
        mock_download.side_effect = Exception("Download failed")
        
        with pytest.raises(Exception):
            loader.process()
        
        mock_cleanup.assert_called_once()


class TestBatchProcessor:
    """Test cases for BatchProcessor class"""
    
    def test_initialization_csv(self, temp_dir):
        """Test BatchProcessor initialization with CSV file"""
        columns = ['tconst', 'titleType']
        processor = BatchProcessor(
            file_path=os.path.join(temp_dir, "test.csv"),
            output_file=os.path.join(temp_dir, "output.csv"),
            batch_size=100,
            columns=columns
        )
        
        assert processor.separator == ','
        assert processor.columns == columns
        assert processor.batch_size == 100
    
    def test_initialization_tsv(self, temp_dir):
        """Test BatchProcessor initialization with TSV file"""
        processor = BatchProcessor(
            file_path=os.path.join(temp_dir, "test.tsv"),
            output_file=os.path.join(temp_dir, "output.csv"),
            batch_size=100,
            columns=['tconst', 'titleType']
        )
        
        assert processor.separator == '\t'
    
    def test_process_with_column_filtering(self, sample_csv_file, temp_dir):
        """Test processing with column filtering"""
        output_file = os.path.join(temp_dir, "output.csv")
        columns = ['tconst', 'titleType', 'primaryTitle']
        
        processor = BatchProcessor(
            file_path=sample_csv_file,
            output_file=output_file,
            batch_size=2,  # Small batch size to test chunking
            columns=columns
        )
        
        processor.process()
        
        # Verify output
        assert os.path.exists(output_file)
        result_df = pd.read_csv(output_file)
        assert list(result_df.columns) == columns
        assert len(result_df) == 3
        assert result_df['tconst'].tolist() == ['tt0000001', 'tt0000002', 'tt0000003']
    
    def test_process_tsv_format(self, sample_tsv_file, temp_dir):
        """Test processing TSV format with column filtering"""
        output_file = os.path.join(temp_dir, "output.csv")
        columns = ['tconst', 'titleType']
        
        processor = BatchProcessor(
            file_path=sample_tsv_file,
            output_file=output_file,
            batch_size=2,
            columns=columns
        )
        
        processor.process()
        
        # Verify output
        assert os.path.exists(output_file)
        result_df = pd.read_csv(output_file)
        assert list(result_df.columns) == columns
        assert len(result_df) == 3
    
    def test_custom_filter_functionality(self, sample_csv_file, temp_dir):
        """Test custom row filtering"""
        output_file = os.path.join(temp_dir, "filtered_output.csv")
        columns = ['tconst', 'titleType', 'primaryTitle', 'startYear']
        
        processor = BatchProcessor(
            file_path=sample_csv_file,
            output_file=output_file,
            batch_size=2,
            columns=columns
        )
        
        # Filter to keep only movies from 1894
        def filter_1894(row):
            return row['startYear'] == 1894
        
        processor.filter(filter_1894)
        
        # Verify filtered output
        assert os.path.exists(output_file)
        result_df = pd.read_csv(output_file)
        assert list(result_df.columns) == columns
        assert len(result_df) == 1  # Only one row should match
        assert result_df['startYear'].iloc[0] == 1894
        assert result_df['tconst'].iloc[0] == 'tt0000001'


class TestLoadAllDatasets:
    """Test cases for load_all_datasets function"""
    
    @patch.object(DatasetLoader, 'process')
    def test_default_data_path(self, mock_process):
        """Test load_all_datasets with default data path"""
        load_all_datasets("data")
        assert mock_process.call_count == 2
    
    @patch.object(DatasetLoader, 'process')
    def test_custom_data_path(self, mock_process):
        """Test load_all_datasets with custom data path"""
        load_all_datasets("custom_data")
        assert mock_process.call_count == 2
    
    @patch.object(DatasetLoader, '__init__', return_value=None)
    @patch.object(DatasetLoader, 'process')
    def test_correct_dataset_parameters(self, mock_process, mock_init):
        """Test correct dataset URLs and parameters"""
        load_all_datasets("test_data")
        
        assert mock_init.call_count == 2
        
        # Verify first dataset parameters
        first_call = mock_init.call_args_list[0]
        assert first_call[0][0] == 'https://datasets.imdbws.com/title.basics.tsv.gz'
        assert first_call[0][1] == ['tconst', 'titleType', 'primaryTitle', 'startYear', 'genres']
        assert first_call[0][2] == 'test_data/basic_titles.csv'


class TestIntegrationWorkflow:
    """Integration tests for complete workflow"""
    
    def create_gzip_content(self, content):
        """Helper to create gzipped content"""
        import io
        buffer = io.BytesIO()
        with gzip.GzipFile(fileobj=buffer, mode='wb') as f:
            f.write(content.encode('utf-8'))
        return buffer.getvalue()
    
    @patch('requests.get')
    def test_end_to_end_workflow(self, mock_get, temp_dir):
        """Test complete download → extract → process workflow"""
        # Prepare test data
        mock_tsv_content = (
            "tconst\ttitleType\tprimaryTitle\tstartYear\tgenres\n"
            "tt0000001\tshort\tCarmencita\t1894\tDocumentary,Short\n"
            "tt0000002\tshort\tLe clown et ses chiens\t1892\tAnimation,Short\n"
        )
        
        # Mock HTTP response
        gzip_content = self.create_gzip_content(mock_tsv_content)
        mock_response = Mock()
        mock_response.iter_content.return_value = [gzip_content]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value.__enter__.return_value = mock_response
        
        # Create loader and process
        output_file = os.path.join(temp_dir, "output.csv")
        loader = DatasetLoader(
            url="https://example.com/test.tsv.gz",
            columns=['tconst', 'titleType', 'primaryTitle'],
            output_file=output_file
        )
        
        # Execute workflow
        original_cwd = os.getcwd()
        try:
            os.chdir(temp_dir)
            loader.process()
            
            # Verify results
            assert os.path.exists(output_file)
            result_df = pd.read_csv(output_file)
            
            expected_columns = ['tconst', 'titleType', 'primaryTitle']
            assert list(result_df.columns) == expected_columns
            assert len(result_df) == 2
            assert result_df['tconst'].tolist() == ['tt0000001', 'tt0000002']
            
        finally:
            os.chdir(original_cwd)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 