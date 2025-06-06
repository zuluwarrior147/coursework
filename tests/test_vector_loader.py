import pytest
import pandas as pd
import tempfile
import os
from unittest.mock import Mock, patch
from scripts.vector_loader import VectorLoader


class TestVectorLoader:
    """Test cases for VectorLoader class"""
    
    @pytest.fixture
    def sample_df(self):
        """Sample dataframe with movie data and embeddings"""
        return pd.DataFrame([
            {
                'tconst': 'tt0111161',
                'primaryTitle': 'The Shawshank Redemption',
                'startYear': 1994,
                'embedding': [0.1, 0.2, 0.3, 0.4, 0.5]
            },
            {
                'tconst': 'tt0068646', 
                'primaryTitle': 'The Godfather',
                'startYear': 1972,
                'embedding': [0.5, 0.4, 0.3, 0.2, 0.1]
            }
        ])
    
    @patch('scripts.vector_loader.QdrantClient')
    def test_load_parquet_file(self, mock_qdrant, sample_df):
        """Test loading a parquet file"""
        # Mock Qdrant client
        mock_client = Mock()
        mock_qdrant.return_value = mock_client
        mock_client.get_collections.return_value.collections = []
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create temporary parquet file
            parquet_file = os.path.join(tmp_dir, 'test.parquet')
            sample_df.to_parquet(parquet_file, index=False)
            
            # Test the loader
            loader = VectorLoader()
            loader.load_parquet_file(parquet_file)
            
            # Verify upsert was called
            mock_client.upsert.assert_called_once()
            
            # Check the call arguments
            call_args = mock_client.upsert.call_args
            assert call_args.kwargs['collection_name'] == 'movie_embeddings'
            assert len(call_args.kwargs['points']) == 2
    
    @patch('scripts.vector_loader.QdrantClient')
    def test_ensure_collection(self, mock_qdrant):
        """Test collection creation"""
        mock_client = Mock()
        mock_qdrant.return_value = mock_client
        mock_client.get_collections.return_value.collections = []
        
        loader = VectorLoader()
        
        # Verify collection creation was called
        mock_client.create_collection.assert_called_once()
        call_args = mock_client.create_collection.call_args
        assert call_args.kwargs['collection_name'] == 'movie_embeddings' 