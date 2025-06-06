import pytest
from unittest.mock import Mock, patch
from scripts.embedder import MovieEmbedder


class TestMovieEmbedder:
    """Test cases for MovieEmbedder class"""
    
    @pytest.fixture
    def sample_movies(self):
        """Sample movie data with summaries for testing"""
        return [
            {
                'primaryTitle': 'The Shawshank Redemption', 
                'startYear': 1994,
                'summary': 'A story of hope and redemption in prison.'
            },
            {
                'primaryTitle': 'The Godfather', 
                'startYear': 1972,
                'summary': 'A crime saga about family and power.'
            }
        ]
    
    @patch('scripts.embedder.OpenAI')
    def test_embed_single_movie(self, mock_openai_class, sample_movies):
        """Test single movie embedding generation"""
        mock_client = Mock()
        mock_response = Mock()
        mock_response.data = [Mock()]
        mock_response.data[0].embedding = [0.1, 0.2, 0.3, 0.4, 0.5]
        mock_client.embeddings.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        embedder = MovieEmbedder()
        result = embedder._embed_single_movie(sample_movies[0])
        
        assert result == [0.1, 0.2, 0.3, 0.4, 0.5]
        mock_client.embeddings.create.assert_called_once_with(
            model='text-embedding-3-small',
            input='A story of hope and redemption in prison.'
        )
    
    @patch('scripts.embedder.OpenAI')
    def test_embed_batch(self, mock_openai_class, sample_movies):
        """Test batch embedding processing"""
        mock_client = Mock() 
        mock_response = Mock()
        mock_response.data = [Mock()]
        mock_response.data[0].embedding = [0.1, 0.2, 0.3]
        mock_client.embeddings.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        embedder = MovieEmbedder(max_workers=2)
        results = embedder.embed_batch(sample_movies)
        
        assert len(results) == 2
        assert all(result == [0.1, 0.2, 0.3] for result in results)
        assert mock_client.embeddings.create.call_count == 2 