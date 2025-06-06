import pytest
from unittest.mock import Mock, patch
from scripts.summarizer import MovieSummarizer


class TestMovieSummarizer:
    """Test cases for MovieSummarizer class"""
    
    @pytest.fixture
    def sample_movies(self):
        """Sample movie data for testing"""
        return [
            {'primaryTitle': 'The Shawshank Redemption', 'startYear': 1994},
            {'primaryTitle': 'The Godfather', 'startYear': 1972}
        ]
    
    @patch('scripts.summarizer.OpenAI')
    def test_summarize_single_movie(self, mock_openai_class, sample_movies):
        """Test single movie summarization"""
        mock_client = Mock()
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "Great movie about hope."
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        summarizer = MovieSummarizer()
        result = summarizer._summarize_single_movie(sample_movies[0])
        
        assert result == "Great movie about hope."
        mock_client.chat.completions.create.assert_called_once()
    
    @patch('scripts.summarizer.OpenAI')
    def test_summarize_batch(self, mock_openai_class, sample_movies):
        """Test batch processing"""
        mock_client = Mock() 
        mock_response = Mock()
        mock_response.choices = [Mock()]
        mock_response.choices[0].message.content = "Movie summary"
        mock_client.chat.completions.create.return_value = mock_response
        mock_openai_class.return_value = mock_client
        
        summarizer = MovieSummarizer(max_workers=2)
        results = summarizer.summarize_batch(sample_movies)
        
        assert len(results) == 2
        assert all(result == "Movie summary" for result in results)
        assert mock_client.chat.completions.create.call_count == 2 