import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict
from openai import OpenAI

# Simple logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYSTEM_MSG = "You are an assistant who is an expert in movie industry"
USER_PROMPT = "Summarise {title} ({year}) movie in under 80 words, conveying its intent, themes, and tone. Return raw text, no markdown."


class MovieSummarizer:
    def __init__(self, model: str = 'gpt-4.1-nano', max_workers: int = 10):
        self.model = model
        self.max_workers = max_workers
        self.client = OpenAI()
    
    def _summarize_single_movie(self, movie: Dict) -> str:
        """Summarize a single movie"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_MSG},
                    {"role": "user", "content": USER_PROMPT.format(
                        title=movie['primaryTitle'], 
                        year=movie['startYear']
                    )}
                ]
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.warning(f"Failed to summarize {movie['primaryTitle']}: {e}")
            return "Summary unavailable"
    
    def summarize_batch(self, movies: List[Dict]) -> List[str]:
        """Summarize movies using concurrent requests"""
        logger.info(f"Processing {len(movies)} movies with {self.max_workers} workers")
        
        from concurrent.futures import as_completed
        
        summaries = []
        completed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._summarize_single_movie, movie) for movie in movies]
            
            for future in as_completed(futures):
                summaries.append(future.result())
                completed += 1
                if completed % 10 == 0 or completed == len(movies):
                    logger.info(f"Completed {completed}/{len(movies)} movies")
        
        logger.info(f"Finished processing all {len(movies)} movies")
        return summaries
    
    def summarize_dataset(self, data_path: str, output_path: str):
        """Process entire dataset"""
        logger.info(f"Loading dataset from {data_path}")
        
        df = pd.read_csv(data_path)
        movies = df.to_dict('records')
        
        summaries = self.summarize_batch(movies)
        df['summary'] = summaries
        
        df.to_json(output_path, orient='records', lines=True)
        logger.info(f"Saved results to {output_path}")


def summarize_dataset(data_path: str, output_path: str = './data/processed/enhanced.json'):
    """Legacy function"""
    summarizer = MovieSummarizer()
    summarizer.summarize_dataset(data_path, output_path)


if __name__ == '__main__':
    summarizer = MovieSummarizer()
    summarizer.summarize_dataset('./data/processed/top_rated_weighted.csv')


