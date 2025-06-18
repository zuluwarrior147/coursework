import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
from openai import OpenAI
from prompts import SYSTEM_SUMMARY_PROMPT, USER_SUMMARY_PROMPT, REFINEMENT_SUMMARY_SYSTEM_PROMPT, REFINEMENT_SUMMARY_USER_PROMPT


# Simple logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MovieSummarizer:
    def __init__(self, model: str = 'gpt-4.1-nano', max_workers: int = 5):
        self.model = model
        self.max_workers = max_workers
        self.client = OpenAI()
    
    def _summarize_single_movie(self, movie: Dict) -> str:
        """Summarize a single movie"""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_SUMMARY_PROMPT},
                    {"role": "user", "content": USER_SUMMARY_PROMPT.format(
                        title=movie['primaryTitle'], 
                        year=movie['startYear']
                    )}
                ]
            )
            refined_response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": REFINEMENT_SUMMARY_SYSTEM_PROMPT},
                    {"role": "user", "content": REFINEMENT_SUMMARY_USER_PROMPT.format(input=response.choices[0].message.content)}
                ]
            )
            return refined_response.choices[0].message.content
        except Exception as e:
            logger.warning(f"Failed to summarize {movie['primaryTitle']}: {e}")
            return "Summary unavailable"
    
    def summarize_in_parallel(self, movies: List[Dict]) -> List[str]:
        """Summarize movies using concurrent requests"""
        logger.info(f"Processing {len(movies)} movies with {self.max_workers} workers")
        
        summaries = [None] * len(movies)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_index = {
                executor.submit(self._summarize_single_movie, movie): i 
                for i, movie in enumerate(movies)
            }
            
            for completed, future in enumerate(as_completed(future_to_index), 1):
                index = future_to_index[future]
                summaries[index] = future.result()
                if completed % 10 == 0 or completed == len(movies):
                    logger.info(f"Completed {completed}/{len(movies)} movies")
        
        return summaries
    
    def summarize_dataset(self, data_path: str, output_path: str = './data/processed/enhanced.json'):
        """Process entire dataset"""
        logger.info(f"Loading dataset from {data_path}")
        
        df = pd.read_csv(data_path)
        
        df['summary'] = self.summarize_in_parallel(df.to_dict('records'))
        df.to_json(output_path, orient='records', lines=True)
        logger.info(f"Saved results to {output_path}")


def summarize_dataset(data_path: str, output_path: str = './data/processed/enhanced.json'):
    """Legacy function"""
    summarizer = MovieSummarizer()
    summarizer.summarize_dataset(data_path, output_path)


if __name__ == '__main__':
    summarize_dataset('./data/top_rated_weighted.csv')
