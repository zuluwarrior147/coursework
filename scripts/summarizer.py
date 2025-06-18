import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
from openai import OpenAI


# Simple logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SYSTEM_MSG = """You are a strict classification model. You can only respond with words from the dictionary below. 
You are not allowed to use any other words — no variations, no synonyms, no guesses.

Return only a single comma-separated line. No extra text. No formatting. No explanation.

Dictionary = ["lighthearted", "melancholic", "bittersweet", "bleak", "uplifting", "tense", "satirical", 
"heartwarming", "darkly-comic", "existentialist", "thought-provoking", "mind-bending", "nostalgic", 
"subversive", "redemption", "forbidden-love", "power-corruption", "identity-crisis", "survivalist", 
"slow-burn", "breakneck", "visually-immersive", "stylized-choreography", "dreamlike", "dialogue-heavy", 
"action-packed", "star-vehicle", "character-study", "cult-favorite", "family-oriented", "lore-rich", 
"silence-utilizing", "provocative", "political", "whimsical", "gritty", "tragic", "suspenseful", 
"comedic", "intimate", "psychedelic", "dystopian", "post-apocalyptic", "sentimental", 
"multi-layered", "morally-ambiguous", "sprawling", "atmospheric", "raw", "exuberant"]"""

USER_PROMPT = """Summarise the movie "{title}" ({year}) using only the dictionary provided in the system prompt.
You must choose between 5 and 10 words from dictionary. Return only a single comma-separated line. No extra text."""

REFINEMENT_SYSTEM_PROMPT = """You are a strict language filter. Your job is to repair a given list of descriptive words so that:

1. Only words from the dictionary below are used.  
2. The final result contains 5 to 10 words total.  
3. If any word is not in the dictionary, you must replace it with the **most semantically similar** word from the dictionary.    
4. Return a single comma-separated line with the final words.

Only use this dictionary:

[
"lighthearted", "melancholic", "bittersweet", "bleak", "uplifting", "tense", "satirical", 
"heartwarming", "darkly-comic", "existentialist", "thought-provoking", "mind-bending", "nostalgic", 
"subversive", "redemption", "forbidden-love", "power-corruption", "identity-crisis", "survivalist", 
"slow-burn", "breakneck", "visually-immersive", "stylized-choreography", "dreamlike", "dialogue-heavy", 
"action-packed", "star-vehicle", "character-study", "cult-favorite", "family-oriented", "lore-rich", 
"silence-utilizing", "provocative", "political", "whimsical", "gritty", "tragic", "suspenseful", 
"comedic", "intimate", "psychedelic", "dystopian", "post-apocalyptic", "sentimental", 
"multi-layered", "morally-ambiguous", "sprawling", "atmospheric", "raw", "exuberant"
]
"""

REFINEMENT_USER_PROMPT = """
Original input:
{input}
Return only a single comma-separated line. No extra text.
"""


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
                    {"role": "system", "content": SYSTEM_MSG},
                    {"role": "user", "content": USER_PROMPT.format(
                        title=movie['primaryTitle'], 
                        year=movie['startYear']
                    )}
                ]
            )
            refined_response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": REFINEMENT_SYSTEM_PROMPT},
                    {"role": "user", "content": REFINEMENT_USER_PROMPT.format(input=response.choices[0].message.content)}
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
