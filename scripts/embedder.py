import pandas as pd
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict
from openai import OpenAI

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class MovieEmbedder:
    def __init__(self, model: str = 'text-embedding-3-small', max_workers: int = 10):
        self.model = model
        self.max_workers = max_workers
        self.client = OpenAI()
    
    def _embed_single_movie(self, movie: Dict) -> List[float]:
        """Generate embedding for a single movie summary"""
        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=movie['summary']
            )
            return response.data[0].embedding
        except Exception as e:
            logger.warning(f"Failed to embed {movie.get('primaryTitle', 'unknown')}: {e}")
            return []
    
    def embed_batch(self, movies: List[Dict]) -> List[List[float]]:
        """Generate embeddings using concurrent requests"""
        logger.info(f"Processing {len(movies)} movies with {self.max_workers} workers")
        
        embeddings = []
        completed = 0
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(self._embed_single_movie, movie) for movie in movies]
            
            for future in as_completed(futures):
                embeddings.append(future.result())
                completed += 1
                if completed % 10 == 0 or completed == len(movies):
                    logger.info(f"Completed {completed}/{len(movies)} embeddings")
        
        logger.info(f"Finished processing all {len(movies)} embeddings")
        return embeddings
    
    def embed_dataset(self, data_path: str, output_path: str, batch_size: int = 500):
        """Process dataset in batches and save incrementally"""
        logger.info(f"Loading dataset from {data_path}")
        
        df = pd.read_json(data_path, lines=True).head(30)
        total_movies = len(df)
        logger.info(f"Processing {total_movies} movies in batches of {batch_size}")
        
        # Process in batches
        for batch_num, start_idx in enumerate(range(0, total_movies, batch_size)):
            end_idx = min(start_idx + batch_size, total_movies)
            batch_df = df.iloc[start_idx:end_idx].copy()
            
            logger.info(f"Processing batch {batch_num + 1}: movies {start_idx + 1}-{end_idx}")
            
            # Get embeddings for this batch
            movies = batch_df.to_dict('records')
            embeddings = self.embed_batch(movies)
            
            # Prepare output batch
            batch_output = batch_df.drop('summary', axis=1)
            batch_output['embedding'] = embeddings
            
            # Save batch to parquet
            batch_output_path = output_path.replace('.parquet', f'_{batch_num:02d}.parquet')
            batch_output.to_parquet(batch_output_path, engine="pyarrow", index=False)
            logger.info(f"Saved batch {batch_num + 1} to {batch_output_path}")
        
        logger.info(f"Completed processing all {total_movies} movies in {batch_num + 1} batches")


if __name__ == '__main__':
    embedder = MovieEmbedder()
    embedder.embed_dataset(
        './data/processed/enhanced.json', 
        './data/embeddings/enhanced_embeddings.parquet',
        batch_size=10
    ) 