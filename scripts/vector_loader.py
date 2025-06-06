import os
import pandas as pd
import logging
from typing import List
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance
import uuid

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VectorLoader:
    def __init__(self, host: str = "localhost", port: int = 6333, collection_name: str = "movie_embeddings"):
        self.client = QdrantClient(host=host, port=port)
        self.collection_name = collection_name
        self._ensure_collection()
    
    def _ensure_collection(self):
        """Create collection if it doesn't exist"""
        try:
            collections = self.client.get_collections().collections
            if not any(c.name == self.collection_name for c in collections):
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(size=1536, distance=Distance.COSINE),
                )
                logger.info(f"Created collection: {self.collection_name}")
            else:
                logger.info(f"Using existing collection: {self.collection_name}")
        except Exception as e:
            logger.error(f"Failed to create/access collection: {e}")
            raise
    
    def load_parquet_file(self, file_path: str):
        """Load a single parquet file into Qdrant"""
        logger.info(f"Loading {file_path}")
        
        try:
            df = pd.read_parquet(file_path)
            logger.info(f"Read {len(df)} records from {file_path}")
            
            # Prepare points for Qdrant
            points = []
            for _, row in df.iterrows():
                point = PointStruct(
                    id=str(uuid.uuid4()),
                    vector=row['embedding'].tolist() if hasattr(row['embedding'], 'tolist') else row['embedding'],
                    payload={
                        'tconst': row['tconst'],
                        'title': row['primaryTitle'],
                        'year': int(row['startYear'])
                    }
                )
                points.append(point)
            
            # Insert into Qdrant
            self.client.upsert(
                collection_name=self.collection_name,
                points=points
            )
            
            logger.info(f"Successfully loaded {len(points)} vectors from {file_path}")
            
        except Exception as e:
            logger.error(f"Failed to load {file_path}: {e}")
    
    def load_embeddings_folder(self, folder_path: str):
        """Load all parquet files from the embeddings folder"""
        logger.info(f"Scanning folder: {folder_path}")
        
        if not os.path.exists(folder_path):
            logger.error(f"Folder does not exist: {folder_path}")
            return
        
        # Find all parquet files
        parquet_files = [f for f in os.listdir(folder_path) if f.endswith('.parquet')]
        
        if not parquet_files:
            logger.warning(f"No parquet files found in {folder_path}")
            return
        
        logger.info(f"Found {len(parquet_files)} parquet files")
        
        # Load each file
        for file_name in sorted(parquet_files):
            file_path = os.path.join(folder_path, file_name)
            self.load_parquet_file(file_path)
        
        # Get final collection stats
        collection_info = self.client.get_collection(self.collection_name)
        logger.info(f"Total vectors in collection: {collection_info.points_count}")


def load_embeddings(folder_path: str = "./data/embeddings"):
    """Simple function to load embeddings"""
    loader = VectorLoader()
    loader.load_embeddings_folder(folder_path)


if __name__ == '__main__':
    loader = VectorLoader()
    loader.load_embeddings_folder("./data/embeddings") 