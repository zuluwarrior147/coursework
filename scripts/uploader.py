import os
import typer
from typing import List, Optional
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError


def upload_all_datasets(
    bucket_name: str = typer.Option(..., help="GCS bucket name to upload to"),
    data_path: str = typer.Option("data", help="Local directory containing the datasets"),
):
    """Upload all processed IMDB datasets to Google Cloud Storage bucket."""
    dataset_files = [
        "directors.csv",
        "crew.csv", 
        "basic_titles.csv",
        "ratings.csv"
    ]
    
    uploader = GCSUploader(bucket_name)
    
    for filename in dataset_files:
        local_path = os.path.join(data_path, filename)
        if os.path.exists(local_path):
            uploader.upload_file(local_path, filename)
        else:
            print(f"Warning: {local_path} not found, skipping...")


class GCSUploader:
    """Handle uploading files to Google Cloud Storage."""
    
    def __init__(self, bucket_name: str):
        """
        Args:
            bucket_name: Name of the GCS bucket
            prefix: Optional prefix/folder path in the bucket
        """
        self.bucket_name = bucket_name
        self.client = None
        self.bucket = None
        
    def _init_client(self):
        """Initialize GCS client and bucket (lazy loading)."""
        if self.client is None:
            try:
                self.client = storage.Client()
                self.bucket = self.client.bucket(self.bucket_name)
                print(f"Connected to GCS bucket: {self.bucket_name}")
            except Exception as e:
                raise GoogleCloudError(f"Failed to initialize GCS client: {e}")
    
    def upload_file(self, local_file_path: str, remote_filename: Optional[str] = None) -> str:
        """
        Upload a single file to GCS bucket.
        
        Args:
            local_file_path: Path to local file to upload
            remote_filename: Optional remote filename (defaults to local filename)
            
        Returns:
            GCS path of uploaded file
            
        Raises:
            FileNotFoundError: If local file doesn't exist
            GoogleCloudError: If upload fails
        """
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Local file not found: {local_file_path}")
        
        self._init_client()
        
        if remote_filename is None:
            remote_filename = os.path.basename(local_file_path)
        
        blob_name = f"{remote_filename}"
        blob = self.bucket.blob(blob_name)
        
        file_size = os.path.getsize(local_file_path)
        
        try:
            print(f"Uploading {local_file_path} → gs://{self.bucket_name}/{blob_name}")
            print(f"   File size: {self._format_size(file_size)}")
            
            blob.upload_from_filename(local_file_path)
            
            gcs_path = f"gs://{self.bucket_name}/{blob_name}"
            print(f"Upload complete: {gcs_path}")
            return gcs_path
            
        except Exception as e:
            raise GoogleCloudError(f"Failed to upload {local_file_path}: {e}")
    
    def upload_multiple_files(self, file_mappings: List[tuple]) -> List[str]:
        """
        Upload multiple files to GCS bucket.
        
        Args:
            file_mappings: List of (local_path, remote_filename) tuples
            
        Returns:
            List of GCS paths for uploaded files
        """
        uploaded_paths = []
        
        for local_path, remote_filename in file_mappings:
            try:
                gcs_path = self.upload_file(local_path, remote_filename)
                uploaded_paths.append(gcs_path)
            except (FileNotFoundError, GoogleCloudError) as e:
                print(f"Failed to upload {local_path}: {e}")
                continue
        
        return uploaded_paths
    
    def delete_file(self, remote_filename: str) -> bool:
        """
        Delete a file from GCS bucket.
        
        Args:
            remote_filename: Name of remote file to delete
            
        Returns:
            True if successful, False otherwise
        """
        self._init_client()
        
        blob_name = f"{remote_filename}"
        blob = self.bucket.blob(blob_name)
        
        try:
            blob.delete()
            print(f"Deleted: gs://{self.bucket_name}/{blob_name}")
            return True
        except Exception as e:
            print(f"Failed to delete {blob_name}: {e}")
            return False
    
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format file size in human readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB" 