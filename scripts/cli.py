import typer
import sys
from pathlib import Path

# Add project root to Python path for clean absolute imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.loader import load_all_datasets
from scripts.gc_uploader import upload_all_datasets
from scripts.summarizer import summarize_dataset

app = typer.Typer(help="ML Coursework Data Loading CLI")

@app.command()
def load(data_path: str = typer.Option("data", help="Directory to save the processed datasets")):
    """Load and process all IMDB datasets."""
    typer.echo(f"Loading datasets to: {data_path}")
    try:
        load_all_datasets(data_path)
        typer.echo("All datasets loaded successfully!")
    except Exception as e:
        typer.echo(f"Error loading datasets: {e}")
        raise typer.Exit(1)

@app.command()
def upload(
    bucket_name: str = typer.Option(..., help="GCS bucket name to upload to"),
    data_path: str = typer.Option("data", help="Local directory containing the datasets"),
):
    """Upload processed IMDB datasets to Google Cloud Storage bucket."""
    typer.echo(f"Uploading datasets from {data_path} to gs://{bucket_name}")
    try:
        upload_all_datasets(bucket_name, data_path)
        typer.echo("All datasets uploaded successfully!")
    except Exception as e:
        typer.echo(f"Error uploading datasets: {e}")
        raise typer.Exit(1)

@app.command()
def version():
    """Show version information."""
    typer.echo("ML Coursework CLI v0.1.0")

@app.command()
def summarize(data_path: str = typer.Option("data/processed/top_rated_weighted.csv", help="Path to the dataset"),
              output_path: str = typer.Option("data/processed/enhanced.json", help="Path to the output file")):
    typer.echo(f"Getting summaries for {data_path}")
    try:
        summarize_dataset(data_path, output_path)
        typer.echo("Dataset summarized successfully!")
    except Exception as e:
        typer.echo(f"Error summarizing datasets: {e}")
        raise typer.Exit(1)

if __name__ == "__main__":
    app()