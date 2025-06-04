import typer
import sys
from pathlib import Path

# Add project root to Python path for clean absolute imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.loader import load_all_datasets

app = typer.Typer(help="ML Coursework Data Loading CLI")

@app.command()
def load(data_path: str = typer.Option("data", help="Directory to save the processed datasets")):
    """Load and process all IMDB datasets."""
    typer.echo(f"📁 Loading datasets to: {data_path}")
    try:
        load_all_datasets(data_path)
        typer.echo("✅ All datasets loaded successfully!")
    except Exception as e:
        typer.echo(f"❌ Error loading datasets: {e}")
        raise typer.Exit(1)

@app.command()
def version():
    """Show version information."""
    typer.echo("ML Coursework CLI v0.1.0")

if __name__ == "__main__":
    app()