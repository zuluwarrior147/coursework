import typer
import sys
from pathlib import Path

# Add project root to Python path for clean absolute imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.loader import load_all_datasets


app = typer.Typer()
app.command()(load_all_datasets)

if __name__ == "__main__":
    app()