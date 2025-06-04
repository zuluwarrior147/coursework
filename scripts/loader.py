import os
import requests
import pandas as pd
import gzip
import shutil
import typer
from urllib.parse import urlparse

def load_all_datasets(data_path: str = typer.Option("data")):
    os.makedirs(data_path, exist_ok=True)
    datasets = [('https://datasets.imdbws.com/title.crew.tsv.gz', ['tconst', 'directors'], f"{data_path}/directors.csv"),
                ('https://datasets.imdbws.com/title.principals.tsv.gz', ['tconst', 'nconst', 'category', 'job', 'characters'], f"{data_path}/crew.csv"),
                ('https://datasets.imdbws.com/title.basics.tsv.gz', ['tconst', 'titleType', 'primaryTitle', 'startYear', 'genres'], f"{data_path}/basic_titles.csv"),
                ('https://datasets.imdbws.com/title.ratings.tsv.gz', ['tconst', 'averageRating', 'numVotes'], f"{data_path}/ratings.csv")]

    for dataset in datasets:
        loader = DatasetLoader(dataset[0], dataset[1], dataset[2])
        loader.process()

class DatasetLoader:
    def __init__(self, url: str, columns: list, output_file: str):
        self.url = url
        self.columns = columns
        self.output_file = output_file

        self.download_path = os.path.basename(urlparse(url).path)
        self.extract_path = 'extracted_temp'
        self.extracted_file = None

    def download(self):
        print(f"Downloading from {self.url} → {self.download_path}...")
        with requests.get(self.url, stream=True) as r:
            r.raise_for_status()
            with open(self.download_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print("Download complete.")

    def extract(self):
        print("Extracting .gz file...")
        os.makedirs(self.extract_path, exist_ok=True)

        extracted_filename = os.path.splitext(self.download_path)[0]
        output_file = os.path.join(self.extract_path, extracted_filename)

        with gzip.open(self.download_path, 'rb') as f_in, open(output_file, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

        self.extracted_file = output_file
        print(f"Extracted to: {output_file}")

    def find_data_file(self):
        if self.extracted_file and os.path.exists(self.extracted_file):
            return self.extracted_file

        for root, _, files in os.walk(self.extract_path):
            for file in files:
                if file.endswith('.csv') or file.endswith('.tsv'):
                    return os.path.join(root, file)
        raise FileNotFoundError("No CSV or TSV file found in archive.")

    def cleanup(self):
        if os.path.exists(self.download_path):
            os.remove(self.download_path)
            print(f"Deleted: {self.download_path}")
        if self.extracted_file and os.path.exists(self.extracted_file):
            os.remove(self.extracted_file)
            print(f"Deleted: {self.extracted_file}")
        if os.path.exists(self.extract_path):
            shutil.rmtree(self.extract_path)
            print(f"Deleted directory: {self.extract_path}")

    def process(self):
        try:
            self.download()
            self.extract()
            data_file = self.find_data_file()

            BatchProcessor(data_file, self.output_file, 10000, self.columns).process()
            print(f"Saved filtered data to: {self.output_file}")
        finally:
            self.cleanup()
            print("Cleanup complete. Done.")

class BatchProcessor:
    def __init__(self, file_path: str, output_file: str, batch_size: int, columns: list):
        self.file_path = file_path
        self.output_file = output_file
        self.batch_size = batch_size
        self.columns = columns
        self.separator = '\t' if file_path.endswith('.tsv') else ','

    def process(self):
        print(f"Processing {self.file_path} in chunks of {self.batch_size} rows...")

        df = pd.read_csv(self.file_path, sep=self.separator, usecols=self.columns, chunksize=self.batch_size)

        for i, chunk in enumerate(df):
            chunk.to_csv(self.output_file,mode='w' if i == 0 else 'a',index=False, header=(i == 0))
            print(f"{'Wrote' if i == 0 else 'Appended'} chunk {i}, {len(chunk)} rows")

        print("Finished processing.")


    def filter(self, filter_func):
        print(f"Processing {self.file_path} in chunks of {self.batch_size} rows...")

        df = pd.read_csv(self.file_path, sep=self.separator, chunksize=self.batch_size)

        for i, chunk in enumerate(df):
            chunk = chunk[chunk.apply(filter_func, axis=1)]
            chunk.to_csv(self.output_file,mode='w' if i == 0 else 'a',index=False, columns=self.columns, header=(i == 0))
            print(f"{'Wrote' if i == 0 else 'Appended'} chunk {i}, {len(chunk)} rows")

        print("Finished processing.")
