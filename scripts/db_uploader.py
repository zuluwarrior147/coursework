import os
import pandas as pd
from db import MovieDB

def upload_to_db(data_path: str):
    db = MovieDB(
        dbname=os.getenv("PG_DB", "mydb"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASS", "secret"),
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432"))
    )

    df = pd.read_json(data_path, lines=True)
    for _, row in df.iterrows():
        db.add_movie(row['primaryTitle'], row['startYear'], row['summary'], row['weighted_rating'])

    db.close()

if __name__ == "__main__":
    db = MovieDB(
        dbname=os.getenv("PG_DB", "mydb"),
        user=os.getenv("PG_USER", "postgres"),
        password=os.getenv("PG_PASS", "secret"),
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432"))
    )
    print(db.search_by_tags(["darkly-comic","provocative","stylized-choreography","dialogue-heavy","cult-favorite"]))
    # upload_to_db("./data/processed/enhanced.json")