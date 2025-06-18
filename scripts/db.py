import psycopg2
from typing import List
from dictionary import to_bitmask


class MovieDB:
    def __init__(self, dbname, user, password, host="localhost", port=5432):
        self.conn = psycopg2.connect(
            dbname=dbname, user=user, password=password,
            host=host, port=port
        )
        self.conn.autocommit = True
        self._init_schema()

    def _init_schema(self):
        with self.conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS movies (
                id SERIAL PRIMARY KEY,
                title TEXT NOT NULL,
                year INTEGER NOT NULL,
                rating DECIMAL(4, 3) NOT NULL,
                tag_mask BIT(50) NOT NULL,
                UNIQUE(title, year)
            );
            """)

    def add_movie(self, title: str, year: int, tags: List[str], rating: float):
        bitmask = to_bitmask(tags)
        print(f"Adding movie {title} with tags {tags} and bitmask {bitmask}")
        with self.conn.cursor() as cur:
            cur.execute(
                """INSERT INTO movies (title, year, rating, tag_mask) VALUES (%s, %s, %s, %s) 
                ON CONFLICT (title, year) DO NOTHING""",
                (title, year, rating, bitmask)
            )

    def search_by_tags(self, tags: List[str], limit: int = 5):
        bitmask = to_bitmask(tags)
        print(bitmask)
        with self.conn.cursor() as cur:
            cur.execute(f"""
            WITH input(mask) AS (
                SELECT B'{bitmask}'::BIT(50)
            )
            SELECT
              m.title,
              BIT_COUNT(m.tag_mask & i.mask) AS match_count
            FROM movies m, input i
            WHERE m.tag_mask & i.mask <> B'0'
            ORDER BY match_count DESC, m.rating DESC
            LIMIT %s;
            """, (limit,))
            return cur.fetchall()

    def close(self):
        self.conn.close()
