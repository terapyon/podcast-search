from pathlib import Path
import duckdb
from config import DUCKDB_FILE


HERE = Path(__file__).parent
STORE_DIR = HERE.parent / "store"


def create_table():
    conn = duckdb.connect(DUCKDB_FILE)
    podcasts_create = """CREATE TABLE podcasts (
        id BIGINT PRIMARY KEY, 
        title TEXT, date DATE, guests TEXT[], length BIGINT, audio TEXT
        );
    """
    episodes_create = """CREATE TABLE episodes (
        id BIGINT, part BIGINT, start INTERVAL, end_ INTERVAL, text TEXT,
        PRIMARY KEY (id, part)
        );
    """
    embeddings_create = """CREATE TABLE embeddings (
        id BIGINT, part BIGINT, embedding FLOAT[1024],
        PRIMARY KEY (id, part)
        );
    """
    conn.execute(podcasts_create)
    conn.execute(episodes_create)
    conn.execute(embeddings_create)
    conn.commit()
    conn.close()
    print("Tables created.")


def insert_podcast():
    conn = duckdb.connect(DUCKDB_FILE)
    sql = """INSERT INTO podcasts
        SELECT id, title, date, [], length, audio
          FROM read_parquet(?);
    """
    conn.execute(sql, [str(STORE_DIR / 'podcast-title-list-202301-202501.parquet')])
    conn.commit()
    conn.close()


if __name__ == "__main__":
    import sys
    args = sys.argv
    if len(args) == 2:
        if args[1] == "create":
            create_table()
        elif args[1] == "podcastinsert":
            insert_podcast()
    else:
        print("Usage: python store.py create")
        sys.exit(1)
