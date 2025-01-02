from pathlib import Path
import duckdb
from embedding import get_embeddings
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
        id BIGINT, part BIGINT, start BIGINT, end_ BIGINT, text TEXT,
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
    conn.execute(sql, [str(STORE_DIR / 'title-list-202301-202501.parquet')])
    conn.commit()
    conn.close()


def insert_episodes():
    conn = duckdb.connect(DUCKDB_FILE)
    sql = """INSERT INTO episodes
        SELECT id, part, start, end_, text
          FROM read_parquet(?);
    """
    conn.execute(sql, [str(STORE_DIR / 'podcast-*.parquet')])
    conn.commit()
    conn.close()


def embed_store():
    conn = duckdb.connect(DUCKDB_FILE)
    sql_select = """SELECT id, part, text FROM episodes;"""
    data = conn.execute(sql_select).df()
    targets = data["text"].tolist()
    enbeddings = get_embeddings(targets)
    for id_, part, emb in zip(data["id"], data["part"], enbeddings):
        # print(id_, title)
        conn.execute(
            "INSERT INTO embeddings VALUES (?, ?, ?)", (id_, part, emb.tolist())
        )
    conn.commit()
    conn.close()


def create_index():
    conn = duckdb.connect(DUCKDB_FILE)
    conn.execute("LOAD vss;")
    conn.execute("SET hnsw_enable_experimental_persistence=true;")
    conn.execute("""CREATE INDEX embeddings_index 
                      ON embeddings USING HNSW (embedding);""")
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
        elif args[1] == "episodeinsert":
            insert_episodes()
        elif args[1] == "embed":
            embed_store()
        elif args[1] == "index":
            create_index()
        elif args[1] == "all":
            create_table()
            insert_podcast()
            insert_episodes()
            embed_store()
            create_index()
        else:
            print("Usage: python store.py all")
            sys.exit(1)
    else:
        print("Usage: python store.py create")
        sys.exit(1)
