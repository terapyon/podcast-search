import duckdb
from config import DUCKDB_FILE


def create_table():
    conn = duckdb.connect(DUCKDB_FILE)
    podcasts_create = """CREATE TABLE podcasts (
        id INTEGER PRIMARY KEY, 
        title TEXT, date DATE, guests TEXT[], length INTEGER, audio TEXT
        );
    """
    episodes_create = """CREATE TABLE episodes (
        id INTEGER, part INTEGER, start INTERVAL, end_ INTERVAL, text TEXT,
        PRIMARY KEY (id, part)
        );
    """
    embeddings_create = """CREATE TABLE embeddings (
        id INTEGER, part INTEGER, embedding FLOAT[1024],
        PRIMARY KEY (id, part)
        );
    """
    conn.execute(podcasts_create)
    conn.execute(episodes_create)
    conn.execute(embeddings_create)
    conn.commit()
    conn.close()
    print("Tables created.")


if __name__ == "__main__":
    import sys
    args = sys.argv
    if len(args) == 2:
        if args[1] == "create":
            create_table()
    else:
        print("Usage: python store.py create")
        sys.exit(1)
