import streamlit as st
import duckdb
from embedding import get_embeddings
from config import DUCKDB_FILE


@st.cache_resource
def get_conn():
    return duckdb.connect(DUCKDB_FILE)


query = """WITH ordered_embeddings AS (
    SELECT embeddings.id, embeddings.part FROM embeddings
        ORDER BY array_distance(embedding, ?::FLOAT[1024])
    LIMIT 10
)
SELECT
    p.title,
    p.date,
    e.start,
    e.text
  FROM
      ordered_embeddings oe
  JOIN
      episodes e
    ON
      oe.id = e.id AND oe.part = e.part
  JOIN
      podcasts p
    ON
      oe.id = p.id;
"""

st.title("terapyon cannel search")

word = st.text_input("Search word")
if word:
    st.write(f"Search word: {word}")
    embeddings = get_embeddings([word], query=True)
    word_embedding = embeddings[0, :]

    conn = get_conn()
    result = conn.execute(query, (word_embedding,)).df()
    selected = st.dataframe(result, 
                            on_select="rerun",
                            selection_mode="single-row")
    if selected:
        rows = selected["selection"].get("rows")
        if rows:
            row = rows[0]
            st.text(result.iloc[row, 3])
