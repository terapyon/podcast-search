import streamlit as st
import duckdb
from embedding import get_embeddings
from config import DUCKDB_FILE


@st.cache_resource
def get_conn():
    return duckdb.connect(DUCKDB_FILE)


title_query = """SELECT id, title FROM podcasts
    ORDER BY date DESC;
"""

query = """WITH filtered_podcasts AS (
    SELECT id
      FROM podcasts
        WHERE id in ?
),
ordered_embeddings AS (
    SELECT embeddings.id, embeddings.part
    FROM embeddings
    JOIN filtered_podcasts fp ON embeddings.id = fp.id
    ORDER BY array_distance(embedding, ?::FLOAT[1024])
    LIMIT 10
)
SELECT
    p.title,
    p.date,
    e.start,
    e.text,
    e.part,
    p.audio,
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

conn = get_conn()
titles = conn.execute(title_query).df()
selected_title: list[str] | None = st.multiselect("Select title", titles["title"])
if selected_title:
    st.write(f"Selected title: {selected_title}")
    selected_ids = titles.loc[titles.loc[:, "title"].isin(selected_title), "id"].tolist()
else:
    st.write("All titles")
    selected_ids = titles.loc[:, "id"].tolist()

word = st.text_input("Search word")
if word:
    st.write(f"Search word: {word}")
    embeddings = get_embeddings([word], query=True)
    word_embedding = embeddings[0, :]

    result = conn.execute(query,
                          (selected_ids, word_embedding,)).df()
    selected = st.dataframe(result,
                            column_order=["title", "date", "part", "start", "text", "audio"],
                            on_select="rerun",
                            selection_mode="single-row")
    if selected:
        rows = selected["selection"].get("rows")
        if rows:
            row = rows[0]
            st.text(result.iloc[row, 3])
