import numpy as np
from sentence_transformers import SentenceTransformer

MODEL_NAME = "cl-nagoya/ruri-large"
PREFIX_QUERY = "クエリ: "  # "query: "
PASSAGE_QUERY = "文章: "  # "passage: "

model = SentenceTransformer(MODEL_NAME)


def get_embeddings(texts: list[str], query=False, passage=False) -> np.ndarray:
    if query:
        texts = [PREFIX_QUERY + text for text in texts]
    if passage:
        texts = [PASSAGE_QUERY + text for text in texts]
    # texts = [text[i : i + CHUNK_SIZE] for i in range(0, len(text), CHUNK_SIZE)]
    embeddings = model.encode(texts)
    # print(embeddings.shape)
    # print(type(embeddings))
    return embeddings
