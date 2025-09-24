"""
Build ChromaDB from HeteroData Reviews
======================================

This script loads reviews from a PyTorch Geometric HeteroData object, 
splits them into token-based chunks, generates embeddings using a 
HuggingFace BERT-based Portuguese model, and stores them in a persistent 
ChromaDB collection for efficient retrieval and similarity search.

Features
--------
- Extract 'text', 'review_id', and 'rating' from HeteroData nodes.
- Split texts into token-based chunks while preserving metadata.
- Generate embeddings for each chunk using BERT embeddings.
- Store embeddings in a persistent ChromaDB collection.
- Handles batch insertion to manage memory efficiently.

Usage
-----
Run the pipeline as a standalone script:
    python build_review_chromadb.py

Steps performed
---------------
1. Load the HeteroData file from `MODELS_DIR`.
2. Split review texts into token chunks.
3. Generate embeddings using HuggingFace BERT.
4. Insert embeddings into a persistent ChromaDB collection at `VECTOR_DB_GRAPH_NAVIGATOR`.
"""

import os
import torch
from typing import List
from torch_geometric.data import HeteroData
from langchain.text_splitter import TokenTextSplitter
from langchain_community.vectorstores import Chroma
from langchain_community.embeddings import HuggingFaceEmbeddings
from config.paths import MODELS_DIR, VECTOR_DB_GRAPH_NAVIGATOR

# -----------------------------
# Load reviews from HeteroData
# -----------------------------

def load_reviews(hetero_path: str) -> list:
    """
    Load review texts, IDs, and ratings from a HeteroData object.

    Parameters
    ----------
    hetero_path : str
        Path to the serialized HeteroData object.

    Returns
    -------
    reviews : list
        List of dictionaries containing 'text', 'review_id', and 'rating'.
    """
    print(f"Loading HeteroData from: {hetero_path}")

    # Safely load HeteroData with torch
    with torch.serialization.safe_globals([HeteroData]):
        hetero_data = torch.load(hetero_path, weights_only=False)

    if "Review" not in hetero_data.node_types:
        raise ValueError("HeteroData does not contain 'Review' node type.")

    required_attrs = ["text", "review_id", "rating"]
    for attr in required_attrs:
        if not hasattr(hetero_data["Review"], attr):
            raise ValueError(f"Review nodes must have '{attr}' attribute.")

    texts = hetero_data["Review"].text
    review_ids = hetero_data["Review"].review_id
    ratings = hetero_data["Review"].rating

    # Combine into a list of dictionaries
    reviews = [{"text": t, "review_id": rid, "rating": r} 
               for t, rid, r in zip(texts, review_ids, ratings)]

    print(f"Loaded {len(reviews)} reviews.")
    return reviews

# ---------------------------------------
# Split review texts into token chunks
# ---------------------------------------

def chunk_reviews_by_tokens(
    reviews: list,
    chunk_size: int = 256,
    chunk_overlap: int = 32,
    encoding_name: str = "cl100k_base"
) -> list:
    """
    Split review texts into token-based chunks while keeping metadata.

    Parameters
    ----------
    reviews : list
        List of review dictionaries with 'text', 'review_id', 'rating'.
    chunk_size : int
        Maximum number of tokens per chunk.
    chunk_overlap : int
        Number of tokens to overlap between chunks.
    encoding_name : str
        Tokenizer encoding name.

    Returns
    -------
    chunked_reviews : list
        List of dictionaries with 'text' and 'metadata' for each chunk.
    """
    # Initialize token-based text splitter
    splitter = TokenTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=chunk_overlap,
        encoding_name=encoding_name
    )

    chunked_reviews = []

    # Process each review
    for review in reviews:
        review_text = review["text"]
        metadata = {
            "review_id": review["review_id"],
            "rating": review["rating"]
        }

        # Split review into chunks
        chunks = splitter.split_text(review_text)

        # Store each chunk with metadata
        for idx, chunk in enumerate(chunks):
            chunk_meta = metadata.copy()
            chunk_meta["chunk_index"] = idx
            chunked_reviews.append({
                "text": chunk,
                "metadata": chunk_meta
            })

    print(f"Generated {len(chunked_reviews)} chunks from {len(reviews)} reviews.")
    return chunked_reviews

# -----------------------------------------
# Generate embeddings for review chunks
# -----------------------------------------

def embed_review_chunks(review_chunks: List[str]) -> List[List[float]]:
    """
    Generate embeddings for a list of review chunks using HuggingFace BERT.

    Parameters
    ----------
    review_chunks : List[str]
        List of review texts to embed.

    Returns
    -------
    embeddings : List[List[float]]
        Embedding vectors for each chunk.
    """
    # Choose device dynamically
    device = "cuda" if torch.cuda.is_available() else "mps" if torch.backends.mps.is_available() else "cpu"
    print(f"Embedding chunks on device: {device}")

    model = HuggingFaceEmbeddings(
        model_name="neuralmind/bert-base-portuguese-cased",
        model_kwargs={"device": device},
    )

    # Generate embeddings
    embeddings = model.embed_documents(review_chunks)
    print(f"Generated embeddings for {len(review_chunks)} chunks")
    return embeddings

# -----------------------------------------
# Initialize ChromaDB collection
# -----------------------------------------

def initialize_db(persist_dir: str) -> Chroma:
    """
    Initialize a ChromaDB collection for storing embeddings.

    Parameters
    ----------
    persist_dir : str
        Directory to persist the collection.

    Returns
    -------
    collection : Chroma
        Initialized Chroma collection ready for insertion.
    """
    embedding_fn = HuggingFaceEmbeddings(
        model_name="neuralmind/bert-base-portuguese-cased"
    )

    # Delete existing collection if present
    collection = Chroma(
        collection_name="review_embeddings",
        embedding_function=embedding_fn,
        persist_directory=persist_dir,
    )
    collection.delete_collection()

    # Recreate empty collection
    collection = Chroma(
        collection_name="review_embeddings",
        embedding_function=embedding_fn,
        persist_directory=persist_dir,
    )
    return collection

# -----------------------------------------
# Insert review chunks into ChromaDB
# -----------------------------------------

def insert_review_chunks(collection: Chroma, chunked_reviews: list, batch_size: int = 5000) -> None:
    """
    Insert review chunks into Chroma collection in batches.

    Parameters
    ----------
    collection : Chroma
        The Chroma collection to insert into.
    chunked_reviews : list
        List of review chunk dictionaries with 'text' and 'metadata'.
    batch_size : int
        Number of chunks to insert per batch.
    """
    total = len(chunked_reviews)
    print(f"Inserting {total} chunks into ChromaDB (batch size={batch_size})")

    # Insert in batches to avoid memory issues
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        batch = chunked_reviews[start:end]

        texts = [chunk["text"] for chunk in batch]
        metadatas = [chunk["metadata"] for chunk in batch]
        ids = [f"{m['review_id']}_chunk{m['chunk_index']}" for m in metadatas]

        # Generate embeddings for current batch
        embeddings = embed_review_chunks(texts)

        # Add to Chroma collection
        collection.add_texts(texts=texts, metadatas=metadatas, ids=ids, embeddings=embeddings)
        print(f"Inserted batch {start}–{end}")

    collection.persist()
    print("All chunks inserted and collection persisted.")

# -----------------------------------------
# Main pipeline execution
# -----------------------------------------

if __name__ == "__main__":
    # Load reviews from HeteroData
    hetero_path = os.path.join(MODELS_DIR, "neo4j_heterodata.pt")
    reviews = load_reviews(hetero_path)

    # Split reviews into token chunks
    chunked_reviews = chunk_reviews_by_tokens(reviews)

    # Initialize ChromaDB collection
    collection = initialize_db(VECTOR_DB_GRAPH_NAVIGATOR)

    # Embed chunks and insert into ChromaDB
    insert_review_chunks(collection, chunked_reviews)

    # Verify stored documents
    stored = collection.get(include=["documents"])
    total = len(stored.get("documents", []))
    print(f"Pipeline complete: {total} documents stored in ChromaDB.")