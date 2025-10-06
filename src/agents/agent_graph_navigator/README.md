# Graph Navigator Agent

## Overview
The **Graph Navigator Agent** is a key component of the **Graph Reasoning Agent (GRA)** project, designed to **traverse and explore PyTorch Geometric (PyG) HeteroData graphs**. It enables for **iterative expansion of nodes and relations**, allowing for deep contextual understanding of the graph without relying on pre-defined queries. The agent can seamlessly integrate **RAG-based retrieval** to enrich the graph with **text from reviews**, enhancing reasoning with real-world contextual data.

This approach supports flexible graph exploration and incremental context retrieval, enabling reasoning workflows that span multiple nodes, relations, and review-based textual sources.

The workflow proceeds as follows:
- **Question Processing**: Extract initial nodes and relevant keywords from the user’s question.
- **Node Expansion**: Expand these nodes in the PyG HeteroData graph by traversing their relations.
- **RAG Integration**: Retrieve relevant review or textual data for extracted nodes using a RAG (Retrieval-Augmented Generation) retriever.
- **State Update**: Merge the expanded nodes and retrieved textual context into the internal graph state.
- **Answer Generation**: Generate a response to the user query using the enriched graph state, combining structural information with contextual data from reviews.

This workflow enables the agent to address complex queries that require **both structural reasoning** over the graph and **contextual understanding** derived from review text.

## Main Dependencies  
Key libraries used in the **Graph Navigator Agent** pipeline:

- **torch / torch-geometric**: Core libraries for handling and traversing **PyTorch Geometric (PyG) HeteroData** graphs.  
- **neo4j / py2neo** *(optional)*: Used when synchronizing or exporting graph data between **Neo4j** and **PyG** representations.  
- **transformers / sentence-transformers**: Provides language models (e.g., **BERTimbau**, **MiniLM**) for generating embeddings or semantic representations of node text and reviews.  
- **chromadb**: Vector database used to store and retrieve embeddings from **review text** for **RAG-based enrichment**.  
- **langchain / langchain_huggingface**: Interfaces the RAG retriever with language models to fetch relevant textual context for graph nodes.  
- **python-dotenv**: Loads environment variables, such as API keys or model paths, securely.  
- **os, json, re, shutil**: Standard Python libraries for data management, file handling, and JSON manipulation within the graph reasoning workflow.

> **Note:** The **Neo4j** dependency is optional and only required if the graph needs to be synchronized or exported between **Neo4j** and **PyG**.

---

## Prerequisites & Setup

### Neo4j (Optional)
The **Graph Navigator Agent** primarily operates on **PyTorch Geometric (PyG) HeteroData** graphs.  
However, if you wish to synchronize or import graph data from a **Neo4j** database, you can use either:

- **Neo4j AuraDB (cloud)**: Create a free account and database at [Neo4j AuraDB](https://neo4j.com/product/auradb/).  
- **Neo4j Desktop (local)**: Install Neo4j locally via [Neo4j Desktop](https://neo4j.com/download/).

> **Note:** Neo4j integration is optional — it is only required if you plan to load or export data between Neo4j and the PyG graph.

### Python & Environment
- Recommended: **Python 3.10+**
- It’s strongly recommended to use a virtual environment.

<details><summary>Linux / macOS</summary>

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

</details><details><summary>Windows (PowerShell)</summary>

```bash
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

</details>

### Environment Variables
Create a `.env` file in your project root with the following variables:

```bash
# Optional Neo4j credentials (if syncing data)
NEO4J_URI=<your_neo4j_bolt_uri>
NEO4J_USER=<your_neo4j_username>
NEO4J_PASSWORD=<your_neo4j_password>

# LLM credentials (Groq)
GROQ_API_KEY=<your_api_key_here>
```

- `NEO4J_*`: Only required if connecting to a Neo4j database.
- `GROQ_API_KEY`: Required if using the **Groq LLM** for RAG-based retrieval or text generation from reviews.

> **Note:** Keep this file private and never push it to GitHub.

### Required Data Files
The following files must exist in your project’s data directory (e.g., `GRAPH_DATA_DIR`) for the **Graph Navigator Agent** to function properly:

- `hetero_graph.pt`
  Serialized **PyG HeteroData** graph containing nodes, relations, and attributes.

- `review_texts.csv`
  CSV file containing reviews associated with graph nodes, used for **RAG-based retrieval**.

- (Optional) `embedding_index`
  Vector database (e.g., **ChromaDB** directory) storing embeddings for review texts.

> **Note:** Ensure these files are properly generated and up-to-date before running the agent.

---
