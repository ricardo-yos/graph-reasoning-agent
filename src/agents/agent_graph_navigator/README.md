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
The following file must exist in your project’s data directory (e.g., `MODELS_DIR`) for the **Graph Navigator Agent** to function correctly:

- `neo4j_heterodata.pt`  
  A serialized **PyG HeteroData** object containing all nodes, relationships, and attributes extracted from the Neo4j database, including review texts and metadata.

> **Note:** Make sure this file is generated and up-to-date before running the agent.

---

## Dataset Creation

The **Graph Navigator Agent** relies on structured graph data derived from Neo4j to perform reasoning and RAG-based enrichment. The dataset creation process ensures that nodes, relations, and review texts are properly formatted for graph traversal and semantic retrieval.

The graph is exported from Neo4j and stored as a **PyG HeteroData** object (`neo4j_heterodata.pt`).  
It contains multiple types of nodes representing neighborhoods, businesses (places), roads, intersections, and reviews. Relationships connect these nodes to reflect real-world connections, such as neighborhoods containing places, places having reviews, and roads linking intersections. All node attributes, including review texts, ratings, geographic coordinates, and other metadata, are stored directly in the graph, enabling the agent to traverse and retrieve contextual information efficiently.

### Dataset Preparation Workflow

1. **Export the Neo4j graph** using:
   ```bash
   python export_neo4j_to_heterodata.py
   ```
   This script extracts nodes, relationships, and attributes from Neo4j, converting them into a **PyG HeteroData** object saved as `neo4j_heterodata.pt`.

2. **Build the review embedding index:**
   The agent reads review texts directly from the `Review` nodes in the graph for RAG-based enrichment and reasoning.
   ```bash
   python build_review_chromadb.py
   ```
   Loads `neo4j_heterodata.pt`, extracts all review texts from the graph, generates embeddings using a BERT-based model, and stores them in `VECTOR_DB_DIR` for efficient semantic retrieval.

> **Note:** All review texts are stored within the graph; no external CSV is needed. The embeddings are generated automatically for retrieval.

---

## HeteroData Structure

The `neo4j_heterodata.pt` file is a PyTorch Geometric **HeteroData** object that represents the full graph exported from Neo4j.  
It contains multiple node types, edge types, and attributes, allowing flexible graph traversal and reasoning.

### Node Types and Attributes
| Node Type        | Description                                                                      | Main Attributes                                                                                                                                                                                                                  |
| ---------------- | -------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Neighborhood** | Represents a geographic district of Santo André.                                 | `name`, `area_km2`, `centroid_lat`, `centroid_lon`, `neighborhood_id`, `average_monthly_income`, `literacy_rate`, `population_with_income`, `total_literate_population`, `total_private_households`, `total_resident_population` |
| **Place**        | Represents a business related to pet care (e.g., pet shops, veterinary clinics). | `name`, `place_id`, `rating`, `type` (`'pet_store'` or `'veterinary_care'`), `latitude`, `longitude`, `num_reviews`                                                                                                              |
| **Road**         | Represents a street segment connecting intersections.                            | `name`, `highway`, `oneway`, `length`, `maxspeed`, `osmid`, `road_id`, `u`, `v`                                                                                                                                                  |
| **Intersection** | Represents a physical road intersection.                                         | `highway`, `osmid`, `lat`, `lon`, `street_count`                                                                                                                                                                                 |
| **Review**       | Represents a user review collected from Google Places.                           | `rating`, `review_id`, `author`, `text`, `date`                                                                                                                                                                                  |

### Edge Types
Each relation is represented in PyG using a triplet of node types `(src_type, relation_type, dst_type)`.

| Edge Type                              | Description                                          |
| -------------------------------------- | ---------------------------------------------------- |
| `(Neighborhood, "CONTAINS", Place)`    | Connects neighborhoods to the places within them.    |
| `(Neighborhood, "CONTAINS", Road)`     | Connects neighborhoods to their road segments.       |
| `(Road, "CONTAINS", Place)`            | Links roads to places located along them.            |
| `(Intersection, "ROAD", Intersection)` | Connects intersections that belong to the same road. |
| `(Place, "NEAR", Intersection)`        | Links each place to its nearest intersection.        |
| `(Place, "HAS_REVIEW", Review)`        | Connects a place to its user reviews.                |

### Semantic Representation in HeteroData

In this project, the HeteroData object from PyTorch Geometric (PyG) is not used for numerical tensor computation, but as a **semantic graph structure** representing entities and their relationships within the city of Santo André’s pet care ecosystem.

Each node type (e.g., `Place`, `Review`, `Neighborhood`) corresponds to a real-world entity. Instead of dense feature tensors, the `.x` attribute stores textual or contextual information, such as names, reviews, ratings, or other metadata. This allows the graph to capture rich semantic information while keeping the flexibility of PyG’s heterogeneous graph structure.

#### Node Types and Example Data

- **Neighborhood**  
  Represents a geographic district and its key attributes.
  ```python
  data["Neighborhood"].x = [
      {"name": "Vila Assunção", "area_km2": 1.5, "centroid_lat": -23.6698, "centroid_lon": -46.5270}
  ]
  ```

- **Place**  
  Represents a business related to pet care.
  ```python
  data["Place"].x = [
      {"name": "Pet Shop do Bairro", "type": "pet_store", "rating": 4.6},
      {"name": "Clínica Veterinária Santo André", "type": "veterinary_care", "rating": 4.9},
  ]
  ```

- **Review**  
  Represents a user review with textual content and rating.
  ```python
  data["Review"].x = [
      {"text": "Excelente atendimento!", "rating": 5, "author": "Maria"},
      {"text": "Demorou muito o atendimento.", "rating": 2, "author": "Paulo"},
  ]
  ```

Key points:

- `.x` holds **descriptive attributes** (names, types, ratings, texts, etc.) rather than numeric vectors.
- Relations (e.g., `data["Place", "HAS_REVIEW", "Review"].edge_index`) define **how entities are connected**.
- This structure allows agents to **traverse and reason over interconnected data**, enabling context expansion and RAG-based retrieval over graph-linked reviews.

By treating **HeteroData as a semantic substrate**, the Graph Navigator Agent can perform **conceptual reasoning** and **contextual expansion**` instead of pure numeric graph processing.

### Graph Expansion Flow in HeteroData

In the **Graph Reasoning Agent**, graph exploration starts from a **root node** — usually a `Neighborhood` — and expands iteratively to retrieve related entities such as `Places`, `Reviews`, `Roads`, and `Intersections`.  

This process allows the **Graph Navigator Agent** to traverse the **semantic graph** and enrich the reasoning context with meaningful, text-linked information.

#### 1. Starting from a Neighborhood

Each `Neighborhood` node represents a geographic region of Santo André, with demographic attributes (e.g., income, literacy rate, population).  
Expansion begins by identifying which entities are **contained within** that neighborhood.

```python
data["Neighborhood"].x = [
    {"name": "Vila Pires", "average_monthly_income": 3200, "population": 25000},
]
```

#### 2. Expanding to Places

From a given `Neighborhood`, the agent expands through the edge `(Neighborhood)-[:CONTAINS]->(Place)` to retrieve all **businesses** located in that area.

```python
data["Place"].x = [
    {"name": "Pet Shop Vila Pires", "type": "pet_store", "rating": 4.7},
    {"name": "Clínica Vet Pires", "type": "veterinary_care", "rating": 4.9},
]
```

#### 3. Expanding to Reviews

Each `Place` node connects to one or more `Review` nodes via `(Place)-[:HAS_REVIEW]->(Review)`. This allows the agent to retrieve the **textual feedback** related to each business.

```python
data["Review"].x = [
    {"text": "Excelente atendimento e cuidado com meu cachorro!", "rating": 5},
    {"text": "Preço justo e profissionais atenciosos.", "rating": 4},
]
```

#### 4. Connecting to Physical Context
Optionally, the agent can also expand to `Road` and `Intersection` nodes, building a **spatial context** around places and enabling richer reasoning.

---
