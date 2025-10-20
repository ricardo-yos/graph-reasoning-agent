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

## Usage

This section explains how to run the **Graph Navigator Tester**, a diagnostic script designed to validate the semantic navigation process over the `HeteroData` structure.

It allows developers to simulate how the Graph Navigator Agent expands nodes and explores relationships among `Neighborhood`, `Place`, and `Review` entities.

### 1. Edit the question
Open the file `agent_graph_navigator/graph_navigator_tester.py` and edit the question directly in the script:

```python
# --------------------------------------------------------
# Test block: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    # Load environment variables
    load_env()

    # Initialize the LLM manager and GraphNavigatorAgent
    llm_manager = LLMManager()
    agent = GraphNavigatorAgent(llm=llm_manager)

    # Test pipeline with a single question
    question = "Quais os petshops no bairro Jardim possuem elogios no atendimento?"
    run_graph_navigator_pipeline(question, agent)
```

### 2. Run the script
From the `src` directory, execute the script:

```python
cd src
python agents/agent_graph_navigator/graph_navigator_tester.py
```

### 3. See the results

Once executed, the script prints the step-by-step navigation process:

```python
[1 - Extracted Nodes]
{'Neighborhood': [{'name': 'Jardim'}], 'Place': [{'type': 'pet_store'}], 'RAG': [{'text': 'petshops'}, {'text': 'elogios no atendimento'}]}

[2 - Expanded Nodes]
{ ... }

[3 - RAG Reviews]
{ ... }

[4 - LLM Answer]
Os petshops no bairro Jardim que possuem elogios no atendimento são:

- Xodocão Pet Store: equipe de estética canina muito competente e atendimento muito bom.
- Pets Onaga - Unidade Jardim: atendimento muito atencioso e prestativo, bons preços e produtos completos.
- Pet Center Jardins: equipe totalmente profissional e atendimento excelente.
- Garden Pet Shop: atendimento excelente e cuidado com os pets.
- Petz: serviço de banho e tosa e autoatendimento disponíveis.
```
The output shows each **expansion layer** — from neighborhoods to places to reviews — demonstrating how the agent traverses the **semantic relationships** stored in the `HeteroData` structure.

---

## Architecture
The **Graph Navigator Agent** follows a modular architecture where each component is responsible for a specific stage of the semantic reasoning pipeline.

This design enables explainability and traceability of every decision, from question parsing to final answer generation.

### System Overview

   ```mermaid
   flowchart TD
       A[User Question] --> B[Question Parser]
       B --> C[Filter Nodes]
       C --> D[Node Relations Filter]
       D --> E[RAG Reviews]
       E --> F[Graph Answer]
   ```

This flow shows how a natural language question is progressively decomposed, filtered, expanded, and finally answered through graph-based reasoning.

1. `question_parser.py`
   This module interprets the **user’s natural language question** and extracts semantic entities (nodes) and intent clues.
   - **Input**: Text question
   - **Output**: A dictionary with structured entities, such as:

   ```python
   {
       "Neighborhood": [{"name": "Jardim"}],
       "Place": [{"type": "pet_store"}],
       "RAG": [{"text": "elogios no atendimento"}]
   }
   ```
   - **Purpose**: Identify what entities the user refers to and what type of relationship or attribute is being queried.

2. `filter_nodes.py`
   Responsible for selecting the **matching nodes** within the `HeteroData` graph based on the extracted entities.
   - **Input**: Parsed nodes from the question
   - **Output**: Filtered graph subset containing only relevant nodes
   - **Purpose**: Efficiently narrow the graph to only those entities related to the query (e.g., specific neighborhoods or business types).

3. `node_relations_filter.py`
   After node filtering, this module traverses **semantic relationships** in the graph to expand relevant entities.
   - **Input**: Filtered nodes and the relation mapping
   - **Output**: Expanded graph data with related nodes (e.g., places contained in a neighborhood)
   - **Purpose**: Build the **semantic context** around the entities by following their edges (`CONTAINS`, `HAS_REVIEW`, `NEAR`, etc.).

4. `rag_reviews.py`
   Implements **Retrieval-Augmented Generation (RAG)** for reviews.
   It searches, ranks, and selects the most relevant reviews associated with the expanded nodes.
   - **Input**: Expanded nodes and question context
   - **Output**: A set of reviews containing semantic matches to the question intent
   - **Purpose**: Provide the LLM with meaningful textual evidence for answer generation.

5. `graph_answer.py`
   The final stage, where the **LLM (Large Language Model)** synthesizes the results and generates a coherent natural-language answer.
   - **Input**: Relevant reviews, entities, and context
   - **Output**: Human-readable answer (e.g., list of places with positive feedback)
   - **Purpose**: Combine retrieved information with reasoning to produce a natural and context-aware response.

### Summary of the Flow

| Step | Module                     | Description                                      | Input              | Output                  |
| ---- | -------------------------- | ------------------------------------------------ | ------------------ | ----------------------- |
| 1    | `question_parser.py`       | Extracts entities and intent from user question  | Question text      | Structured entities     |
| 2    | `filter_nodes.py`          | Filters nodes in the graph by extracted entities | Entities           | Relevant nodes          |
| 3    | `node_relations_filter.py` | Expands nodes via semantic relationships         | Filtered nodes     | Expanded nodes          |
| 4    | `rag_reviews.py`           | Retrieves most relevant reviews                  | Expanded nodes     | Ranked reviews          |
| 5    | `graph_answer.py`          | Generates the final answer                       | Reviews + entities | Natural language answer |

### Neuro-Symbolic Integration

This architecture exemplifies a **Neuro-Symbolic AI approach** — combining symbolic reasoning from the structured graph (relations, entities, and attributes) with neural understanding from the LLM.

The graph provides **explicit structure and relationships**, while the LLM interprets and summarizes the implicit semantics in textual reviews.
Together, they enable contextual reasoning that goes beyond pure data retrieval or text generation — allowing the agent to infer meaningful, semantically grounded answers.

---

## Key Takeaways and Limitations

### Key Takeaways

- The **Graph Navigator Agent** integrates symbolic reasoning (graph traversal) with neural understanding (LLM), demonstrating a **neuro-symbolic approach** to question answering.
- The pipeline combines **structured graph exploration** with **RAG-based text retrieval**, allowing contextual answers that connect entities such as neighborhoods, places, and reviews.
- Each processing stage — from **question parsing** to **RAG retrieval** — contributes to building a semantically grounded reasoning chain.
- The system operates as a **proof of concept**, showing how a local knowledge graph (HeteroData) can be semantically navigated using natural language queries.
- The project provides a **foundation for more advanced city-level reasoning systems**, integrating NLP, knowledge graphs, and LLMs for urban intelligence and semantic search.

### Limitations

While the **Graph Navigator Agent** demonstrates a structured approach to semantic reasoning over a city-level knowledge graph, it still faces several important limitations:

- **Incomplete attribute extraction** – During the Question Parser stage, the LLM may fail to extract all relevant attributes for each node (e.g., missing the type or name field).
- **RAG text extraction errors** – When generating the RAG text context, the LLM can misinterpret the intent or fail to produce accurate retrieval cues, leading to confusion in later reasoning steps.
- **No spatial reasoning** – The agent does not perform real spatial or geographic searches. It only navigates relationships defined within the HeteroData graph structure.
- **Limited to known nodes and attributes** – If a question refers to entities or attributes not represented in the graph schema (e.g., “eventos para adoção”), the agent will not find a valid path or answer.
- **Possible hallucinations** – Both during parsing and final response generation, the LLM may produce fabricated or mismatched information, especially if similar review embeddings exist in the same vector space.
- **Static graph context** – Updates in data (e.g., new reviews or businesses) require graph regeneration; the agent does not dynamically synchronize with external sources.

---
