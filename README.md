# Graph Reasoning Agent

**Graph Reasoning Agent** is an experimental framework that combines **graph-based knowledge representation** with **LLM-powered reasoning**. It shows how structured knowledge in a graph can guide a language model to reason along explicit relationships, producing **more accurate answers** than relying solely on unstructured data.

Traditional **retrieval-augmented generation (RAG)** relies only on **semantic similarity** to retrieve information from unstructured knowledge. Models based solely on embeddings **ignore relationships, causal links, and hierarchical structures**, which can lead to incomplete or inaccurate answers, as the model may hallucinate, misinterpret connections, or miss subtle dependencies. By integrating a **Knowledge Graph** with an **LLM**, paths through graph nodes are traced to construct reasoning chains, enriching responses with structured relationships and related knowledge for **more precise, contextually grounded answers**.

This approach aligns with the principles of **Neuro-Symbolic AI**, which combines **symbolic reasoning**—explicit, rule-based manipulation of structured knowledge—with the **learning capabilities of neural networks**. In the context of the Graph Reasoning Agent, the PyG HeteroData graph represents the **symbolic knowledge**, encoding entities, relationships, and hierarchical structures, while the LLM provides the **neural reasoning** component, generating answers and traversing graph paths guided by learned contextual understanding. By merging these paradigms, the agent can perform **structured reasoning with flexible natural language understanding**, leveraging both the precision of symbolic logic and the adaptability of neural models.

This project is a **Minimum Viable Product (MVP)**, demonstrating the concept of combining **LLM reasoning with graph-structured knowledge**. It aims to leverage **LLM capabilities for domain-specific tasks**, enabling accurate responses in **local or specialized contexts**. By combining structured knowledge with generative reasoning, this approach offers **more reliable and relevant answers for niche applications** and serves as a foundation for future experiments and extensions.

## Overview  

This project implements an **AI agent** for questions about **petshops**, **socioeconomic data of neighborhoods**, and **road networks from OpenStreetMap (OSM)** in Santo André. The master agent receives a user question and determines which of the two sub-agents will handle it:

1. **Cypher RAG Agent**: Responds to direct questions based on structured graph queries in **Neo4j**.  
   - Example questions:  
     - “Which petshops are located in Jardim neighborhood?”  
     - “Which veterinary clinics in Campestre neighborhood have ratings above 4?”  
     - “What is the population of the Parque das Nações neighborhood?”
   - Provides **precise answers** using the graph structure.  

2. **Navigator Graph Agent**: Handles complex questions by tracing paths in the **PyG graph**, expanding related entities, and enriching results with relevant textual information from customer reviews.  
   - Example questions:  
     - “Which petshops in Jardim neighborhood have good grooming services?”  
     - "Which locations in the Vila Assunção neighborhood offer pet accessories?"
     - "Find locations in the Campestre neighborhood where customers mention excellent service and product variety."  
   - Combines **graph structure** and **textual insights** to deliver **contextually grounded responses**.  

The **master agent** ensures that each question is routed to the appropriate sub-agent, choosing the Cypher RAG Agent for **direct, structured queries in Neo4j** and the Navigator Graph Agent for **complex or nuanced questions in PyG**.  

**Note**: The agent only answers questions that fall within the **knowledge encoded in the graph** and associated textual data, so questions outside this domain may not be answered accurately.

## Target Audience

- Developers and AI practitioners
- Researchers in knowledge graphs and LLMs
- Data scientists working with structured and unstructured data
- Anyone interested in domain-specific AI applications

---

## Project Structure

```bash
graph-reasoning-agent/
│
├── data/                                # All datasets used by the project
│   ├── fuzzy/                           # Fuzzy matching datasets for text normalization
│   │   ├── neighborhood_names.csv       # List of neighborhood name variations
│   │   ├── places_names.csv             # List of place name variations
│   │   └── street_names.csv             # List of street name variations
│   │
│   ├── interim/                         # Intermediate datasets generated during processing
│   │   └── santo_andre_sidra_ibge/      # SIDRA/IBGE demographic data for Santo André
│   │       ├── failed_downloads.json    # Log of datasets that failed to download from SIDRA
│   │       └── neighborhoods_sidra_long.csv  # Long-format version of IBGE data before transformation
│   │
│   ├── processed/                       # Final cleaned and processed datasets
│   │   ├── google_places/               # Processed data from Google Places API
│   │   │   ├── places_reviews.geojson   # Geospatial dataset of business reviews
│   │   │   └── reviews_processed.csv    # Cleaned and preprocessed review texts
│   │   ├── santo_andre_osm/             # Data extracted from OpenStreetMap (OSM)
│   │   │   └── santo_andre_osm_layers.gpkg  # OSM spatial layers stored in GeoPackage format
│   │   ├── santo_andre_sidra_ibge/      # Processed IBGE (SIDRA) data
│   │   │   └── neighborhoods_sidra_wide.csv # Wide-format version of IBGE neighborhood data
│   │   └── santo_andre_siga/            # Data from Santo André’s SIGA (geospatial management system)
│   │       ├── neighborhoods_processed.csv   # Cleaned SIGA neighborhood data
│   │       └── neighborhoods_processed.geojson # SIGA neighborhood data in geospatial format
│   │
│   ├── rag/                             # Data related to RAG (Retrieval-Augmented Generation) components
│   │   ├── rag_base_queries.csv         # Base query templates for RAG system
│   │   ├── rag_questions_cypher.csv     # RAG questions mapped to Cypher queries
│   │   └── rag_questions_cypher.json    # JSON version of RAG–Cypher question mappings
│   │
│   └── raw/                             # Raw data before any transformation
│       ├── google_places/               # Unprocessed data collected from Google Places API
│       │   ├── places_reviews.json      # Raw JSON data of business reviews
│       │   └── reviews.csv              # Tabular version of the reviews
│       │
│       ├── santo_andre_sidra_ibge/      # Raw demographic data from SIDRA/IBGE for each census sector
│       │   ├── 3547809001/              # Individual census sector folders (one per neighborhood region)
│       │   ├── 3547809002/
│       │   ├── 3547809003/
│       │   ├── 3547809004/
│       │   ├── 3547809005/
│       │   ├── ...                      # (Several other census sectors omitted for brevity)
│       │   ├── 3547809132/
│       │   ├── 3547809500/
│       │   └── Bairro em Município - Santo André (SP).xlsx  # Summary spreadsheet downloaded from SIDRA
│       │
│       └── santo_andre_siga/            # Raw files from Santo André’s municipal GIS (SIGA)
│           └── SIGA_LIM_BAIRROS_OFICIAL/
│               └── SIGA_LIM_BAIRROS_OFICIALPolygon.shp  # Official shapefile with neighborhood boundaries
│
├── models/
│   └── neo4j_heterodata.pt              # Trained PyTorch model for Neo4j heterogeneous graph embeddings
│
├── src/                                 # Source code for the entire project
│   ├── agents/                          # Intelligent agents for reasoning, querying, and orchestration
│   │   ├── agent_cypher_rag/            # RAG-based Cypher query agent
│   │   │   ├── operations/              # Core operations for Cypher generation and interpretation
│   │   │   │   ├── cypher_answer_generation.py # Generates answers using Cypher and retrieved data
│   │   │   │   ├── cypher_correction.py        # Fixes incorrect Cypher queries
│   │   │   │   ├── cypher_generation.py        # Builds Cypher queries based on intent and entities
│   │   │   │   ├── intent_detection.py         # Detects user intent from natural language input
│   │   │   │   └── match_cypher.py             # Matches natural language questions to stored Cypher templates
│   │   │   ├── rag_cypher_tester.py      # Script to test Cypher generation with RAG
│   │   │   ├── rag_cypher.py             # Main RAG–Cypher agent implementation
│   │   │   ├── README.md                 # Documentation for this agent
│   │   │   └── scripts/                  # Utility scripts for data prep and debugging
│   │   │       ├── build_rag_vector_db.py      # Builds ChromaDB vector database for RAG
│   │   │       ├── chroma_tester.py            # Tests vector retrieval performance
│   │   │       ├── convert_csv_to_json.py      # Converts CSV data to JSON format for RAG use
│   │   │       ├── expand_rag_questions.py     # Expands question set for RAG training and evaluation
│   │   │       └── generate_fuzzy_datasets.py  # Generates fuzzy text matching datasets
│   │   │
│   │   ├── agent_graph_navigator/       # Graph navigation and reasoning agent
│   │   │   ├── graph_navigator_tester.py # Tests graph traversal and node querying
│   │   │   ├── graph_navigator.py        # Main graph navigation logic
│   │   │   ├── operations/               # Core operations used by the graph navigator
│   │   │   │   ├── filter_nodes.py       # Filters graph nodes by attributes and relationships
│   │   │   │   ├── graph_answer.py       # Generates structured answers from graph data
│   │   │   │   ├── node_relations_filter.py # Filters node connections and relationships
│   │   │   │   ├── question_parser.py    # Parses natural language questions into graph operations
│   │   │   │   └── rag_reviews.py        # Handles RAG queries over customer review data
│   │   │   ├── README.md                 # Documentation for the graph navigator agent
│   │   │   └── scripts/                  # Helper scripts for graph data preparation
│   │   │       ├── build_review_chromadb.py    # Builds a ChromaDB from review embeddings
│   │   │       └── export_neo4j_to_heterodata.py # Exports Neo4j data into a heterogeneous format
│   │   │
│   │   └── agent_orchestrator/          # Coordinates multiple agents to perform reasoning tasks
│   │       ├── agent_state.py           # Tracks and updates the current reasoning state
│   │       └── build_agent.py           # Constructs and initializes agent pipelines
│   │
│   ├── cli_agent.py                     # Command-line interface to interact with agents
│   │
│   ├── config/                          # Configuration files and environment utilities
│   │   ├── constants.py                 # Global constants and default parameters
│   │   ├── env_loader.py                # Loads environment variables from .env or system settings
│   │   └── paths.py                     # Centralized management of project file paths
│   │
│   ├── data_processing/                 # Scripts for cleaning and transforming raw datasets
│   │   ├── google_places/               # Processing scripts for Google Places data
│   │   │   ├── convert_places_json_to_geojson.py # Converts raw JSON to GeoJSON format
│   │   │   └── prepare_reviews_for_neo4j.py      # Prepares reviews for Neo4j graph import
│   │   ├── ibge/                        # Data processing scripts for IBGE/SIDRA datasets
│   │   │   ├── convert_sidra_long_to_wide.py     # Converts long-format IBGE data to wide-format
│   │   │   ├── fetch_sidra_totals.py            # Fetches aggregated totals from SIDRA API
│   │   │   ├── retry_failed_downloads.py         # Retries failed SIDRA downloads
│   │   │   └── sidra_downloader.py               # Downloads census data from IBGE SIDRA API
│   │   ├── osm/                         # Processing scripts for OpenStreetMap data
│   │   │   └── extract_osm_layers.py             # Extracts and cleans relevant OSM spatial layers
│   │   └── siga/                        # Processing scripts for Santo André’s SIGA datasets
│   │       └── neighborhoods_pipeline.py         # Full pipeline for SIGA neighborhood data processing
│   │
│   ├── graph/                           # Graph-related modules for Neo4j integration
│   │   ├── client_tester.py             # Tests connection and queries to Neo4j
│   │   ├── neo4j/                       # Neo4j database connection and data handling logic
│   │   │   ├── client.py                # Manages Neo4j client sessions
│   │   │   ├── connector.py             # Handles authentication and driver setup
│   │   │   ├── insert_nodes.py          # Inserts nodes into the Neo4j database
│   │   │   ├── insert_relationships.py  # Creates relationships between nodes
│   │   │   └── neo4j_deletions.py       # Deletes nodes or relationships from Neo4j
│   │   ├── neo4j_pipeline.py            # End-to-end pipeline for building and updating the graph
│   │   ├── README.md                    # Documentation for graph components
│   │   └── spatial/                     # Spatial operations for geographic enrichment
│   │       └── assign_spatial_attributes.py # Assigns geospatial metadata to graph nodes
│   │
│   ├── llm/                             # Modules for Large Language Model (LLM) management
│   │   ├── llm_manager.py               # Wrapper for interacting with LLM APIs or local models
│   │   └── llm_tester.py                # Tests LLM responses and configurations
│   │
│   └── utils/                           # Utility functions and helpers
│       └── logger.py                    # Configurable logging setup for all modules
│
├── .gitignore                           # Specifies which files/folders Git should ignore
├── LICENSE                              # Open-source license for this project
├── README.md                            # Project documentation and usage guide
└── requirements.txt                     # List of Python dependencies required to run the project
```

---

## Architecture

The system follows a **Neuro-Symbolic AI architecture**, combining **neural intelligence** (language models and embeddings) with **symbolic reasoning** (knowledge graphs and Cypher logic).
This hybrid approach enables the system to interpret natural language queries semantically, reason over structured graph data, and generate context-aware insights about neighborhoods, businesses, and reviews in Santo André.

### Knowledge Graph

The system is built on a **heterogeneous semantic graph** that integrates multiple data layers — neighborhoods, businesses, roads, intersections, and reviews.
Each node type represents a distinct entity, and the edges capture their semantic relationships.

**Node types** include:

- `Neighborhood`: demographic and geographic data
- `Place`: pet-related businesses (pet shops, veterinary care)
- `Road` and `Intersection`: street network topology
- `Review`: user-generated feedback linked to places

**Main relationships**:

- `(Neighborhood)-[:CONTAINS]->(Place)`
- `(Place)-[:HAS_REVIEW]->(Review)`
- `(Place)-[:NEAR]->(Intersection)`
- `(Neighborhood)-[:CONTAINS]->(Road)`
- `(Intersection)-[:ROAD]->(Intersection)`

This structure allows for spatial, semantic, and social reasoning — enabling questions such as *“Which neighborhoods have the highest concentration of poorly rated pet shops near major roads?”*

### Graph Environments

The project employs two complementary graph environments:

- **Neo4j Graph (Symbolic Layer)** — used by the RAG Cypher Agent to execute Cypher queries for explainable reasoning over structured knowledge.
- **Heterogeneous Semantic Graph (PyTorch Geometric)** — used by the Graph Navigator Agent for in-memory graph processing, embedding computation, and data exploration.

Together, these layers combine symbolic reasoning and neural graph representations, forming a bridge between **knowledge-based logic** and **learning-based inference**.

> For a detailed description of the graph structure, node properties, and relationships, see [graph/README.md](src/graph/README.md).

### Neuro-Symbolic Layer
This layer bridges **neural** and **symbolic** representations:
- **Neural side**: Language models (LLMs) and embedding-based retrieval (RAG) interpret unstructured queries and map them into semantically meaningful graph operations.
- **Symbolic side**: The Neo4j knowledge graph provides an interpretable, rule-based structure that supports reasoning, constraints, and explainability.
- **Integration mechanism**: Cypher queries generated by the **RAG Cypher Agent** form the interface between neural understanding and symbolic inference, producing results that are both accurate and explainable.

### Agents Overview
Agents are modular components that interact with the Neuro-Symbolic system to perform specialized tasks such as query processing, reasoning, and data navigation.

#### Master Agent
- **Role**: Coordinates other agents and manages the flow of queries and responses.
- **Responsibilities**:
  - Orchestrate communication between the **RAG Cypher Agent** and **Graph Navigator Agent**
  - Maintain conversational and contextual consistency across multiple interactions
- **Reference**: See [src/agents/agent_orchestrator/README.md](src/agents/agent_orchestrator/README.md) for implementation details.

#### RAG Cypher Agent
- **Role**: Converts natural language queries into Cypher statements through a **Retrieval-Augmented Generation (RAG)** process.
- **Responsibilities**:
  - Detect user intent
  - Retrieve relevant Cypher examples and patterns from the vector database
  - Generate executable graph queries to reason over structured knowledge
- **Reference**: See [src/agents/agent_cypher_rag/README.md](src/agents/agent_cypher_rag/README.md).

#### Graph Navigator Agent
- **Role**: Directly interacts with the knowledge graph for exploration and analytics.
- **Responsibilities**:
  - Execute Cypher queries on the graph
  - Return structured insights and recommendations
  - Handle graph updates as needed
- **Reference**: See [src/agents/agent_graph_navigator/README.md](src/agents/agent_graph_navigator/README.md).

---
