# RAG Cypher Agent

## Overview
The **RAG Cypher Agent** was created to address a common limitation of large language models (LLMs): they often fail to generate precise and executable Cypher queries. LLMs tend to hallucinate properties, produce invalid syntax, or ignore the actual structure of the graph.  

To solve this, the agent applies **retrieval-augmented generation (RAG)**. Instead of relying only on the model’s raw generation, it retrieves relevant examples from a dataset of **questions and corresponding Cypher queries** and uses them to guide the LLM. This retrieval-augmented process ensures that the model is grounded in real examples, increasing the likelihood of producing accurate and meaningful Cypher queries for Neo4j.  

In practice, the workflow is as follows:  
- A user asks a natural language question.  
- The retriever searches for **similar examples** from the dataset.  
- These examples are provided as **context** to the LLM.  
- Guided by this context, the LLM generates a new **Cypher query** adapted to the user’s request.  

This approach increases accuracy, reduces errors, and helps ensure that the generated Cypher is both syntactically correct and semantically relevant for querying Neo4j.

## Main Dependencies
Key libraries used in the RAG Cypher Agent pipeline:

- **neo4j**: Connects to the Neo4j graph database and executes Cypher queries.  
- **llm.llm_manager**: Interface to the language model (e.g., Groq) for generating and refining Cypher queries from natural language questions.
- **sentence-transformers / langchain_huggingface**: Generates embeddings from natural language questions for semantic search and retrieval.  
- **chromadb**: Vector database for storing embeddings and metadata (question, intention, Cypher) for RAG retrieval.  
- **python-dotenv**: Loads environment variables, such as API keys or Neo4j credentials, securely.  
- **os, json, csv, re, shutil**: Standard Python libraries used for file handling, dataset processing, and JSON/CSV manipulation.

---

## Prerequisites & Setup

### Neo4j Account & Setup
To use the RAG Cypher Agent, you must have access to a running Neo4j instance.  
You can either:

- **Neo4j AuraDB (cloud)**: Create a free account and database at [Neo4j AuraDB](https://neo4j.com/product/auradb/).  
- **Neo4j Desktop (local)**: Install Neo4j locally via [Neo4j Desktop](https://neo4j.com/download/).

Make sure your database contains the nodes and relationships required for your queries.

### Python & Environment
- Recommended: **Python 3.10+**
- It's strongly recommended to use a virtual environment.

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
# Neo4j credentials
NEO4J_URI=<your_neo4j_bolt_uri>
NEO4J_USER=<your_neo4j_username>
NEO4J_PASSWORD=<your_neo4j_password>

# LLM credentials (Groq)
GROQ_API_KEY=<your_api_key_here>
```

- `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`: Required for connecting to your Neo4j database.  
- `GROQ_API_KEY`: Required by `LLMManager` to access the Groq LLM for generating Cypher variations.  

> **Note:** Keep this file private and do not push it to GitHub.

### Required Data Files
The following files must exist in your `RAG_DATA_DIR` for the RAG Cypher Agent to work correctly:

- `rag_base_queries.csv`
  Contains the original questions, intentions, and Cypher queries that will be expanded.

- `rag_questions_cypher.json`
  JSON file with the expanded questions and Cypher queries, used to build the vector database.

> **Note:** Ensure these files are up-to-date before running the scripts.

---

## Dataset Creation
The performance of the RAG Cypher Agent heavily depends on the quality and coverage of the dataset of **question–Cypher pairs**. This dataset serves as the foundation for the retrieval step, guiding the LLM to generate accurate and executable queries.

### How to Generate the Dataset
1. **Prepare the base CSV**  
   Make sure `rag_base_queries.csv` exists in your `RAG_DATA_DIR`, containing original questions, intentions, and Cypher queries.

2. **Run the expansion script**  
   Execute `expand_rag_questions.py` to generate variations of each question and Cypher query using secondary commands:

```bash
python src/agents/agent_cypher_rag/expand_rag_questions.py
```

This produces `rag_questions_cypher.csv` with all expanded variations.

3. **Convert CSV to JSON**
   Run `convert_csv_to_json.py` to transform the CSV into a JSON file suitable for building the vector database:

```bash
python src/agents/agent_cypher_rag/convert_csv_to_json.py
```
This produces `rag_questions_cypher.json` in your `RAG_DATA_DIR`.

4. **Build the RAG vector database**

Execute `build_rag_vector_db.py` to create embeddings for each question and persist the vector database in Chroma:

```bash
python src/agents/agent_cypher_rag/build_rag_vector_db.py
```

This stores the vector database in `VECTOR_DB_DIR`.

5. **Test the vector database**

Run `chroma_tester.py` to verify that the database was built correctly and that queries return expected results:

```bash
python src/agents/agent_cypher_rag/chroma_tester.py
```

### Improving and Maintaining the Dataset
1. **Analyze gaps in coverage**: Regularly review which types of questions or Cypher patterns are missing and add examples to fill those gaps.
2. **Ensure topic coverage**: Continuously check that all relevant entities, relationships, and query patterns in your Neo4j graph are represented. Expanding coverage reduces the chance of the LLM failing on unseen or uncommon scenarios.  
3. **Refine existing pairs**: Improve the clarity of natural language questions and simplify Cypher queries where possible to avoid ambiguity.  
4. **Expand with real-world data**: Incorporate actual user questions and their correct Cypher queries as they appear in production.  
5. **Validate continuously**: Re-test examples to ensure queries remain valid and aligned with the evolving graph schema.  
6. **Iterate strategically**: Add edge cases, optimize examples for complex query patterns, and remove redundant entries to keep the dataset focused and effective.  

> Maintaining this dataset is an **iterative process** that grows with your use cases.

### Importance of Comprehensive Coverage
- The **retriever** relies on similarity: if a type of question is missing, the LLM may not generate a correct query.
- Diverse examples help the agent **generalize better**, reducing hallucinations and invalid syntax.
- Ensures consistent performance across **all types of queries** the users may ask.

By maintaining a well-structured and comprehensive dataset, the RAG Cypher Agent can reliably generate accurate Cypher queries for a wide range of questions.

> **Note:** This workflow reflects the approach I found effective for generating the dataset, though it may not be the only or optimal method.

---
