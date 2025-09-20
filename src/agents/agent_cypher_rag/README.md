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

## Usage

To test the pipeline, simply edit the user question in **rag_cypher_tester.py** and run the script.

### 1. Edit the question
Open `src/agents/agent_cypher_rag/rag_cypher_tester.py` and replace the example question:

```python
# ----------------------------
# Entry point
# ----------------------------
if __name__ == "__main__":
    # User question (edit this line)
    user_question = "Quantos petshops existem no bairro Vila Luzita?"
    
    # Run RAG Cypher pipeline
    results = run_rag_cypher_pipeline(user_question)

    # Print pipeline outputs
    print("Detected intentions:", results["detected_intentions"])
    print("Matched Cypher queries:", results["matched_cyphers"])
    print("Generated Cypher query:", results["generated_cypher"])
    print("Corrected Cypher query:", results["corrected_cypher"])
    print("Query results:", results["query_results"])
    print("Final answer:", results["final_answer"])
```

### 2. Run the script
From the `src` directory, execute:

```python
cd src
python agents/agent_cypher_rag/rag_cypher_tester.py
```

### 3. See the results
```python
Detected intentions: ['aggregate_nodes']

Matched Cypher queries: 
[
   "MATCH (n:Neighborhood)-[:CONTAINS]->(p:Place {type: 'pet_store'}) RETURN n.name, COUNT(p) AS total_petshops", 
   "MATCH (n:Neighborhood {name: 'Vila Bastos'})-[:CONTAINS]->(p:Place {type: 'pet_store'}) WHERE p.rating > 4 RETURN COUNT(p) AS total_petshops"
]

Generated Cypher query: 
MATCH (n:Neighborhood {name: 'Vila Luzita'})-[:CONTAINS]->(p:Place {type: 'pet_store'}) 
RETURN COUNT(p) AS total_petshops

Corrected Cypher query: 
MATCH (n:Neighborhood {name: 'Vila Luzita'})-[:CONTAINS]->(p:Place {type: 'pet_store'}) 
RETURN COUNT(p) AS total_petshops

Query results: 
[
   {'total_petshops': 2}
]

Final answer: 
"Existem 2 petshops no bairro Vila Luzita."
```

---

## Architecture
The **RAG Cypher Agent** integrates multiple components to convert natural language questions into executable Cypher queries and return structured answers. It uses a **retrieval-augmented generation (RAG)** approach combined with a Neo4j database.

### Components

1. **User Query**  
   - Receives a natural language question from the user.
   - Example: `"Mostre os petshops do bairro Jardim e os dados socieconômicos"`

2. **Intent Detection** (`detect_intention`)  
   - Uses the LLM to detect user intentions from the input question.
   - Updates the agent state with `intent_detected`.

3. **Cypher Matching** (`cypher_matching`)  
   - Retrieves relevant pre-defined Cypher queries from a dataset based on detected intents.
   - Uses **ChromaDB** + **sentence-transformers embeddings** to find the top matches.
   - Updates `matched_cyphers` in the state.

4. **Cypher Generation** (`generate_cypher_query`)  
   - Generates a new Cypher query using the LLM guided by the matched examples.
   - Updates `cypher_generated`.

5. **Cypher Correction** (`correct_cypher_query`)  
   - Applies fuzzy matching to generated Cypher queries and user questions, ensuring correct entity names using reference datasets.  
   - Fixes typos or inconsistencies from user input or LLM generation.
   - Updates `cypher_corrected` and `user_question_corrected`.

6. **Neo4j Execution** (`neo4j_client.run_query`)  
   - Runs the corrected Cypher query in Neo4j.
   - Returns structured query results stored in `query_results`.

7. **Answer Generation** (`generate_cypher_answer`)  
   - Produces a clear natural language answer based on query results and the corrected question.
   - Updates `final_response`.

### Simplified Flow
```python
User Question
↓
Intent Detection → Cypher Matching → Cypher Generation → Cypher Correction → Neo4j Execution → Answer Generation → Final Response
```

### Notes
- The agent maintains state using `MasterAgentState` and `CypherAgentState`.  
- Each step handles errors gracefully to ensure the pipeline does not break.  
- External modules are used for LLM interaction, query retrieval, and Neo4j operations.

---

## Sample Questions & Answers
This section shows sample questions you can ask the RAG Cypher Agent and the kind of responses it produces. It also provides guidance on how to write effective queries.

### How to Ask Questions
- Be specific about the type of information you want (e.g., petshops, ratings, neighborhoods, socio-economic data).  
- Formulate questions based on the graph structure, mentioning nodes (e.g., neighborhoods, places, intersections) and relationships (e.g., CONTAINS, HAS_REVIEW) to improve query accuracy.
- The agent can handle typos and variations thanks to fuzzy matching in Cypher correction.

### Example Interactions
| User Question | Final Answer |
|---------------|--------------|
| "Qual bairro tem a maior taxa de alfabetização?" | "O bairro com a maior taxa de alfabetização é Vila Bastos, com uma taxa de 99,7%." |
| "Quais locais oferecem serviço veterinário no bairro Vila Pires?" | "Os locais que oferecem serviço veterinário no bairro Vila Pires são: Samis Pet Clínica Veterinária, Mundo Rural Pet Shop e Clínica Veterinária Santo Andre e Villa Vet Clínica Veterinária." |
| "Calcule a média de avaliações de petshops no bairro Jardim." | "A média de avaliações de petshops no bairro Jardim é 4,7." |
| "Liste os petshops do bairro Vila Pires com avaliações acima de 4 estrelas" | "Existem os seguintes petshops no bairro Vila Pires com avaliações acima de 4 estrelas: Pet Shop Vila Pires (4.6 estrelas), Avicultura Kotori (4.6 estrelas), Vira Lactea - Espaço Pet Petshop em Santo André (4.9 estrelas), Mila Auau Estética Animal (4.8 estrelas) e Cãobeleireiro L'amour Pet (5.0 estrelas)" |
| "Mostre os bairros com maior número de pet shops e suas médias de renda mensal." | "Os bairros com maior número de pet shops e suas médias de renda mensal são: Vila Pires com 10 pet shops e média de renda mensal de R$ 2.287,08, Jardim com 10 pet shops e média de renda mensal de R$ 5.850,56, Vila Camilópolis com 9 pet shops e média de renda mensal de R$ 1.699,73, Campestre com 9 pet shops e média de renda mensal de R$ 2.956,62 e Vila Metalúrgica com 9 pet shops e média de renda mensal de R$ 1.552,14" |
| "Qual a distância do bairro Vila Pires até o bairro Vila Assunção?" | "A distância entre o bairro Vila Pires e o bairro Vila Assunção é aproximadamente 1,8 km." |
| "Qual é o comprimento total das ruas no bairro Centro em km?" | "O comprimento total das ruas no bairro Centro é de 60,896 km." |
| "Liste os pet shops do bairro Jardim com rating acima de 4 e mais de 10 reviews." | "Aqui estão os pet shops do bairro Jardim com rating acima de 4 e mais de 10 reviews: Pet Center Jardins, Ary dos Palhaços - Aquarismo Marinho, Vet Selvagem Pet Shop & Clinica Veterinaria e Pets Onaga - Unidade Jardim" |
| "Qual o petshop mais próximo um do outro no bairro Vila Pires?" | "O petshop mais próximo de outro no bairro Vila Pires é o Tutu Pet Store e o Mila Auau Estética Animal, que estão muito próximos um do outro." |
| "Quantas ruas tem o bairro Vila Assunção?" | "A Vila Assunção tem 916 ruas." |

### Tips
- Start with **general queries** if you are unsure of the dataset contents.  
- Use **keywords** such as service types, neighborhood names, or ratings to narrow down results.  
- The agent supports **multiple intents** in a single question, e.g., "Show petshops in Jardim with rating above 4 and more than 10 reviews.."  

---

## Key Takeaways

- **Dataset Coverage Matters**: The quality of generated Cypher queries depends on having representative examples for all entity types (neighborhoods, roads, reviews, etc.).  
- **Fuzzy Matching Helps but Has Trade-offs**: Typos are corrected automatically, but similar names can sometimes be misinterpreted.  
- **Multi-intent Queries Supported**: The agent can handle multiple constraints in a single question (e.g., location + rating + service type).  
- **Modular and Extensible**: Components like the retriever, Cypher correction, or LLM can be swapped or upgraded without redesigning the pipeline.  
- **Performance Sensitive**: Execution time depends on ChromaDB retrieval and Neo4j indexing; large graphs may affect latency.
- **Domain Transfer Potential**: Although tailored for Neo4j and Cypher, the same architecture can be applied to other graph-based domains with minimal changes.

## Limitations

- **Retriever Sensitivity**: If ChromaDB surfaces Cypher examples that are not semantically close to the user’s intent, the LLM may generate incorrect queries.  
- **LLM Sensitivity**: Small wording changes in questions can lead to different Cypher outputs.  
- **Error Propagation**: Mistakes in intent detection or retrieval propagate downstream, affecting final answers.  
- **Dataset Coverage**: Queries involving entities or relationships poorly represented in the dataset are harder to resolve and may reduce accuracy.
- **Complex Multi-hop Queries**: While supported, queries spanning many relationships can increase the chance of errors or incomplete answers.  
- **Human-in-the-loop Needed**: Continuous dataset refinement is required to maintain performance.

> These constraints highlight the importance of continuous iteration, but they also open opportunities to extend the system with stronger retrievers, schema-aware models, or hybrid approaches.

---
