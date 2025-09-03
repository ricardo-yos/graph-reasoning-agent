# Graph Reasoning Agent

**Graph Reasoning Agent** is an experimental framework that combines **graph-based knowledge representation** with **LLM-powered reasoning**. It shows how structured knowledge in a graph can guide a language model to reason along explicit relationships, producing **more accurate answers** than relying solely on unstructured data.

Traditional **retrieval-augmented generation (RAG)** relies only on **semantic similarity** to retrieve information from unstructured knowledge. Models based solely on embeddings **ignore relationships, causal links, and hierarchical structures**, which can lead to incomplete or inaccurate answers, as the model may hallucinate, misinterpret connections, or miss subtle dependencies. By integrating a **Knowledge Graph** with an **LLM**, paths through graph nodes are traced to construct reasoning chains, enriching responses with structured relationships and related knowledge for **more precise, contextually grounded answers**.

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
