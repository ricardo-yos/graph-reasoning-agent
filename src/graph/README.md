# Graph Processing and Neo4j Module

## Overview
This module provides tools and scripts to process geographic and review data and insert it into a Neo4j graph database. It includes ETL pipelines, node and relationship
insertion utilities, and scripts for testing and maintenance.

It handles neighborhoods, places, roads, intersections, and reviews, ensuring data integrity with unique identifiers, indexes, and batch operations, while keeping the spatial and connectivity relationships between entities intact.

The processed graph is stored directly on the Neo4j server, making it accessible via Cypher queries and Neo4j tools (Browser, Bloom, or external clients).

## Main Dependencies
- Python 3.10+
- pandas
- geopandas
- py2neo
- shapely
- python-dotenv (for environment variable management)

---

## Prerequisites & Setup

### Neo4j Account & Setup
To use this module you must have access to a running Neo4j instance.  
You can create a free account and database at [Neo4j AuraDB](https://neo4j.com/product/auradb/)  
or install Neo4j locally via [Neo4j Desktop](https://neo4j.com/download/).

### Python / Environment
- Recommended: **Python 3.10+**
- Create a virtual environment:

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

---

## Usage
1. Load environment variables:
```bash
export NEO4J_URI=<your_neo4j_bolt_uri>
export NEO4J_USER=<your_neo4j_username>
export NEO4J_PASSWORD=<your_neo4j_password>
```
2. Run the main ETL pipeline:
```bash
python -m graph.neo4j_pipeline
```

3. Test the Neo4j connection:
```bash
python -m graph.client_tester
```
4. Delete all nodes and relationships:
```bash
python -m graph.neo4j.neo4j_deletions
```

---

## Expected Data Formats

The `graph/` module expects input data in specific formats to ensure proper insertion
into the Neo4j graph database. The main data types handled are neighborhoods,
places, roads, intersections, and reviews.

### 1. Neighborhoods
- **Type:** `geopandas.GeoDataFrame`  
- **Required columns:**
  - `neighborhood_id` (unique identifier)
  - `name` (neighborhood name)
  - `area_km2` (area in square kilometers)
  - `average_monthly_income`  
  - `literacy_rate`  
  - `population_with_income`  
  - `total_literate_population`  
  - `total_private_households`  
  - `total_resident_population`  
  - `centroid_lat`, `centroid_lon` (coordinates of the neighborhood centroid)

### 2. Places
- **Type:** `geopandas.GeoDataFrame` 
- **Required columns:**
  - `place_id` (unique identifier)
  - `name`  
  - `rating`  
  - `num_reviews`  
  - `type` (business type or POI category)  
  - `latitude`, `longitude`  

### 3. Roads
- **Type:** `geopandas.GeoDataFrame`  
- **Required columns:**
  - `road_id` (unique identifier)  
  - `name`  
  - `highway` (type, e.g., residential, primary)  
  - `length` (meters)  
  - `oneway` (boolean)  
  - `maxspeed`  
  - `u`, `v` (start and end node IDs)  
  - `osmid` (optional OpenStreetMap ID)

### 4. Intersections
- **Type:** `geopandas.GeoDataFrame`  
- **Required columns:**
  - `osmid` (unique identifier)  
  - `geometry` (shapely `Point`)  
  - `highway` (type)  
  - `street_count` (number of connecting streets)

### 5. Reviews
- **Type:** `pandas.DataFrame`  
- **Required columns:**
  - `review_id` (unique identifier)  
  - `author`  
  - `rating`  
  - `text`  
  - `date` (optional, date of the review)

> **Note:** All IDs should be unique to avoid duplicate nodes in Neo4j.  
> Spatial data should be in the same coordinate reference system (CRS) for proper joins.

---

## Nodes and Relationships

### Nodes
- **Neighborhood**  
  Represents a city neighborhood with attributes such as:  
  - `neighborhood_id` (unique identifier)  
  - `name`  
  - `area_km2`  
  - `population`  
  - socioeconomic indicators (e.g., income, literacy rate)  

- **Place**  
  Represents a business or point of interest (e.g., pet shops) with attributes:  
  - `place_id` (unique identifier)  
  - `name`  
  - `rating`  
  - `num_reviews`  
  - `type` (business category)  
  - `latitude`, `longitude`  

- **Road**  
  Represents a street segment with attributes:  
  - `road_id` (unique identifier)  
  - `name`  
  - `highway` (e.g., residential, primary)  
  - `length` (meters)  
  - `oneway` (boolean)  
  - `maxspeed`  
  - `u`, `v` (start and end node IDs)  
  - `osmid`  

- **Intersection**  
  Represents road intersections or nodes in the street network with attributes:  
  - `osmid` (unique identifier)  
  - `geometry` (point location)  
  - `street_count` (number of connecting streets)  

- **Review**  
  Represents a user review with attributes:  
  - `review_id` (unique identifier)  
  - `author`  
  - `rating`  
  - `text`  
  - `date` (optional)  

### Relationships

- **(:Neighborhood)-[:CONTAINS]->(:Place)**  
  A neighborhood contains one or more places (businesses or points of interest).

- **(:Neighborhood)-[:CONTAINS]->(:Road)**  
  A neighborhood contains one or more road segments within its boundaries.

- **(:Road)-[:CONTAINS]->(:Place)**  
  A road segment contains one or more places located along it (optional, if needed for spatial queries).

- **(:Place)-[:NEAR]->(:Intersection)**  
  A place is geographically near an intersection.

- **(:Place)-[:HAS_REVIEW]->(:Review)**  
  A place has one or more user reviews describing customer experiences.

- **(:Intersection)-[:ROAD]->(:Intersection)**  
  An intersection is connected to another intersection via a road segment.

> **Note:** Relationships are directional, reflecting real-world connections.  
> Neighborhoods contain places and roads, roads can contain places and intersections are connected via ROAD relationships, and places are linked to reviews and nearby intersections.

---

## Architecture and Module Responsibilities
### Folders
- **neo4j/**: contains modules related to Neo4j operations.
- **spatial/**: contains modules for spatial processing and assigning spatial relationships.

### Modules
- `neo4j_pipeline.py`: main ETL pipeline for processing data and building the Neo4j graph.
- `client_tester.py`: script to test Neo4j connection and run sample queries.
- `neo4j/connector.py`: manages Neo4j connection and index creation.
- `neo4j/insert_nodes.py`: handles batch insertion of nodes (neighborhoods, places, roads, intersections, reviews).
- `neo4j/insert_relationships.py`: handles batch insertion of relationships between nodes.
- `neo4j/neo4j_deletions.py`: utility to delete all nodes and relationships in batches.
- `spatial/assign_spatial_attributes.py`: assigns spatial relationships between geographic entities.

---

## Best Practices and Performance Tips

When working with the Neo4j graph in this project, following best practices
ensures efficiency, correctness, and maintainability.

- **Batch Operations:** Insert nodes and relationships in batches using `UNWIND` to improve performance.  
- **Unique Identifiers:** Ensure each node has a unique ID (`neighborhood_id`, `place_id`, `road_id`, `osmid`, `review_id`) to avoid duplicates.  
- **CRS Consistency:** All GeoDataFrames should share the same coordinate reference system before spatial operations.  
- **Indexing:** Create indexes on key properties to speed up MERGE and lookup operations.  
- **Data Cleaning:** Remove duplicates and handle missing values before insertion.  
- **Testing:** Use `client_tester.py` to verify connections and sample queries.  
- **Incremental Updates:** For large datasets, insert data incrementally; use `neo4j_deletions.py` only when necessary.

> Following these practices ensures your Neo4j graph remains accurate, efficient, and maintainable,
> especially when dealing with large spatial datasets and complex relationships.
