"""
Neo4j to PyTorch Geometric HeteroData Exporter
==============================================

This script connects to a Neo4j graph database, exports its nodes and relationships
to a NetworkX graph, converts the graph into a PyTorch Geometric HeteroData object,
saves the HeteroData to disk, and provides a function to inspect its contents.

Key Features
------------
- Node attributes are preserved and stored as lists.
- Edge relationships are keyed by (source_type, relation_type, destination_type).
- Supports readable inspection of nodes and edges for debugging or exploration.

Dependencies
------------
- py2neo
- networkx
- torch
- torch_geometric
- numpy

Usage
-----
Run the script from the command line:
    python export_neo4j_to_heterodata.py
"""

import os
import torch
import numpy as np
import networkx as nx
from py2neo import Graph
from config.env_loader import load_env, get_neo4j_credentials
from torch_geometric.data import HeteroData
from config.paths import MODELS_DIR

# -----------------------------
# Export Neo4j graph to NetworkX
# -----------------------------
def export_to_networkx(graph) -> nx.Graph:
    """
    Export nodes and relationships from a Neo4j graph into a NetworkX graph.

    Parameters
    ----------
    graph : py2neo.Graph
        A connected Neo4j graph instance.

    Returns
    -------
    G : nx.Graph
        A NetworkX graph containing nodes with their attributes and edges with relationship types.

    Notes
    -----
    - Node types are stored under the attribute 'ntype'.
    - Edge types are stored under the attribute 'rel_type'.
    """
    G = nx.Graph()

    # Add nodes from Neo4j
    for record in graph.run("MATCH (n) RETURN id(n) AS id, labels(n) AS labels, properties(n) AS props"):
        nid = record["id"]
        label = record["labels"][0]
        props = record["props"]
        G.add_node(nid, ntype=label, **props)

    # Add edges from Neo4j
    for record in graph.run(
        "MATCH (n)-[r]->(m) RETURN id(n) AS source, id(m) AS target, type(r) AS type"
    ):
        G.add_edge(record["source"], record["target"], rel_type=record["type"])

    return G

# -----------------------------
# Convert NetworkX graph to HeteroData
# -----------------------------
def networkx_to_heterodata(G, device="cpu") -> HeteroData:
    """
    Convert a NetworkX graph into a PyTorch Geometric HeteroData object.

    Parameters
    ----------
    G : nx.Graph
        The NetworkX graph to convert.
    device : str, optional
        Device to store PyTorch tensors ('cpu' or 'cuda'), by default "cpu".

    Returns
    -------
    hetero_data : HeteroData
        A heterogeneous graph suitable for PyTorch Geometric. Node attributes
        are stored as lists for each node type, and edges are stored as tensors
        under the 'edge_index' attribute for each edge type.

    Notes
    -----
    - Node attributes are kept in lists (not tensors) to maintain readability.
    - Edge types are keyed by (source_type, relation_type, destination_type).
    """
    hetero_data = HeteroData()
    id2idx = {}

    # Identify node types
    node_attrs = nx.get_node_attributes(G, "ntype")
    node_types = set(node_attrs.values())

    for ntype in node_types:
        # Extract nodes of this type
        nodes_of_type = [(nid, G.nodes[nid]) for nid, attr in G.nodes(data=True) if attr.get("ntype") == ntype]
        if not nodes_of_type:
            continue

        # Map original node IDs to indices
        nids = np.array([nid for nid, _ in nodes_of_type])
        id2idx[ntype] = {nid: idx for idx, nid in enumerate(nids)}
        hetero_data[ntype].num_nodes = len(nids)

        # Convert node attributes to lists
        keys = list(nodes_of_type[0][1].keys())
        for key in keys:
            values = np.array([attr[key] for _, attr in nodes_of_type])
            hetero_data[ntype][key] = values.tolist()

    # Process edges
    edge_dict = {}
    edges_data = np.array([(src, dst, G.edges[src, dst].get("rel_type", "rel")) for src, dst in G.edges()])
    if len(edges_data) > 0:
        src_nodes = edges_data[:, 0].astype(int)
        dst_nodes = edges_data[:, 1].astype(int)
        rel_types = edges_data[:, 2]

        for src, dst, rel in zip(src_nodes, dst_nodes, rel_types):
            src_type = G.nodes[src]["ntype"]
            dst_type = G.nodes[dst]["ntype"]
            key = (src_type, rel, dst_type)

            if key not in edge_dict:
                edge_dict[key] = [[], []]

            edge_dict[key][0].append(id2idx[src_type][src])
            edge_dict[key][1].append(id2idx[dst_type][dst])

    # Convert edge lists to tensors
    for key, (src_list, dst_list) in edge_dict.items():
        hetero_data[key].edge_index = torch.tensor(
            np.vstack([np.array(src_list), np.array(dst_list)]),
            dtype=torch.long,
            device=device
        )

    return hetero_data

# -----------------------------
# Save HeteroData to disk
# -----------------------------
def save_hetero_graph(hetero_data, filename="neo4j_heterodata.pt"):
    """
    Save a HeteroData object to disk as a PyTorch file.

    Parameters
    ----------
    hetero_data : HeteroData
        The heterogeneous graph to save.
    filename : str, optional
        File name for saving the graph, by default "neo4j_heterodata.pt".

    Returns
    -------
    save_path : str
        Full path where the HeteroData file is saved.
    """
    os.makedirs(MODELS_DIR, exist_ok=True)
    save_path = os.path.join(MODELS_DIR, filename)
    torch.save(hetero_data, save_path)
    print(f"Heterogeneous graph saved at: {save_path}")
    return save_path

# -----------------------------
# Inspect HeteroData contents
# -----------------------------
def inspect_hetero_data(hetero_data):
    """
    Print the nodes and edges stored in a HeteroData object in a readable format.

    Parameters
    ----------
    hetero_data : HeteroData
        The graph to inspect.
    """
    # Iterate through node types
    for ntype in hetero_data.node_types:
        node_store = hetero_data[ntype]
        num_nodes = node_store.num_nodes
        print(f"\nNode type: {ntype} ({num_nodes} nodes)")
        for idx in range(num_nodes):
            print(f"\nNode {idx}:")
            for key, value in node_store.items():
                if key == "num_nodes":
                    continue
                print(f"  {key}: {value[idx]}")

    # Iterate through edge types
    for et in hetero_data.edge_types:
        ei = hetero_data[et].edge_index
        print(f"\nEdge type: {et}, shape: {ei.shape}")

# -----------------------------
# Main script
# -----------------------------
if __name__ == "__main__":
    # Connect to Neo4j
    print("Connecting to Neo4j...")
    load_env()
    url, user, password = get_neo4j_credentials()
    graph = Graph(url, auth=(user, password))

    # Export graph and convert to HeteroData
    print("Exporting graph to NetworkX...")
    G = export_to_networkx(graph)

    print("Converting NetworkX to HeteroData...")
    hetero_data = networkx_to_heterodata(G, device="cpu")

    # Save and inspect
    print("Saving HeteroData...")
    save_hetero_graph(hetero_data)

    print("Inspecting HeteroData:")
    inspect_hetero_data(hetero_data)

    print("\nDone!")
