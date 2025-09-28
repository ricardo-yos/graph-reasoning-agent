"""
Graph Relations Refiner
=======================

This module provides a function `filter_nodes_by_relations` to refine filtered nodes in a 
heterogeneous graph based on relations between node types. It uses NumPy for 
vectorized operations and returns updated filtered indices and expanded nodes. 
A simple test block with mock data is included to validate the behavior.

Dependencies
------------
- numpy
- typing (standard library)

Usage
-----
Example:

    from agents.agent_graph_navigator.operations.graph_relations import filter_nodes_by_relations

    filtered_indices, expanded_nodes = filter_nodes_by_relations(
        hetero_data, relation_keys, extracted_nodes, filtered_indices, result
    )
"""

import numpy as np

def filter_nodes_by_relations(hetero_data, relation_keys, extracted_nodes, filtered_indices, result):
    """
    Applies relations between node types to refine the filtered nodes.
    Uses NumPy for vectorized operations.

    Parameters
    ----------
    hetero_data : HeteroData or dict-like
        Heterogeneous graph data containing nodes and edge indices.
        Expected to support `hetero_data[edge_type].edge_index` 
        and `hetero_data[node_type]` for attributes.
    relation_keys : dict
        Dictionary defining the relations to apply. 
        Example: {"User": {"Review": "wrote"}}
    extracted_nodes : dict
        Extracted nodes from the graph, keyed by node type.
    filtered_indices : dict
        Current filtered indices for each node type.
        Example: {"User": [0, 1], "Review": [0, 2]}
    result : dict
        Current expanded nodes with attributes.

    Returns
    -------
    filtered_indices : dict
        Updated filtered indices after applying relations.
    result : dict
        Updated expanded nodes after applying relations.
    """
    for src_type, targets in relation_keys.items():
        # Skip if no source nodes were filtered
        if src_type not in filtered_indices:
            continue

        for tgt_type, rel in targets.items():
            # Skip if target type not extracted
            if tgt_type not in extracted_nodes:
                continue

            edge_type = (src_type, rel, tgt_type)

            # Skip if relation not found in the graph
            if edge_type not in hetero_data.edge_types:
                continue

            # Extract edge indices (src → tgt connections)
            edge_index = hetero_data[edge_type].edge_index  # shape [2, num_edges]
            src_nodes = np.array(edge_index[0])
            tgt_nodes = np.array(edge_index[1])

            # Get the currently filtered source indices
            src_indices = np.array(filtered_indices[src_type])

            # Boolean mask: which edges connect from filtered sources
            connected_mask = np.isin(src_nodes, src_indices)

            # Collect target nodes connected to the filtered sources
            connected_tgt = tgt_nodes[connected_mask]

            # Refine the filtered indices of target nodes
            filtered_indices[tgt_type] = [
                idx for idx in filtered_indices[tgt_type] if idx in connected_tgt
            ]

            # Update result with attributes of refined target nodes
            node_store = hetero_data[tgt_type]
            expanded = [
                {
                    "type": tgt_type,
                    "attributes": {
                        k: node_store[k][idx] for k in node_store.keys() if k != "num_nodes"
                    },
                }
                for idx in filtered_indices[tgt_type]
            ]
            result[tgt_type] = expanded

    return filtered_indices, result

# --------------------------------------------------------
# Test block: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    class MockEdgeStore:
        """Mock edge store simulating PyG EdgeStore behavior."""

        def __init__(self, edge_index):
            self.edge_index = edge_index

    class MockHeteroData(dict):
        """Mock heterogeneous graph data with edge types."""

        @property
        def edge_types(self):
            return [("User", "wrote", "Review")]

    # Create a mock heterogeneous graph
    hetero_data = MockHeteroData({
        ("User", "wrote", "Review"): MockEdgeStore(
            np.array([[0, 1, 2], [0, 1, 2]])  # Edges: User 0→Review 0, User 1→Review 1, User 2→Review 2
        ),
        "Review": {
            "text": ["Good service", "Bad experience", "Average"],
            "rating": [5, 1, 3],
            "review_id": [101, 102, 103],
        },
    })

    relation_keys = {"User": {"Review": "wrote"}}
    extracted_nodes = {"Review": [0, 1, 2]}
    filtered_indices = {"User": [1], "Review": [0, 1, 2]}
    result = {}

    # Apply relation filtering
    filtered, expanded = filter_nodes_by_relations(
        hetero_data, relation_keys, extracted_nodes, filtered_indices, result
    )

    # Debug output
    print("Filtered indices:", filtered)
    print("Expanded nodes:", expanded)