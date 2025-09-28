"""
Graph Node Filtering Module
===========================

This module provides functionality to filter nodes in a heterogeneous graph
(`HeteroData`-like structure) based on specified attribute conditions. It uses
NumPy for vectorized string comparisons to efficiently handle attribute filtering.

Features
--------
- Filter nodes by matching attribute values.
- Return both indices of matching nodes and their expanded attributes.
- Includes a simple tester with mocked graph data.

Dependencies
------------
- numpy

Usage
-----
Example:

    from agents.agent_graph_navigator.operations.filter_nodes import filter_nodes_by_attributes

    # Suppose hetero_data is your HeteroData or similar structure
    extracted_nodes = {
        "Neighborhood": [{"name": "Vila Luzita"}],
        "Place": [{"type": "pet_store"}]
    }

    filtered_indices, result = filter_nodes_by_attributes(hetero_data, extracted_nodes)

    print("Filtered indices:", filtered_indices)
    print("Expanded nodes:", result)
"""

import numpy as np

def filter_nodes_by_attributes(hetero_data, extracted_nodes):
    """
    Filters nodes from the hetero_data based on the attributes in extracted_nodes.
    Uses vectorized operations with NumPy for string comparisons.

    Parameters
    ----------
    hetero_data : HeteroData
        Heterogeneous graph data containing nodes and attributes.
    extracted_nodes : dict
        Dictionary of node types and attribute filters.
        Example:
        {"Neighborhood": [{"name": "Vila Luzita"}], 
         "Place": [{"type": "pet_store"}], 
         "RAG": [{"text": "..."}]}

    Returns
    -------
    filtered_indices : dict
        Dictionary of node type to list of filtered node indices.
    result : dict
        Dictionary of node type to list of expanded nodes with attributes.
    """
    result = {}
    filtered_indices = {}

    # Iterate through each node type and its filters
    for ntype, filters in extracted_nodes.items():
        if ntype not in hetero_data.node_types:
            continue  # Skip if node type does not exist in the graph

        node_store = hetero_data[ntype]
        num_nodes = getattr(node_store, "num_nodes", 0)

        # Initialize mask as True for all nodes of this type
        mask = np.ones(num_nodes, dtype=bool)

        # Apply filters one by one
        for f in filters:
            local_mask = np.zeros(num_nodes, dtype=bool)
            for key, value in f.items():
                if key in node_store:
                    # Vectorized string/attribute comparison
                    attr_array = np.array(node_store[key], dtype=object)
                    local_mask |= (attr_array == value)
            mask &= local_mask  # Combine conditions with AND

        # Extract indices of nodes that satisfy the filters
        indices = np.where(mask)[0].tolist()
        filtered_indices[ntype] = indices

        # Expand attributes for the filtered nodes
        expanded = [
            {
                "type": ntype,
                "attributes": {k: node_store[k][idx] for k in node_store.keys() if k != "num_nodes"}
            }
            for idx in indices
        ]
        if expanded:
            result[ntype] = expanded

    return filtered_indices, result

# --------------------------------------------------------
# Test block: run only if this file is executed directly
# --------------------------------------------------------

if __name__ == "__main__":
    class MockNodeStore(dict):
        """Mock node store simulating attributes of a node type."""
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            # num_nodes is derived from the length of the first attribute array
            self.num_nodes = len(next(iter(kwargs.values()))) if kwargs else 0

    class MockHeteroData(dict):
        """Mock heterogeneous graph data with node types."""
        @property
        def node_types(self):
            return list(self.keys())

    # Create a mock heterogeneous graph with Neighborhood and Place nodes
    hetero_data = MockHeteroData({
        "Neighborhood": MockNodeStore(
            name=["Vila Luzita", "Centro", "Jardim"],
            population=[20000, 50000, 30000],
        ),
        "Place": MockNodeStore(
            type=["pet_store", "vet_clinic", "pet_store"],
            name=["PetShop A", "VetClinic B", "PetShop C"],
        ),
    })

    # Example query: find places that are pet stores and neighborhood "Vila Luzita"
    extracted_nodes = {
        "Neighborhood": [{"name": "Vila Luzita"}],
        "Place": [{"type": "pet_store"}],
    }

    # Apply the filtering function
    filtered_indices, result = filter_nodes_by_attributes(hetero_data, extracted_nodes)

    # Print results for verification
    print("Filtered indices:", filtered_indices)
    print("Expanded nodes:", result)
