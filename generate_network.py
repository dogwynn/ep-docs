#!/usr/bin/env python3
"""
Network Generation Script - Build co-occurrence network from extracted names.

Loads extracted names from JSON file and generates network visualizations
and edge/node CSV files.

Usage:
    python extract_names.py   # Run first to extract names
    python generate_network.py  # Then run this to generate network
"""

from pathlib import Path
from collections import defaultdict
from itertools import combinations
import json

import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd

# Configuration
INPUT_FILE = Path("./extracted_names.json")
OUTPUT_PDF = Path("./network_map_spacy.pdf")
OUTPUT_TOP100_PDF = Path("./network_map_top100.pdf")
OUTPUT_EDGES_CSV = Path("./network_edges_spacy.csv")
OUTPUT_NODES_CSV = Path("./network_nodes_spacy.csv")

MIN_APPEARANCES = 5  # Minimum file appearances to include a person
MIN_EDGE_WEIGHT = 3  # Minimum co-occurrences to show an edge


def main():
    print("=== NETWORK GENERATION ===\n")

    # Load extracted names
    if not INPUT_FILE.exists():
        print(f"Error: {INPUT_FILE} not found.")
        print("Run extract_names.py first to extract person names.")
        return

    print(f"Loading extracted names from {INPUT_FILE}...")
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        file_persons_raw = json.load(f)

    # Convert lists back to sets for processing
    file_persons = {k: set(v) for k, v in file_persons_raw.items()}

    print(f"Loaded data for {len(file_persons)} files\n")

    # Count total occurrences of each person across all files
    person_counts = defaultdict(int)
    for persons in file_persons.values():
        for person in persons:
            person_counts[person] += 1

    print(f"Unique persons found: {len(person_counts)}")

    # Filter to persons that appear in at least N files
    frequent_persons = {p for p, count in person_counts.items() if count >= MIN_APPEARANCES}
    print(f"Persons appearing in >= {MIN_APPEARANCES} files: {len(frequent_persons)}\n")

    # Build co-occurrence network
    print("Building co-occurrence network...")
    edge_weights = defaultdict(int)

    for file_path, persons in file_persons.items():
        # Filter to only frequent persons
        persons_in_file = persons & frequent_persons

        if len(persons_in_file) >= 2:
            # Create all pairs (sorted to ensure consistent ordering)
            for p1, p2 in combinations(sorted(persons_in_file), 2):
                edge_weights[(p1, p2)] += 1

    print(f"Total unique edges (co-occurrences): {len(edge_weights)}")
    print(f"Total co-occurrence instances: {sum(edge_weights.values())}\n")

    if edge_weights:
        # Create edge dataframe
        edges_df = pd.DataFrame([
            {'from': p1, 'to': p2, 'weight': w}
            for (p1, p2), w in edge_weights.items()
        ]).sort_values('weight', ascending=False)

        # Show top co-occurrences
        print("--- Top 20 Most Frequent Co-occurrences ---")
        for i, row in edges_df.head(20).iterrows():
            print(f"{edges_df.index.get_loc(i)+1:2d}. {row['from']} <-> {row['to']} ({row['weight']} files)")

        # Filter edges for visualization
        edges_filtered = edges_df[edges_df['weight'] >= MIN_EDGE_WEIGHT]
        print(f"\nEdges with weight >= {MIN_EDGE_WEIGHT}: {len(edges_filtered)}")

        # Get nodes in filtered network
        nodes_in_network = set(edges_filtered['from']) | set(edges_filtered['to'])

        # Create node attributes
        nodes_df = pd.DataFrame([
            {'name': p, 'appearances': person_counts[p]}
            for p in nodes_in_network
        ]).sort_values('appearances', ascending=False)

        print(f"Nodes in network: {len(nodes_df)}\n")

        # Create NetworkX graph
        G = nx.Graph()

        # Add nodes with attributes
        for _, row in nodes_df.iterrows():
            G.add_node(row['name'], appearances=row['appearances'])

        # Add edges with weights
        for _, row in edges_filtered.iterrows():
            G.add_edge(row['from'], row['to'], weight=row['weight'])

        # Network statistics
        print("--- Network Statistics ---")
        print(f"Nodes: {G.number_of_nodes()}")
        print(f"Edges: {G.number_of_edges()}")
        print(f"Density: {nx.density(G):.4f}")

        # Centrality measures
        degree_cent = dict(G.degree())

        print("\n--- Top 15 Most Connected Individuals (by degree) ---")
        top_degree = sorted(degree_cent.items(), key=lambda x: x[1], reverse=True)[:15]
        for i, (name, deg) in enumerate(top_degree, 1):
            print(f"{i:2d}. {name} (connections: {deg})")

        # Create visualization
        print("\nGenerating network visualization...")

        # Calculate figure size based on number of nodes
        n_nodes = G.number_of_nodes()
        fig_size = max(20, min(50, n_nodes ** 0.5 * 2))

        fig, ax = plt.subplots(figsize=(fig_size, fig_size))

        # Layout
        pos = nx.spring_layout(G, k=2/n_nodes**0.5, iterations=50, seed=42)

        # Node sizes based on appearances (log scale)
        node_sizes = [100 + 50 * (person_counts[n] ** 0.5) for n in G.nodes()]

        # Edge widths based on weight
        edge_widths = [0.5 + G[u][v]['weight'] * 0.3 for u, v in G.edges()]

        # Draw edges
        nx.draw_networkx_edges(G, pos, width=edge_widths, alpha=0.4, edge_color='gray', ax=ax)

        # Draw nodes
        nx.draw_networkx_nodes(G, pos, node_size=node_sizes, node_color='steelblue', alpha=0.7, ax=ax)

        # Draw labels
        nx.draw_networkx_labels(G, pos, font_size=6, ax=ax)

        ax.set_title(f"Network of Individuals in Documents\n"
                     f"Nodes: {G.number_of_nodes()} | Edges: {G.number_of_edges()} | "
                     f"Edge = co-occurrence in same file (weight >= {MIN_EDGE_WEIGHT})",
                     fontsize=14)
        ax.axis('off')

        plt.tight_layout()
        plt.savefig(OUTPUT_PDF, format='pdf', dpi=150, bbox_inches='tight')
        plt.close()

        print(f"\nNetwork map saved to: {OUTPUT_PDF}")

        # Generate top 100 visualization
        print("\nGenerating top 100 individuals network visualization...")

        top_100_nodes = [name for name, _ in sorted(degree_cent.items(), key=lambda x: x[1], reverse=True)[:100]]
        G_top100 = G.subgraph(top_100_nodes).copy()

        if G_top100.number_of_nodes() > 0:
            n_nodes_top = G_top100.number_of_nodes()
            fig_size_top = 40  # Large fixed size for clarity

            fig2, ax2 = plt.subplots(figsize=(fig_size_top, fig_size_top))

            # Higher k value = more spacing between nodes
            pos_top = nx.spring_layout(G_top100, k=8/n_nodes_top**0.5, iterations=200, seed=42)

            node_sizes_top = [150 + 80 * (person_counts[n] ** 0.5) for n in G_top100.nodes()]
            edge_widths_top = [0.8 + G_top100[u][v]['weight'] * 0.4 for u, v in G_top100.edges()]

            nx.draw_networkx_edges(G_top100, pos_top, width=edge_widths_top, alpha=0.5, edge_color='gray', ax=ax2)
            nx.draw_networkx_nodes(G_top100, pos_top, node_size=node_sizes_top, node_color='steelblue', alpha=0.8, ax=ax2)
            nx.draw_networkx_labels(G_top100, pos_top, font_size=8, ax=ax2)

            ax2.set_title(f"Top 100 Most Connected Individuals\n"
                          f"Nodes: {G_top100.number_of_nodes()} | Edges: {G_top100.number_of_edges()}",
                          fontsize=14)
            ax2.axis('off')

            plt.tight_layout()
            plt.savefig(OUTPUT_TOP100_PDF, format='pdf', dpi=150, bbox_inches='tight')
            plt.close()

            print(f"Top 100 network map saved to: {OUTPUT_TOP100_PDF}")

        # Save CSVs
        edges_df.to_csv(OUTPUT_EDGES_CSV, index=False)
        nodes_df.to_csv(OUTPUT_NODES_CSV, index=False)
        print(f"Edge list saved to: {OUTPUT_EDGES_CSV}")
        print(f"Node list saved to: {OUTPUT_NODES_CSV}")

    else:
        print("No co-occurrences found. Try lowering the MIN_APPEARANCES threshold.")

    print("\nNetwork generation complete!")


if __name__ == "__main__":
    main()
