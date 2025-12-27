#!/usr/bin/env python3
"""
Preprocess data for the person browser application.
Generates person-indexed JSON files for fast client-side loading.
"""

import json
import csv
import os
import re
from collections import defaultdict
from pathlib import Path

# Configuration
OUTPUT_DIR = Path("data")
PERSONS_DIR = OUTPUT_DIR / "persons"
MAX_QUOTES_PER_PERSON = 100
MAX_DOCUMENTS_PER_PERSON = 200

def safe_filename(name):
    """Convert a person name to a safe filename."""
    return re.sub(r'[^A-Z0-9]', '_', name.upper()).strip('_')

def load_alias_mapping():
    """Load alias to canonical name mapping."""
    alias_to_canonical = {}
    canonical_aliases = defaultdict(list)

    alias_file = Path("alias_resolution_output/alias_mapping.csv")
    if not alias_file.exists():
        print("Warning: alias_mapping.csv not found")
        return alias_to_canonical, canonical_aliases

    with open(alias_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            canonical = row['canonical'].strip()
            alias = row['alias'].strip()
            alias_to_canonical[alias] = canonical
            if alias not in canonical_aliases[canonical]:
                canonical_aliases[canonical].append(alias)

    print(f"Loaded {len(alias_to_canonical)} alias mappings")
    return alias_to_canonical, canonical_aliases

def load_network_nodes():
    """Load network nodes with appearance counts."""
    nodes = {}
    nodes_file = Path("network_nodes_spacy.csv")

    if not nodes_file.exists():
        print("Error: network_nodes_spacy.csv not found")
        return nodes

    with open(nodes_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row['name'].strip()
            appearances = int(row['appearances'])
            nodes[name] = appearances

    print(f"Loaded {len(nodes)} network nodes")
    return nodes

def load_network_edges():
    """Load network edges (co-occurrences)."""
    edges = defaultdict(lambda: defaultdict(int))
    edges_file = Path("network_edges_spacy.csv")

    if not edges_file.exists():
        print("Error: network_edges_spacy.csv not found")
        return edges

    with open(edges_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            source = row['from'].strip()
            target = row['to'].strip()
            weight = int(row['weight'])
            edges[source][target] = weight
            edges[target][source] = weight

    print(f"Loaded network edges")
    return edges

def load_profiles():
    """Load entity profiles."""
    profiles = {}
    profiles_file = Path("entity_profiles_output/all_profiles.json")

    if not profiles_file.exists():
        print("Warning: all_profiles.json not found")
        return profiles

    with open(profiles_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
        for profile in data:
            name = profile.get('name', '').strip()
            if name:
                profiles[name] = profile

    print(f"Loaded {len(profiles)} profiles")
    return profiles

def load_quotes(alias_to_canonical):
    """Load quotes and index by speaker (using canonical names)."""
    quotes_by_speaker = defaultdict(list)
    quotes_file = Path("quote_attribution_output/all_quotes.csv")

    if not quotes_file.exists():
        print("Warning: all_quotes.csv not found")
        return quotes_by_speaker

    with open(quotes_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            speaker = row.get('speaker', '').strip().upper()
            if not speaker or speaker == 'THE WITNESS':
                continue

            # Resolve alias to canonical name
            canonical = alias_to_canonical.get(speaker, speaker)

            quote_data = {
                'type': row.get('type', ''),
                'question': row.get('question', '')[:500] if row.get('question') else '',
                'answer': row.get('answer', '')[:500] if row.get('answer') else '',
                'quote': row.get('quote', '')[:500] if row.get('quote') else '',
                'file': row.get('filename', '')
            }

            # Only include if there's meaningful content
            if quote_data['answer'] or quote_data['quote']:
                quotes_by_speaker[canonical].append(quote_data)

    print(f"Loaded quotes for {len(quotes_by_speaker)} speakers")
    return quotes_by_speaker

def load_cross_entity_data():
    """Load cross-entity relationships."""
    person_orgs = defaultdict(list)
    person_locs = defaultdict(list)
    person_years = defaultdict(dict)

    # Person-Organization links
    po_file = Path("cross_entity_output/person_organization_links.csv")
    if po_file.exists():
        with open(po_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                person = row.get('person', '').strip()
                org = row.get('organization', '').strip()
                count = int(row.get('cooccurrences', 0))
                if person and org and count >= 3:
                    person_orgs[person].append({'name': org, 'count': count})

    # Person-Location links
    pl_file = Path("cross_entity_output/person_location_links.csv")
    if pl_file.exists():
        with open(pl_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                person = row.get('person', '').strip()
                loc = row.get('location', '').strip()
                count = int(row.get('cooccurrences', 0))
                if person and loc and count >= 3:
                    person_locs[person].append({'name': loc, 'count': count})

    # Person-Year links
    py_file = Path("cross_entity_output/person_year_links.csv")
    if py_file.exists():
        with open(py_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                person = row.get('person', '').strip()
                year = row.get('year', '').strip()
                count = int(row.get('cooccurrences', 0))
                if person and year:
                    person_years[person][year] = count

    print(f"Loaded cross-entity data for {len(person_orgs)} persons (orgs), {len(person_locs)} (locs)")
    return person_orgs, person_locs, person_years

def generate_person_index(nodes, canonical_aliases):
    """Generate the search index for all persons."""
    persons = []

    for name, appearances in sorted(nodes.items(), key=lambda x: -x[1]):
        aliases = canonical_aliases.get(name, [])
        persons.append({
            'name': name,
            'mentions': appearances,
            'aliases': aliases[:10]  # Limit aliases for index size
        })

    # Build reverse alias map for search
    alias_map = {}
    for name, aliases in canonical_aliases.items():
        for alias in aliases:
            alias_map[alias] = name

    return {
        'persons': persons,
        'alias_map': alias_map
    }

def generate_person_file(name, nodes, edges, profiles, quotes_by_speaker,
                         person_orgs, person_locs, person_years, canonical_aliases):
    """Generate a single person's data file."""

    appearances = nodes.get(name, 0)
    profile = profiles.get(name, {})

    # Get associates from edges
    associates = []
    if name in edges:
        for assoc_name, weight in sorted(edges[name].items(), key=lambda x: -x[1])[:50]:
            associates.append({
                'name': assoc_name,
                'weight': weight
            })

    # Get organizations
    organizations = sorted(person_orgs.get(name, []), key=lambda x: -x['count'])[:30]

    # Get locations
    locations = sorted(person_locs.get(name, []), key=lambda x: -x['count'])[:30]

    # Get timeline
    timeline = person_years.get(name, {})

    # Get quotes (limit to MAX_QUOTES_PER_PERSON)
    quotes = quotes_by_speaker.get(name, [])[:MAX_QUOTES_PER_PERSON]

    # Get documents from profile
    documents = profile.get('documents', [])[:MAX_DOCUMENTS_PER_PERSON]

    # Get aliases
    aliases = canonical_aliases.get(name, [])

    person_data = {
        'name': name,
        'mentions': appearances,
        'aliases': aliases,
        'associates': associates,
        'organizations': organizations,
        'locations': locations,
        'timeline': timeline,
        'quotes': quotes,
        'documents': documents,
        'quote_count': len(quotes_by_speaker.get(name, []))
    }

    return person_data

def main():
    print("=" * 60)
    print("Person Browser Data Preprocessor")
    print("=" * 60)

    # Create output directories
    OUTPUT_DIR.mkdir(exist_ok=True)
    PERSONS_DIR.mkdir(exist_ok=True)

    # Load all data
    print("\n[1/6] Loading alias mappings...")
    alias_to_canonical, canonical_aliases = load_alias_mapping()

    print("\n[2/6] Loading network nodes...")
    nodes = load_network_nodes()

    print("\n[3/6] Loading network edges...")
    edges = load_network_edges()

    print("\n[4/6] Loading entity profiles...")
    profiles = load_profiles()

    print("\n[5/6] Loading quotes...")
    quotes_by_speaker = load_quotes(alias_to_canonical)

    print("\n[6/6] Loading cross-entity data...")
    person_orgs, person_locs, person_years = load_cross_entity_data()

    # Generate search index
    print("\n" + "=" * 60)
    print("Generating search index...")
    index = generate_person_index(nodes, canonical_aliases)

    index_file = OUTPUT_DIR / "person_index.json"
    with open(index_file, 'w', encoding='utf-8') as f:
        json.dump(index, f)

    index_size = index_file.stat().st_size / 1024
    print(f"Saved search index: {index_file} ({index_size:.1f} KB)")
    print(f"  - {len(index['persons'])} persons")
    print(f"  - {len(index['alias_map'])} alias mappings")

    # Generate individual person files
    print("\n" + "=" * 60)
    print("Generating individual person files...")

    count = 0
    for name in nodes.keys():
        person_data = generate_person_file(
            name, nodes, edges, profiles, quotes_by_speaker,
            person_orgs, person_locs, person_years, canonical_aliases
        )

        safe_name = safe_filename(name)
        person_file = PERSONS_DIR / f"{safe_name}.json"

        with open(person_file, 'w', encoding='utf-8') as f:
            json.dump(person_data, f)

        count += 1
        if count % 500 == 0:
            print(f"  Generated {count} person files...")

    print(f"\nGenerated {count} person files in {PERSONS_DIR}")

    # Summary
    print("\n" + "=" * 60)
    print("PREPROCESSING COMPLETE")
    print("=" * 60)
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"  - person_index.json: Search index for all persons")
    print(f"  - persons/: Individual person data files")
    print("\nYou can now run serve.py to start the browser app.")

if __name__ == "__main__":
    main()
