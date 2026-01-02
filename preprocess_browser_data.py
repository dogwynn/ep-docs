#!/usr/bin/env python3
"""
Preprocess data for the person browser application.
Generates person-indexed JSON files for fast client-side loading.
"""

import json
import csv
import os
import re
import time
from collections import defaultdict
from pathlib import Path

try:
    from geopy.geocoders import Nominatim
    from geopy.extra.rate_limiter import RateLimiter
    GEOPY_AVAILABLE = True
except ImportError:
    GEOPY_AVAILABLE = False

# Configuration
OUTPUT_DIR = Path("data")
PERSONS_DIR = OUTPUT_DIR / "persons"
TIMELINE_DIR = OUTPUT_DIR / "timeline"
EVENTS_DIR = TIMELINE_DIR / "events"
MAP_DIR = OUTPUT_DIR / "map"
MAX_QUOTES_PER_PERSON = 100
MAX_DOCUMENTS_PER_PERSON = 200
MAX_PERSONS_IN_GANTT = 500
MAX_EVENTS_PER_YEAR = 5000
MAX_PERSONS_FOR_MAP = 500

# Geocoding configuration
GEOCODE_CACHE_FILE = Path("geocode_cache.json")

# Known false positive locations (legal abbreviations, artifacts)
FALSE_POSITIVE_LOCATIONS = {
    'esq', 'p.a.', 'p.c.', 'p.l.', 'n.a.', 'm.d.', 'j.d.', 'ph.d.',
    'llc', 'inc', 'corp', 'ltd', 'l.p.', 'l.l.c.', 'p.l.l.c.',
    's.d.', 'e.d.', 'n.d.', 'w.d.', 's.d.n.y.', 'e.d.n.y.', 's.ct',
    'cir', 'jr.', 'sr.', 'id.', 'ste', 'dkt', 'r. civ', 'no.',
    'honor', 'notice', 'reply', 'motion', 'order', 'subpoena',
    'exhibit', 'declaration', 'affidavit', 'deposition', 'complaint',
    'indictment', 'respectfully submitted', 'defendant', 'plaintiff',
    'the', 'a', 'an', 'of', 'in', 'on', 'at', 'to', 'for',
}

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

def generate_timeline_data():
    """Generate preprocessed timeline data for the timeline explorer."""
    print("\n" + "=" * 60)
    print("Generating Timeline Data")
    print("=" * 60)

    # Create output directories
    TIMELINE_DIR.mkdir(exist_ok=True)
    EVENTS_DIR.mkdir(exist_ok=True)

    # Step 1: Generate person_gantt.json from person_year_links.csv
    print("\n[Timeline 1/3] Generating person Gantt data...")
    person_years_raw = defaultdict(lambda: {"years": {}, "total": 0})

    py_file = Path("cross_entity_output/person_year_links.csv")
    if py_file.exists():
        with open(py_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                person = row.get('person', '').strip()
                year = row.get('year', '').strip()
                count = int(row.get('cooccurrences', 0))

                # Filter out noise (empty names, fragments, too short)
                if not person or len(person) < 3:
                    continue
                # Skip obvious artifacts
                if person.startswith('&') or person.startswith('-'):
                    continue

                try:
                    year_int = int(year)
                    if 1970 <= year_int <= 2025:
                        person_years_raw[person]["years"][year] = count
                        person_years_raw[person]["total"] += count
                except ValueError:
                    continue

    # Get top N persons by total mentions
    sorted_persons = sorted(
        person_years_raw.items(),
        key=lambda x: x[1]["total"],
        reverse=True
    )[:MAX_PERSONS_IN_GANTT]

    # Build gantt data
    gantt_persons = []
    for name, data in sorted_persons:
        years_dict = data["years"]
        if years_dict:
            year_keys = [int(y) for y in years_dict.keys()]
            gantt_persons.append({
                "name": name,
                "first_year": min(year_keys),
                "last_year": max(year_keys),
                "total": data["total"],
                "years": years_dict
            })

    gantt_data = {
        "persons": gantt_persons,
        "year_range": [1970, 2025],
        "generated": str(Path("cross_entity_output/person_year_links.csv").stat().st_mtime)
    }

    gantt_file = TIMELINE_DIR / "person_gantt.json"
    with open(gantt_file, 'w', encoding='utf-8') as f:
        json.dump(gantt_data, f)

    gantt_size = gantt_file.stat().st_size / 1024
    print(f"  Saved: {gantt_file} ({gantt_size:.1f} KB)")
    print(f"  - {len(gantt_persons)} persons included")

    # Step 2: Generate year_index.json from year_counts.csv
    print("\n[Timeline 2/3] Generating year index...")
    years_list = []
    total_events = 0

    yc_file = Path("timeline_extraction_output/year_counts.csv")
    if yc_file.exists():
        with open(yc_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                year = int(row['year'])
                mentions = int(row['mentions'])
                years_list.append({"year": year, "mentions": mentions})
                total_events += mentions

    year_index = {
        "years": sorted(years_list, key=lambda x: x['year']),
        "total_events": total_events
    }

    year_index_file = TIMELINE_DIR / "year_index.json"
    with open(year_index_file, 'w', encoding='utf-8') as f:
        json.dump(year_index, f)

    print(f"  Saved: {year_index_file}")
    print(f"  - {len(years_list)} years, {total_events:,} total events")

    # Step 3: Generate events/YYYY.json from all_dates.csv
    print("\n[Timeline 3/3] Generating per-year event files...")
    events_by_year = defaultdict(list)

    dates_file = Path("timeline_extraction_output/all_dates.csv")
    if dates_file.exists():
        with open(dates_file, 'r', encoding='utf-8', errors='replace') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    year = int(row.get('year', 0))
                    if year < 1970 or year > 2025:
                        continue

                    month = int(row.get('month', 0)) if row.get('month') else 0

                    event = {
                        "date": row.get('parsed', ''),
                        "original": row.get('original', ''),
                        "file": row.get('file', ''),
                        "context": row.get('context', '')[:300],  # Truncate context
                        "month": month
                    }

                    # Prioritize events with full dates (month != 0 and month != 1 for year-only)
                    events_by_year[year].append(event)

                except (ValueError, KeyError):
                    continue

    # Write per-year files
    year_files_written = 0
    for year, events in sorted(events_by_year.items()):
        # Sort by date, prioritizing full dates
        events_sorted = sorted(
            events,
            key=lambda e: (e['month'] == 0, e['date'])
        )[:MAX_EVENTS_PER_YEAR]

        year_data = {
            "year": year,
            "count": len(events_sorted),
            "total_in_year": len(events),
            "events": events_sorted
        }

        year_file = EVENTS_DIR / f"{year}.json"
        with open(year_file, 'w', encoding='utf-8') as f:
            json.dump(year_data, f)

        year_files_written += 1

    print(f"  Saved {year_files_written} year files in {EVENTS_DIR}")

    # Generate events index
    events_index = {
        "years": {
            str(year): {
                "count": len(events),
                "file": f"events/{year}.json"
            }
            for year, events in events_by_year.items()
        }
    }

    events_index_file = TIMELINE_DIR / "events_index.json"
    with open(events_index_file, 'w', encoding='utf-8') as f:
        json.dump(events_index, f)

    print(f"  Saved: {events_index_file}")

    print("\nTimeline data generation complete!")


def is_valid_location(name):
    """Check if a location name is likely to be a real place."""
    if not name or len(name) < 2:
        return False

    name_lower = name.lower().strip()

    # Check against known false positives
    if name_lower in FALSE_POSITIVE_LOCATIONS:
        return False

    # Skip if mostly numbers
    if sum(c.isdigit() for c in name) > len(name) * 0.5:
        return False

    # Skip if too short and all caps (likely acronym)
    if len(name) <= 3 and name.isupper():
        return False

    # Skip if contains obvious legal patterns
    legal_patterns = [r'\bv\.\b', r'\bvs\.\b', r'\bNo\.\s*\d', r'^\d+\s*$', r'^@']
    for pattern in legal_patterns:
        if re.search(pattern, name, re.IGNORECASE):
            return False

    return True


def normalize_location_name(name):
    """Normalize a location name for consistent matching."""
    # Title case
    normalized = name.strip().title()

    # Handle common variations
    replacements = {
        'The ': '',
        ' Of ': ' of ',
        ' The ': ' the ',
        'U.s.': 'United States',
        'U.s.a.': 'United States',
        'Usa': 'United States',
        'Ny': 'New York',
        'Nyc': 'New York City',
        'D.c.': 'Washington D.C.',
        'Dc': 'Washington D.C.',
    }

    for old, new in replacements.items():
        if normalized.startswith(old):
            normalized = new + normalized[len(old):]

    return normalized


def load_geocode_cache():
    """Load the geocoding cache from disk."""
    if GEOCODE_CACHE_FILE.exists():
        with open(GEOCODE_CACHE_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def save_geocode_cache(cache):
    """Save the geocoding cache to disk."""
    with open(GEOCODE_CACHE_FILE, 'w', encoding='utf-8') as f:
        json.dump(cache, f, indent=2)


def generate_map_data():
    """Generate preprocessed geographic map data with geocoding."""
    print("\n" + "=" * 60)
    print("Generating Geographic Map Data")
    print("=" * 60)

    if not GEOPY_AVAILABLE:
        print("Warning: geopy not installed. Run 'pip install geopy' for geocoding.")
        print("Skipping map data generation.")
        return

    # Create output directory
    MAP_DIR.mkdir(exist_ok=True)

    # Step 1: Load and filter locations
    print("\n[Map 1/5] Loading and filtering locations...")
    locations_file = Path("location_extraction_output/all_locations.csv")

    if not locations_file.exists():
        print(f"Warning: {locations_file} not found. Skipping map generation.")
        return

    locations = {}
    with open(locations_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row.get('location', '').strip()
            freq = int(row.get('mentions', row.get('frequency', 0)))
            loc_type = row.get('type', 'GPE')

            if is_valid_location(name) and freq >= 3:
                normalized = normalize_location_name(name)
                if normalized in locations:
                    locations[normalized]['frequency'] += freq
                else:
                    locations[normalized] = {
                        'name': normalized,
                        'frequency': freq,
                        'type': loc_type
                    }

    print(f"  Found {len(locations)} valid locations after filtering")

    # Step 2: Load geocode cache
    print("\n[Map 2/5] Loading geocode cache...")
    geocode_cache = load_geocode_cache()
    cached_count = sum(1 for loc in locations if loc.lower() in geocode_cache)
    print(f"  {cached_count}/{len(locations)} locations already cached")

    # Sort by frequency to prioritize important locations
    sorted_locs = sorted(locations.items(), key=lambda x: -x[1]['frequency'])

    # Step 3: Geocode uncached locations
    skip_geocoding = os.environ.get('SKIP_GEOCODING', '').lower() in ('1', 'true', 'yes')

    if skip_geocoding:
        print("\n[Map 3/5] Skipping geocoding (SKIP_GEOCODING=1)")
        print(f"  Using {len([k for k in geocode_cache if geocode_cache[k]])} cached coordinates")
    else:
        print("\n[Map 3/5] Geocoding locations...")
        geolocator = Nominatim(user_agent="epstein_doc_analysis_v1")
        geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1.1)

        geocoded_count = 0
        failed_count = 0

        for i, (name, data) in enumerate(sorted_locs):
            cache_key = name.lower()

            if cache_key in geocode_cache:
                continue

            try:
                # Try with USA context first for ambiguous names
                result = geocode(f"{name}, USA", timeout=10)
                if not result:
                    result = geocode(name, timeout=10)

                if result:
                    geocode_cache[cache_key] = {
                        'lat': result.latitude,
                        'lon': result.longitude,
                        'display_name': result.address
                    }
                    geocoded_count += 1
                else:
                    geocode_cache[cache_key] = None
                    failed_count += 1

                # Save cache periodically
                if (geocoded_count + failed_count) % 50 == 0:
                    save_geocode_cache(geocode_cache)
                    print(f"    Geocoded {geocoded_count}, failed {failed_count}, "
                          f"remaining ~{len(sorted_locs) - i - 1}")

            except Exception as e:
                geocode_cache[cache_key] = None
                failed_count += 1

        save_geocode_cache(geocode_cache)
        print(f"  Geocoded {geocoded_count} new locations, {failed_count} failed")

    # Step 4: Load person-location links
    print("\n[Map 4/5] Processing person-location links...")
    person_location_file = Path("cross_entity_output/person_location_links.csv")

    location_persons = defaultdict(list)
    person_locations = defaultdict(list)

    if person_location_file.exists():
        with open(person_location_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                person = row.get('person', '').strip()
                location = row.get('location', '').strip()
                cooccurrences = int(row.get('cooccurrences', 0))

                if not person or not location or cooccurrences < 2:
                    continue

                normalized_loc = normalize_location_name(location)
                cache_key = normalized_loc.lower()

                if cache_key in geocode_cache and geocode_cache[cache_key]:
                    coords = geocode_cache[cache_key]

                    location_persons[normalized_loc].append({
                        'person': person,
                        'cooccurrences': cooccurrences
                    })

                    person_locations[person].append({
                        'location': normalized_loc,
                        'lat': coords['lat'],
                        'lon': coords['lon'],
                        'cooccurrences': cooccurrences
                    })

    print(f"  Processed {len(location_persons)} locations with person links")
    print(f"  Processed {len(person_locations)} persons with location links")

    # Step 5: Generate output files
    print("\n[Map 5/5] Generating map data files...")

    # Build locations.json with geocoded coordinates
    geocoded_locations = []
    for name, data in sorted_locs:
        cache_key = name.lower()
        if cache_key in geocode_cache and geocode_cache[cache_key]:
            coords = geocode_cache[cache_key]
            geocoded_locations.append({
                'id': cache_key.replace(' ', '_'),
                'name': name,
                'lat': coords['lat'],
                'lon': coords['lon'],
                'mentions': data['frequency'],
                'type': data['type'],
                'persons_count': len(location_persons.get(name, []))
            })

    locations_output = {
        'locations': geocoded_locations,
        'total_locations': len(geocoded_locations),
        'total_mentions': sum(loc['mentions'] for loc in geocoded_locations)
    }

    with open(MAP_DIR / 'locations.json', 'w', encoding='utf-8') as f:
        json.dump(locations_output, f)
    print(f"  Saved locations.json ({len(geocoded_locations)} locations)")

    # Build location_index.json (location -> top persons)
    location_index = {}
    for loc_name, persons in location_persons.items():
        cache_key = loc_name.lower()
        if cache_key in geocode_cache and geocode_cache[cache_key]:
            coords = geocode_cache[cache_key]
            sorted_persons = sorted(persons, key=lambda x: -x['cooccurrences'])[:20]
            location_index[loc_name] = {
                'name': loc_name,
                'lat': coords['lat'],
                'lon': coords['lon'],
                'total_mentions': locations.get(loc_name, {}).get('frequency', 0),
                'top_persons': sorted_persons
            }

    with open(MAP_DIR / 'location_index.json', 'w', encoding='utf-8') as f:
        json.dump(location_index, f)
    print(f"  Saved location_index.json ({len(location_index)} entries)")

    # Build person_locations.json (top N persons -> their locations)
    person_totals = {p: sum(loc['cooccurrences'] for loc in locs)
                     for p, locs in person_locations.items()}
    top_persons = sorted(person_totals.items(), key=lambda x: -x[1])[:MAX_PERSONS_FOR_MAP]

    person_locations_output = {}
    for person, total in top_persons:
        locs = person_locations[person]
        sorted_locs = sorted(locs, key=lambda x: -x['cooccurrences'])[:30]
        person_locations_output[person] = {
            'total_locations': len(locs),
            'locations': sorted_locs
        }

    with open(MAP_DIR / 'person_locations.json', 'w', encoding='utf-8') as f:
        json.dump(person_locations_output, f)
    print(f"  Saved person_locations.json ({len(person_locations_output)} persons)")

    print("\nMap data generation complete!")


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

    # Generate timeline data
    generate_timeline_data()

    # Generate map data
    generate_map_data()

    # Summary
    print("\n" + "=" * 60)
    print("PREPROCESSING COMPLETE")
    print("=" * 60)
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"  - person_index.json: Search index for all persons")
    print(f"  - persons/: Individual person data files")
    print(f"  - timeline/: Timeline explorer data")
    print(f"  - map/: Geographic map data")
    print("\nYou can now run serve.py to start the browser app.")

if __name__ == "__main__":
    main()
