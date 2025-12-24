#!/usr/bin/env python3
"""
Name Extraction Script - Extract person names from text files using spaCy NER.

Processes all .txt files in parallel and saves extracted names to a JSON file
for use by the network generation script.

Setup:
    python -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    python -m spacy download en_core_web_sm
"""

from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import json

import spacy

# Configuration
BASE_DIR = Path("./epstein_pdfs")
OUTPUT_FILE = Path("./extracted_names.json")

# Global variable for worker processes
_worker_nlp = None


def _init_worker():
    """Initialize spaCy model for each worker process."""
    global _worker_nlp
    _worker_nlp = spacy.load('en_core_web_sm')
    _worker_nlp.max_length = 2_000_000


def extract_persons(text, nlp):
    """Extract person names from text using spaCy NER."""
    # Process in chunks if text is very long
    max_chunk = 100000
    persons = set()

    for i in range(0, len(text), max_chunk):
        chunk = text[i:i + max_chunk]
        try:
            doc = nlp(chunk)
            for ent in doc.ents:
                if ent.label_ == 'PERSON':
                    # Clean up the name
                    name = ent.text.strip()
                    # Filter out single words (likely false positives)
                    if ' ' in name and len(name) >= 5:
                        # Normalize whitespace
                        name = ' '.join(name.split())
                        persons.add(name)
        except Exception:
            continue

    return persons


def normalize_name(name):
    """Normalize a person name."""
    # Remove possessive suffixes
    if name.endswith("'s"):
        name = name[:-2]
    elif name.endswith("'"):
        name = name[:-1]
    return name.strip()


def filter_persons(persons):
    """Additional filtering for person names."""
    filtered = set()

    # Keywords that indicate non-person entities
    exclude_keywords = [
        'county', 'city', 'state', 'court', 'district', 'university',
        'college', 'school', 'hospital', 'bank', 'company', 'corp',
        'inc', 'llc', 'ltd', 'foundation', 'institute', 'center',
        'department', 'office', 'bureau', 'agency', 'committee',
        'airport', 'street', 'avenue', 'boulevard', 'road', 'drive',
        'island', 'beach', 'park', 'lake', 'river', 'mountain',
        'amendment', 'circuit', 'airlines', 'airways', 'hotel'
    ]

    for name in persons:
        # Normalize the name first
        name = normalize_name(name)

        name_lower = name.lower()
        # Skip if contains exclude keywords
        if any(kw in name_lower for kw in exclude_keywords):
            continue
        # Skip if all uppercase (likely acronym or header)
        if name.isupper():
            continue
        # Skip if contains digits
        if any(c.isdigit() for c in name):
            continue
        # Skip very long "names" (likely parsing errors)
        if len(name) > 50:
            continue
        # Skip if too short after normalization
        if len(name) < 5 or ' ' not in name:
            continue
        filtered.add(name.upper())

    return filtered


def process_file(txt_file):
    """Process a single file - designed for parallel execution."""
    global _worker_nlp
    try:
        text = txt_file.read_text(encoding='utf-8', errors='ignore')
        persons = extract_persons(text, _worker_nlp)
        persons = filter_persons(persons)
        if persons:
            return (str(txt_file), list(persons))
    except Exception:
        pass
    return None


def main():
    print("=== NAME EXTRACTION (spaCy NER) ===\n")

    # Find all .txt files
    print("Scanning for .txt files...")
    txt_files = list(BASE_DIR.rglob("*.txt"))
    print(f"Found {len(txt_files)} .txt files\n")

    if not txt_files:
        print(f"No .txt files found in {BASE_DIR}")
        return

    # Process all files and extract names in parallel
    print("Extracting person names from files...")
    file_persons = {}  # file_path -> list of persons
    total_files = len(txt_files)

    # Determine number of workers
    num_workers = min(multiprocessing.cpu_count(), total_files) if total_files > 0 else 1
    print(f"Using {num_workers} parallel workers\n")

    completed = 0

    with ProcessPoolExecutor(max_workers=num_workers, initializer=_init_worker) as executor:
        futures = {executor.submit(process_file, f): f for f in txt_files}

        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                file_path, persons = result
                file_persons[file_path] = persons

            completed += 1
            if completed % 100 == 0 or completed == total_files:
                print(f"Progress: {completed}/{total_files} files ({100*completed/total_files:.1f}%)")

    print(f"\nFiles with persons found: {len(file_persons)}")

    # Count unique persons
    all_persons = set()
    for persons in file_persons.values():
        all_persons.update(persons)
    print(f"Unique persons found: {len(all_persons)}")

    # Save to JSON
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(file_persons, f, indent=2)

    print(f"\nExtracted names saved to: {OUTPUT_FILE}")
    print("Extraction complete!")


if __name__ == "__main__":
    main()
