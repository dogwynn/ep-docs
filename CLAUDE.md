# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Document analysis pipeline for DOJ Epstein library documents. Downloads PDFs, converts to text, extracts entities (people, organizations, locations, dates), builds co-occurrence networks, and provides interactive web visualizations.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
sudo apt install poppler-utils  # Required for pdftotext
```

## Pipeline Dependencies

Scripts must run in order. Later scripts depend on outputs from earlier ones.

```
download_epstein_pdfs.py → convert_pdfs.sh → extract_names.py → generate_network.py
                                           ↘ extract_locations.py ↘
                                           ↘ extract_organizations.py → cross_entity_analysis.py → entity_profiles.py
                                           ↘ extract_timeline.py ↗
                                           ↘ quote_attribution.py ↘
                                           ↘ alias_resolution.py  → preprocess_browser_data.py → serve.py

Utility scripts (run after convert_pdfs.sh, independent of main pipeline):
├── community_detection.py   (requires network CSVs)
├── ego_networks.py          (requires network CSVs)
├── topic_modeling.py        (requires text files)
├── document_similarity.py   (requires text files)
├── word_frequency.py        (requires text files)
├── key_phrases.py           (requires text files)
├── sentiment_analysis.py    (requires text files)
├── redaction_detection.py   (requires text files)
└── check_txt_files.py       (requires text files)
```

**Key dependencies:**
- `extracted_names.json` ← `extract_names.py` (required by most analysis scripts)
- `network_nodes_spacy.csv`, `network_edges_spacy.csv` ← `generate_network.py`
- `cross_entity_output/` ← `cross_entity_analysis.py` (requires all extraction scripts)
- `data/person_index.json` ← `preprocess_browser_data.py` (required by person browser)

## Common Workflows

```bash
# Quick: Run the web apps (after data exists)
python serve.py              # Person browser
python serve.py --network    # Network viewer
python serve.py --timeline   # Timeline explorer
python serve.py --map        # Geographic map

# Regenerate person browser data after analysis changes
python preprocess_browser_data.py && python serve.py

# Full pipeline from scratch
python download_epstein_pdfs.py -y
cd epstein_pdfs && ../convert_pdfs.sh && cd ..
python extract_names.py
python extract_locations.py
python extract_organizations.py
python extract_timeline.py
python generate_network.py
python alias_resolution.py --apply
python quote_attribution.py
python cross_entity_analysis.py
python entity_profiles.py
python preprocess_browser_data.py
```

## Architecture

### Data Flow
1. **Ingestion**: `download_epstein_pdfs.py` → `epstein_pdfs/*.pdf` → `convert_pdfs.sh` → `epstein_pdfs/*.txt`
2. **Extraction**: spaCy NER extracts entities from `.txt` files → JSON/CSV outputs
3. **Network**: `generate_network.py` builds co-occurrence graph from `extracted_names.json`
4. **Analysis**: Various scripts process extracted data for topic modeling, similarity, etc.
5. **Web**: `preprocess_browser_data.py` aggregates data into `data/` for client-side loading

### Web Applications
- `serve.py` - HTTP server (port 8080) with static files + API endpoints (`/api/quotes`, `/api/search`)
- `person_browser.html` - Single-page app for person profiles (loads from `data/persons/*.json`)
- `network_viewer.html` - WebGL network viz using Sigma.js/Graphology (loads CSVs directly)
- `timeline_explorer.html` - Temporal visualization with timeline, Gantt chart, event list (loads from `data/timeline/`)
- `geographic_map.html` - Leaflet.js map with location heatmap (loads from `data/map/`)
- `layout-worker.js` - Web Worker for force-directed layout (runs off main thread)

### Key Data Files
| File | Source | Used By |
|------|--------|---------|
| `extracted_names.json` | extract_names.py | generate_network.py, entity_profiles.py |
| `network_nodes_spacy.csv` | generate_network.py | network_viewer.html, preprocess_browser_data.py |
| `network_edges_spacy.csv` | generate_network.py | network_viewer.html, preprocess_browser_data.py |
| `alias_resolution_output/alias_mapping.csv` | alias_resolution.py | serve.py, preprocess_browser_data.py |
| `quote_attribution_output/all_quotes.csv` | quote_attribution.py | serve.py, preprocess_browser_data.py |
| `data/person_index.json` | preprocess_browser_data.py | person_browser.html |
| `data/persons/*.json` | preprocess_browser_data.py | person_browser.html |
| `data/timeline/events.json` | preprocess_browser_data.py | timeline_explorer.html |
| `data/map/locations.json` | preprocess_browser_data.py | geographic_map.html |

### Configuration Constants
Most scripts have configurable thresholds at the top of the file:
- `BASE_DIR` - Input directory (default `./epstein_pdfs`)
- `MIN_APPEARANCES` / `MIN_EDGE_WEIGHT` - Network filtering thresholds
- `MIN_*_MENTIONS` / `MIN_COOCCURRENCE` - Entity filtering thresholds

## Utility Scripts

Standalone analysis scripts that can run independently after text conversion:

| Script | Purpose | Output |
|--------|---------|--------|
| `community_detection.py` | Louvain clustering on co-occurrence network | `community_detection_output/` |
| `ego_networks.py` | Extract personal networks around individuals | `ego_networks_output/` |
| `topic_modeling.py` | BERTopic semantic topic discovery | `topic_modeling_output/` |
| `document_similarity.py` | Find duplicate/near-duplicate documents | `document_similarity_output/` |
| `word_frequency.py` | Vocabulary analysis and word clouds | `word_frequency_output/` |
| `key_phrases.py` | TF-IDF phrase extraction | `key_phrases_output/` |
| `sentiment_analysis.py` | VADER sentiment scoring by document | `sentiment_results.csv` |
| `redaction_detection.py` | Identify redacted sections | `redaction_detection_output/` |
| `check_txt_files.py` | Text quality analysis (real/empty/garbage) | `txt_analysis_results.csv` |
