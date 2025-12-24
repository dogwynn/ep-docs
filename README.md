# Epstein Documents Analysis

Tools for downloading and analyzing documents from the DOJ Epstein library.

## Setup

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m spacy download en_core_web_sm
```

## Scripts

### download_epstein_pdfs.py

Downloads all PDFs from the DOJ Epstein library (Court Records, DOJ Disclosures, FOIA).

```bash
python download_epstein_pdfs.py          # Interactive mode
python download_epstein_pdfs.py -y       # Skip confirmation
python download_epstein_pdfs.py --download-saved  # Resume from saved URLs
```

### convert_pdfs.sh

Converts all PDF files to text using `pdftotext`. Text files are created alongside the original PDFs.

```bash
cd epstein_pdfs
../convert_pdfs.sh
```

Requires `pdftotext` (part of `poppler-utils`):
```bash
sudo apt install poppler-utils  # Debian/Ubuntu
```

### check_txt_files.py

Analyzes .txt files and classifies them as real text, empty, or garbage.

```bash
python check_txt_files.py ./epstein_pdfs
```

### sentiment_analysis.py

Performs sentiment analysis on text files using NLTK VADER.

```bash
python sentiment_analysis.py
```

| Variable | Default | Description |
|----------|---------|-------------|
| `BASE_DIR` | `./epstein_pdfs` | Directory containing .txt files |

### extract_names.py

Extracts person names from text files using spaCy NER with parallel processing.

```bash
python extract_names.py
```

| Variable | Default | Description |
|----------|---------|-------------|
| `BASE_DIR` | `./epstein_pdfs` | Directory containing .txt files |

### generate_network.py

Builds a co-occurrence network from extracted names and generates visualizations.

```bash
python extract_names.py      # Run first to extract names
python generate_network.py   # Then generate network
```

| Variable | Default | Description |
|----------|---------|-------------|
| `MIN_APPEARANCES` | `5` | Minimum file appearances to include a person |
| `MIN_EDGE_WEIGHT` | `3` | Minimum co-occurrences to show an edge |

## Output Files

| File | Description |
|------|-------------|
| `epstein_pdfs/` | Downloaded PDF files |
| `epstein_pdfs/pdf_urls.json` | Scraped PDF URLs (JSON format) |
| `epstein_pdfs/pdf_urls.csv` | Scraped PDF URLs with local paths (CSV format) |
| `txt_analysis_results.csv` | File quality report |
| `sentiment_results.csv` | Sentiment analysis results |
| `extracted_names.json` | Extracted person names from NER |
| `network_map_spacy.pdf` | Full network visualization |
| `network_map_top100.pdf` | Top 100 individuals network |
| `network_edges_spacy.csv` | Network edge list |
| `network_nodes_spacy.csv` | Network node list |
