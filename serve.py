#!/usr/bin/env python3
"""HTTP server for the network viewer and person browser applications."""
import http.server
import socketserver
import webbrowser
import os
import json
import csv
import sys
from urllib.parse import urlparse, parse_qs
from pathlib import Path

PORT = 8080

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Cache for quotes data
_quotes_cache = None

def load_quotes():
    """Load and cache all quotes for API queries."""
    global _quotes_cache
    if _quotes_cache is not None:
        return _quotes_cache

    _quotes_cache = []
    quotes_file = Path("quote_attribution_output/all_quotes.csv")

    if not quotes_file.exists():
        return _quotes_cache

    with open(quotes_file, 'r', encoding='utf-8', errors='replace') as f:
        reader = csv.DictReader(f)
        for row in reader:
            _quotes_cache.append(row)

    return _quotes_cache

def normalize_name(name):
    """Normalize name for matching."""
    return name.upper().strip()

class BrowserAPIHandler(http.server.SimpleHTTPRequestHandler):
    """HTTP handler with API endpoints for the person browser."""

    extensions_map = {
        '.js': 'application/javascript',
        '.css': 'text/css',
        '.csv': 'text/csv',
        '.json': 'application/json',
        '.html': 'text/html',
        '.txt': 'text/plain',
        '.pdf': 'application/pdf',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.gif': 'image/gif',
        '.svg': 'image/svg+xml',
        '': 'application/octet-stream',
    }

    def do_GET(self):
        parsed = urlparse(self.path)

        # API routing
        if parsed.path == '/api/quotes':
            self.handle_quotes_api(parse_qs(parsed.query))
        elif parsed.path == '/api/search':
            self.handle_search_api(parse_qs(parsed.query))
        else:
            # Fall back to static file serving
            super().do_GET()

    def send_json_response(self, data, status=200):
        """Send a JSON response."""
        response = json.dumps(data).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(response))
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(response)

    def handle_quotes_api(self, params):
        """Handle /api/quotes?person=NAME&offset=0&limit=50"""
        person = params.get('person', [''])[0]
        offset = int(params.get('offset', ['0'])[0])
        limit = int(params.get('limit', ['50'])[0])

        if not person:
            self.send_json_response({'error': 'person parameter required'}, 400)
            return

        # Load quotes
        all_quotes = load_quotes()

        # Load alias mapping for matching
        alias_to_canonical = {}
        alias_file = Path("alias_resolution_output/alias_mapping.csv")
        if alias_file.exists():
            with open(alias_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    alias_to_canonical[row['alias'].upper().strip()] = row['canonical'].upper().strip()

        # Normalize the search person name
        search_name = normalize_name(person)
        canonical_name = alias_to_canonical.get(search_name, search_name)

        # Find matching quotes
        matching_quotes = []
        for quote in all_quotes:
            speaker = normalize_name(quote.get('speaker', ''))
            # Check if speaker matches (directly or via alias)
            speaker_canonical = alias_to_canonical.get(speaker, speaker)
            if speaker_canonical == canonical_name or speaker == canonical_name:
                matching_quotes.append({
                    'type': quote.get('type', ''),
                    'question': quote.get('question', '')[:500] if quote.get('question') else '',
                    'answer': quote.get('answer', '')[:500] if quote.get('answer') else '',
                    'quote': quote.get('quote', '')[:500] if quote.get('quote') else '',
                    'file': quote.get('filename', ''),
                    'context': quote.get('context', '')[:200] if quote.get('context') else ''
                })

        # Paginate
        total = len(matching_quotes)
        paginated = matching_quotes[offset:offset + limit]

        self.send_json_response({
            'person': person,
            'total': total,
            'offset': offset,
            'limit': limit,
            'quotes': paginated
        })

    def handle_search_api(self, params):
        """Handle /api/search?q=query&limit=50"""
        query = params.get('q', [''])[0].upper().strip()
        limit = int(params.get('limit', ['50'])[0])

        if not query:
            self.send_json_response({'results': []})
            return

        # Load person index
        index_file = Path("data/person_index.json")
        if not index_file.exists():
            self.send_json_response({'error': 'Index not found. Run preprocess_browser_data.py first.'}, 500)
            return

        with open(index_file, 'r', encoding='utf-8') as f:
            index = json.load(f)

        # Search in persons
        results = []
        for person in index['persons']:
            name = person['name'].upper()
            aliases = [a.upper() for a in person.get('aliases', [])]

            if query in name or any(query in a for a in aliases):
                results.append({
                    'name': person['name'],
                    'mentions': person['mentions']
                })

            if len(results) >= limit:
                break

        self.send_json_response({'results': results})

    def log_message(self, format, *args):
        """Suppress logging for cleaner output, except errors."""
        if args[1] != '200':
            super().log_message(format, *args)


def main():
    # Check if data has been preprocessed
    if not Path("data/person_index.json").exists():
        print("Warning: Person browser data not found.")
        print("Run 'python preprocess_browser_data.py' to generate it.")
        print()

    Handler = BrowserAPIHandler

    print(f"Starting server at http://localhost:{PORT}")
    print()
    print("Available applications:")
    print(f"  - Person Browser:    http://localhost:{PORT}/person_browser.html")
    print(f"  - Network Viewer:    http://localhost:{PORT}/network_viewer.html")
    print(f"  - Timeline Explorer: http://localhost:{PORT}/timeline_explorer.html")
    print(f"  - Geographic Map:    http://localhost:{PORT}/geographic_map.html")
    print()
    print("Press Ctrl+C to stop")

    # Determine which app to open
    app = 'person_browser.html'
    if len(sys.argv) > 1:
        if sys.argv[1] == '--network':
            app = 'network_viewer.html'
        elif sys.argv[1] == '--timeline':
            app = 'timeline_explorer.html'
        elif sys.argv[1] == '--map':
            app = 'geographic_map.html'

    webbrowser.open(f'http://localhost:{PORT}/{app}')

    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print("\nServer stopped.")

if __name__ == "__main__":
    main()
