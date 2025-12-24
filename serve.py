#!/usr/bin/env python3
"""Simple HTTP server to serve the network viewer."""
import http.server
import socketserver
import webbrowser
import os

PORT = 8080

os.chdir(os.path.dirname(os.path.abspath(__file__)))

Handler = http.server.SimpleHTTPRequestHandler
Handler.extensions_map.update({
    '.js': 'application/javascript',
    '.css': 'text/css',
    '.csv': 'text/csv',
})

print(f"Starting server at http://localhost:{PORT}")
print("Opening browser...")
print("Press Ctrl+C to stop")

webbrowser.open(f'http://localhost:{PORT}/network_viewer.html')

with socketserver.TCPServer(("", PORT), Handler) as httpd:
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
