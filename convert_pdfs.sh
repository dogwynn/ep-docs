#!/bin/bash

# Convert all PDF files to text using pdftotext
# Recursively searches all subdirectories
# Original PDF files are preserved
# Text files are created alongside the original PDFs

find . -type f -iname "*.pdf" -print0 | while IFS= read -r -d '' pdf_file; do
    txt_file="${pdf_file%.pdf}.txt"
    echo "Converting: $pdf_file -> $txt_file"
    pdftotext "$pdf_file" "$txt_file"
done

echo "Done!"
