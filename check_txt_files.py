#!/usr/bin/env python3
"""
Analyze .txt files in subfolders and report quality percentages.

Classifies files as:
- real_text: Contains readable text content
- empty: Empty or whitespace-only files
- garbage: Contains non-printable characters or repetitive patterns
"""

import sys
import csv
import re
from pathlib import Path
from collections import defaultdict


def is_empty(filepath):
    """Check if a file is empty or whitespace-only."""
    try:
        if filepath.stat().st_size == 0:
            return True
        content = filepath.read_text(encoding="utf-8", errors="ignore")
        return content.strip() == ""
    except Exception:
        return True


def is_garbage(filepath):
    """Check if content appears to be garbage/unreadable."""
    try:
        content = filepath.read_bytes()
    except Exception:
        return False

    if len(content) == 0:
        return False

    # Count printable ASCII characters
    printable_count = sum(
        1 for b in content if (0x20 <= b <= 0x7E) or b in (0x09, 0x0A, 0x0D)
    )
    total_chars = len(content)

    # If more than 30% non-printable characters, likely garbage
    non_printable_ratio = (total_chars - printable_count) / total_chars
    if non_printable_ratio > 0.30:
        return True

    # Convert to string for pattern checks
    try:
        text = content.decode("utf-8", errors="ignore")
    except Exception:
        return True

    # Check for repetitive patterns (same character repeated 50+ times)
    if re.search(r"(.)\1{50,}", text):
        return True

    # Check for very low unique character diversity in longer files
    if len(text) > 100:
        unique_chars = len(set(text))
        if unique_chars < 5:
            return True

    # Check if file has reasonable word-like patterns
    words = text.split()
    if len(words) > 10:
        avg_word_len = sum(len(w) for w in words) / len(words)
        if avg_word_len > 50:
            return True

    return False


def classify_file(filepath):
    """Classify a single file."""
    if is_empty(filepath):
        return "empty"
    elif is_garbage(filepath):
        return "garbage"
    else:
        return "real_text"


def analyze_subfolders(base_path):
    """Analyze all subfolders containing .txt files."""
    base_path = Path(base_path)
    results = []

    # Get all directories
    dirs = [base_path] + [d for d in base_path.rglob("*") if d.is_dir()]

    for dir_path in dirs:
        # Find .txt files in this directory (not recursive)
        txt_files = list(dir_path.glob("*.txt"))
        txt_files += list(dir_path.glob("*.TXT"))

        if not txt_files:
            continue

        # Classify each file
        classifications = [classify_file(f) for f in txt_files]

        total = len(classifications)
        real_text = classifications.count("real_text")
        empty = classifications.count("empty")
        garbage = classifications.count("garbage")

        # Calculate relative path
        try:
            rel_path = dir_path.relative_to(base_path)
            rel_path = "." if str(rel_path) == "." else str(rel_path)
        except ValueError:
            rel_path = str(dir_path)

        results.append({
            "folder": rel_path,
            "total_files": total,
            "real_text_count": real_text,
            "empty_count": empty,
            "garbage_count": garbage,
            "real_text_pct": round(100 * real_text / total, 1),
            "empty_pct": round(100 * empty / total, 1),
            "garbage_pct": round(100 * garbage / total, 1),
        })

    return results


def print_report(results):
    """Print formatted report."""
    if not results:
        print("No .txt files found in any subfolder.")
        return

    print("\n")
    print("=================================================================")
    print("              TXT FILE QUALITY REPORT BY SUBFOLDER               ")
    print("=================================================================\n")

    for row in results:
        print(f"Folder: {row['folder']}")
        print(f"  Total .txt files: {row['total_files']}")
        print(f"  Real text documents: {row['real_text_count']} ({row['real_text_pct']}%)")
        print(f"  Empty files:        {row['empty_count']} ({row['empty_pct']}%)")
        print(f"  Garbage files:      {row['garbage_count']} ({row['garbage_pct']}%)")
        print("-----------------------------------------------------------------")

    # Summary totals
    print("\nOVERALL SUMMARY:")
    total_all = sum(r["total_files"] for r in results)
    real_all = sum(r["real_text_count"] for r in results)
    empty_all = sum(r["empty_count"] for r in results)
    garbage_all = sum(r["garbage_count"] for r in results)

    print(f"  Total .txt files analyzed: {total_all}")
    print(f"  Real text documents: {real_all} ({100 * real_all / total_all:.1f}%)")
    print(f"  Empty files:         {empty_all} ({100 * empty_all / total_all:.1f}%)")
    print(f"  Garbage files:       {garbage_all} ({100 * garbage_all / total_all:.1f}%)")
    print()


def main():
    # Use command line argument if provided, otherwise current directory
    base_path = sys.argv[1] if len(sys.argv) > 1 else "."

    print(f"Analyzing .txt files in: {Path(base_path).resolve()}")

    results = analyze_subfolders(base_path)
    print_report(results)

    # Save results to CSV
    if results:
        output_file = "txt_analysis_results.csv"
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=results[0].keys())
            writer.writeheader()
            writer.writerows(results)
        print(f"Results saved to: {output_file}")


if __name__ == "__main__":
    main()
