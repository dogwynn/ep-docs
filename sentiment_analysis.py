#!/usr/bin/env python3
"""
Sentiment Analysis Script for Text Files by Subfolder.

Analyzes all .txt files and reports sentiment scores per subfolder
using NLTK's VADER sentiment analyzer.
"""

from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import pandas as pd

# Download VADER lexicon if needed
try:
    nltk.data.find("sentiment/vader_lexicon.zip")
except LookupError:
    nltk.download("vader_lexicon", quiet=True)

# Configuration
BASE_DIR = Path("./epstein_pdfs")
OUTPUT_FILE = Path("./sentiment_results.csv")


def read_text_file(file_path):
    """Read and return text from a file."""
    try:
        return file_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def analyze_sentiment(text, analyzer, max_chunks=100, chunk_size=1000):
    """Calculate sentiment scores for text using VADER with chunking.

    Instead of analyzing the entire text at once (slow for large texts),
    we sample chunks and average the scores.
    """
    if not text or len(text) < 10:
        return {
            "compound": 0,
            "positive": 0,
            "negative": 0,
            "neutral": 0,
            "word_count": 0,
        }

    words = text.split()
    word_count = len(words)

    # For small texts, analyze directly
    if word_count <= chunk_size:
        scores = analyzer.polarity_scores(text)
        return {
            "compound": scores["compound"],
            "positive": scores["pos"],
            "negative": scores["neg"],
            "neutral": scores["neu"],
            "word_count": word_count,
        }

    # For large texts, sample chunks evenly distributed through the text
    num_chunks = min(max_chunks, word_count // chunk_size)
    step = word_count // num_chunks

    compound_scores = []
    pos_scores = []
    neg_scores = []
    neu_scores = []

    for i in range(num_chunks):
        start = i * step
        chunk_words = words[start : start + chunk_size]
        chunk_text = " ".join(chunk_words)
        scores = analyzer.polarity_scores(chunk_text)
        compound_scores.append(scores["compound"])
        pos_scores.append(scores["pos"])
        neg_scores.append(scores["neg"])
        neu_scores.append(scores["neu"])

    return {
        "compound": sum(compound_scores) / len(compound_scores),
        "positive": sum(pos_scores) / len(pos_scores),
        "negative": sum(neg_scores) / len(neg_scores),
        "neutral": sum(neu_scores) / len(neu_scores),
        "word_count": word_count,
    }


def process_folder(folder_and_files):
    """Process a single folder - designed for parallel execution."""
    folder, folder_files = folder_and_files

    # Each process needs its own analyzer instance
    analyzer = SentimentIntensityAnalyzer()

    # Combine all text in folder using list (O(n) vs O(n²) for += )
    text_parts = []
    file_count = 0

    for f in folder_files:
        text = read_text_file(f)
        if text:
            text_parts.append(text)
            file_count += 1

    all_text = " ".join(text_parts)

    # Analyze sentiment
    sentiment = analyze_sentiment(all_text, analyzer)

    return {
        "subfolder": str(folder),
        "folder_name": folder.name,
        "file_count": file_count,
        "word_count": sentiment["word_count"],
        "compound_score": round(sentiment["compound"], 4),
        "positive_ratio": round(sentiment["positive"], 4),
        "negative_ratio": round(sentiment["negative"], 4),
        "neutral_ratio": round(sentiment["neutral"], 4),
    }


def main():
    print("=== SENTIMENT ANALYSIS (VADER) ===\n")

    # Find all .txt files
    print("Scanning for .txt files...")
    txt_files = list(BASE_DIR.rglob("*.txt"))
    print(f"Found {len(txt_files)} .txt files\n")

    if not txt_files:
        print(f"No .txt files found in {BASE_DIR}")
        return

    # Group files by subfolder
    files_by_folder = defaultdict(list)
    for f in txt_files:
        files_by_folder[f.parent].append(f)

    subfolders = list(files_by_folder.keys())
    print(f"Processing {len(subfolders)} subfolders...\n")

    # Prepare work items for parallel processing
    work_items = [(folder, files_by_folder[folder]) for folder in subfolders]

    # Determine number of workers
    num_workers = min(multiprocessing.cpu_count(), len(subfolders))
    print(f"Using {num_workers} parallel workers\n")

    results = []
    completed = 0

    # Process folders in parallel
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(process_folder, item): item for item in work_items}

        for future in as_completed(futures):
            result = future.result()
            results.append(result)
            completed += 1

            if completed % 10 == 0 or completed == len(subfolders):
                print(f"Progress: {completed}/{len(subfolders)} subfolders ({100*completed/len(subfolders):.1f}%)")

    # Convert to DataFrame and sort
    df = pd.DataFrame(results)
    df = df.sort_values("compound_score", ascending=False)

    # Display summary
    print("\n=== SENTIMENT ANALYSIS RESULTS ===\n")
    print("Summary Statistics:")
    print(f"  Total subfolders analyzed: {len(df)}")
    print(f"  Total files processed: {df['file_count'].sum()}")
    print(f"  Total words analyzed: {df['word_count'].sum():,}")
    print(f"  Average compound score: {df['compound_score'].mean():.4f}")

    # Filter for folders with sufficient content
    df_filtered = df[df["word_count"] > 100]

    print("\n--- Top 10 Most Positive Subfolders ---")
    top_positive = df_filtered.nlargest(10, "compound_score")
    for i, (_, row) in enumerate(top_positive.iterrows(), 1):
        print(f"{i:2d}. {row['folder_name'][:60]}")
        print(f"    Compound: {row['compound_score']:+.4f} "
              f"(Pos: {row['positive_ratio']:.2f}, Neg: {row['negative_ratio']:.2f}, "
              f"Files: {row['file_count']})")

    print("\n--- Top 10 Most Negative Subfolders ---")
    top_negative = df_filtered.nsmallest(10, "compound_score")
    for i, (_, row) in enumerate(top_negative.iterrows(), 1):
        print(f"{i:2d}. {row['folder_name'][:60]}")
        print(f"    Compound: {row['compound_score']:+.4f} "
              f"(Pos: {row['positive_ratio']:.2f}, Neg: {row['negative_ratio']:.2f}, "
              f"Files: {row['file_count']})")

    # Save results
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"\nFull results saved to: {OUTPUT_FILE}")

    # Distribution summary
    print("\n--- Sentiment Distribution ---")
    positive_count = len(df[df["compound_score"] > 0.05])
    negative_count = len(df[df["compound_score"] < -0.05])
    neutral_count = len(df[(df["compound_score"] >= -0.05) & (df["compound_score"] <= 0.05)])

    print(f"  Positive sentiment (>0.05): {positive_count} subfolders")
    print(f"  Negative sentiment (<-0.05): {negative_count} subfolders")
    print(f"  Neutral sentiment: {neutral_count} subfolders")

    print("\nAnalysis complete!")


if __name__ == "__main__":
    main()
