# Wikipedia Graph Analysis Pipeline
**Apache Spark · Python · AWS S3 · EMR · Distributed Computing**

A two-part distributed data pipeline built on Apache Spark and AWS EMR to analyze the structural properties of the Wikipedia link graph at scale, processing millions of pages across four interconnected datasets.

## Pipeline Overview

| Part | Task | Key Output |
|------|------|------------|
| Part 1 — Mutual Links | Extract bidirectional page links | Parquet file of mutual link pairs |
| Part 2 — Connected Components | Identify clusters of interconnected pages | Parquet file of component assignments |

## Part 1: Mutual Link Extraction (`part1-mutual-links/`)

Identifies pairs of Wikipedia pages that link to each other bidirectionally, accounting for redirects.

**Approach:**
- Filtered `page_df` to main namespace only
- Resolved redirect chains in `redirect_df` to normalize target pages
- Joined `pagelinks_df` with `linktarget_df` to map source → target
- Applied self-join to detect bidirectional pairs, deduplicated with `distinct()`

**Output:** `(page_a, page_b)` DataFrame stored in Parquet format on S3

## Part 2: Connected Components (`part2-connected-components/`)

Computes connected components of the mutual link graph — identifying clusters of pages reachable from each other through chains of mutual links.

**Approach:**
- Initialized each vertex with its own ID as component label
- Iteratively propagated minimum component IDs across edges until convergence
- Checkpointed intermediate state to S3 every N iterations for fault tolerance
- Validated correctness against synthetic test datasets before running at scale

**Output:** `(vertex, component)` DataFrame stored in Parquet format on S3

## Infrastructure

- **Compute:** AWS EMR cluster launched programmatically via `Final_start.py`
- **Storage:** AWS S3 for input datasets, intermediate checkpoints, and final outputs
- **Framework:** PySpark with optimized transformations for large-scale graph data
- **Testing:** Automated test suite (`Final_test.py`) using synthetic datasets to validate mutual link extraction and component convergence logic

## Running the Pipeline

### Prerequisites
- Python 3.x
- AWS credentials configured
- S3 bucket with Wikipedia datasets (`page`, `pagelinks`, `linktarget`, `redirect`)

### Setup
```bash
# Upload pipeline files to your S3 bucket, then update S3 paths in:
# part2-connected-components/Final_start.py

python part2-connected-components/Final_start.py
```

This launches an EMR cluster, runs the Spark job, and writes outputs back to S3.

### Test Suite
```bash
python part2-connected-components/Final_test.py
```
Validates mutual link extraction and connected component logic against known synthetic inputs.

## Dataset
Wikipedia dump datasets: `page`, `pagelinks`, `linktarget`, `redirect` — processed from S3.
