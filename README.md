# Search Keyword Performance Processor

A Python application that analyzes Adobe Analytics hit-level data and attributes revenue to external search engine keywords.

The pipeline processes hit-level data, attributes purchases back to the originating search keyword, and generates a revenue report for marketing analysis.

## The Business Question

How much revenue is the client getting from external search engines, and which keywords are performing the best based on revenue?

## Output

Produces a tab-delimited file named `YYYY-MM-DD_SearchKeywordPerformance.tab`:

```
Search Engine Domain    Search Keyword    Revenue
google.com              ipod              480.00
bing.com                zune              250.00
```

Sorted by revenue descending. The date in the filename reflects the latest date found in the data.

---

## Project Structure

```
adobe-assessment/
│
├── processor.py          # Core processing logic
├── lambda_handler.py     # AWS Lambda entry point
├── glue_job.py           # AWS Glue job for large file processing
├── main.py               # Local CLI runner
│
├── template.yaml         # AWS SAM infrastructure template
├── samconfig.toml        # Local deployment config (gitignored)
│
├── requirements.txt
├── .gitignore
├── README.md
│
└── tests/
    └── test_processor.py # Unit tests for core processing logic
```

---

## Architecture Overview

```
S3 Input File
      │
      ▼
AWS Lambda
(SearchKeywordProcessor class)
      │
      ▼
Revenue Aggregation
      │
      ▼
S3 Output File
```

---

## Documentation

Detailed design decisions and implementation notes are documented in `documentation.pdf`.

---

## Deploy to AWS

This project is deployed using AWS SAM CLI via **AWS CloudShell** — no local setup or AWS CLI installation required. CloudShell runs directly in the AWS Console and is already authenticated.

### Step 1 — Open CloudShell

In the AWS Console, click the **CloudShell icon** (`>_`) in the top navigation bar.

### Step 2 — Clone the Repository

```bash
git clone https://github.com/SanikommuHarshitha/adobe-assessment.git
cd adobe-assessment
```

### Step 3 — Create samconfig.toml

`samconfig.toml` is gitignored and must be created manually. Create the file and paste the following:

```toml
version = 0.1

[default.deploy.parameters]
stack_name = "search-keyword-performance"
region = "us-west-1"
confirm_changeset = true
capabilities = "CAPABILITY_IAM"
parameter_overrides = "BucketName=YOUR_BUCKET_NAME InputKey=search_keyword_performance_raw/YOUR_INPUT_FILE.sql OutputPrefix=search_keyword_performance_processed"
```

### Step 4 — Build and Deploy

```bash
sam build
sam deploy --resolve-s3
```

Type `y` when prompted to confirm the deployment.

### Step 5 — Run

Go to **AWS Console → Lambda → SearchKeywordProcessor → Test**.

Pass the input file in the test event payload:

```json
{
    "input_key": "search_keyword_performance_raw/data[36][51].sql"
}
```

Click **Test**. The output file will be written to:

```
s3://YOUR_BUCKET_NAME/search_keyword_performance_processed/YYYY-MM-DD_SearchKeywordPerformance.tab
```

To view the output from CloudShell:

```bash
aws s3 cp s3://YOUR_BUCKET_NAME/search_keyword_performance_processed/2009-09-27_SearchKeywordPerformance.tab -
```

---

## Unit Tests

Unit tests validate the core processing logic implemented in `SearchKeywordProcessor`.

Tests can be executed directly in AWS CloudShell:

```bash
pip install pytest
pytest tests/
```

---

## Scalability (Files > 10 GB)

The current Lambda implementation loads the full file into memory which will not scale beyond Lambda's limits. There are three different approaches to address this:

- **Approach 1 — Stream line-by-line**: A quick fix to the current Lambda solution. Read and process one line at a time so memory usage stays constant regardless of file size. Best for files up to a few GB.
- **Approach 2 — AWS Glue**: Migrate to AWS Glue (PySpark), a distributed processing engine built for large-scale data. `glue_job.py` contains a ready-to-use Glue implementation of the same logic. Recommended for files consistently exceeding 10 GB.
- **Approach 3 — SQS-based chunking**: Split the file into chunks, queue via SQS, and process in parallel across multiple workers before aggregating results. An alternative to Glue but significantly more engineering effort.
