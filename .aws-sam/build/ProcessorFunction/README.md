# Search Keyword Performance Processor

A Python application that analyzes hit-level Adobe Analytics data to compute revenue attributed to external search engine keywords.

## The Business Question

How much revenue is the client getting from external search engines, and which keywords are performing the best based on revenue?

## Output

Produces a tab-delimited file named `YYYY-MM-DD_SearchKeywordPerformance.tab`:

```
Search Engine Domain    Search Keyword    Revenue
google.com              ipod              480.00
bing.com                zune              250.00
```

Sorted by revenue descending.

---

## Project Structure

```
adobe-assessment/
├── src/
│   ├── processor.py        # Core logic (SearchKeywordProcessor class)
│   └── lambda_handler.py   # AWS Lambda entry point
├── main.py                 # CLI runner for local use
├── template.yaml           # AWS SAM deployment template
├── samconfig.toml          # Local deployment config (gitignored - see setup below)
├── .gitignore
└── README.md
```

---

## Deploy to AWS with SAM CLI

### Prerequisites
- [AWS CLI](https://aws.amazon.com/cli/) installed and configured
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) installed
- An existing S3 bucket with the input file already uploaded

### Step 1 — Configure AWS CLI

```bash
aws configure
```

Enter your AWS Access Key ID, Secret Access Key, and region when prompted.

### Step 2 — Create samconfig.toml

`samconfig.toml` is gitignored and must be created manually in the project root. Create the file and paste the following, replacing values as needed:

```toml
version = 0.1

[default.deploy.parameters]
stack_name = "search-keyword-performance"
region = "us-east-1"
confirm_changeset = true
capabilities = "CAPABILITY_IAM"
parameter_overrides = "BucketName=YOUR_BUCKET_NAME InputKey=source_revenue/YOUR_INPUT_FILE.sql OutputPrefix=output_revenue"
```

### Step 3 — Build and Deploy

```bash
sam build
sam deploy
```

### Step 4 — Run

Invoke the Lambda function from the AWS console or CLI. The function will read the input file from S3, process it, and write the output to:

```
s3://YOUR_BUCKET_NAME/output_revenue/YYYY-MM-DD_SearchKeywordPerformance.tab
```

---

## Scalability (Files > 10 GB)

The current implementation loads the full file into memory which will not scale beyond Lambda's limits. For files of this size:

- **Stream line-by-line** from S3 to keep memory usage constant
- **AWS Glue (PySpark)** for distributed processing across multiple nodes without server management
- **SQS-based chunking** — split the file into chunks, process in parallel across multiple Glue jobs, then aggregate results
