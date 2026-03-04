# Search Keyword Performance Processor

Analyzes hit-level web analytics data to compute revenue attributed to external search engine keywords.

## Output

Produces a tab-delimited `.tab` file named `YYYY-MM-DD_SearchKeywordPerformance.tab` with columns:

| Search Engine Domain | Search Keyword | Revenue |
|----------------------|----------------|---------|
| google.com           | Ipod           | 190.00  |
| bing.com             | Zune           | 250.00  |

Sorted by Revenue descending.

---

## Project Structure

```
search-keyword-performance/
├── src/
│   ├── processor.py        # Core logic (SearchKeywordProcessor class)
│   └── lambda_handler.py   # AWS Lambda entry point
├── tests/
│   └── test_processor.py   # Unit tests
├── main.py                 # CLI runner for local use
├── template.yaml           # AWS SAM deployment template
├── requirements.txt
└── README.md
```

---

## Run Locally

```bash
# Install dependencies
pip install -r requirements.txt

# Run against your data file
python main.py path/to/your/data_file.sql
```

---

## Run Tests

```bash
pytest tests/ -v
```

---

## Deploy to AWS with SAM CLI

### Prerequisites
- [AWS CLI](https://aws.amazon.com/cli/) configured with your credentials
- [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) installed

### Steps

```bash
# 1. Build the SAM application
sam build

# 2. Deploy (guided first time)
sam deploy --guided
```

Follow the prompts. SAM will:
- Package and upload your Lambda code to S3
- Create the input/output S3 buckets
- Deploy the Lambda function with the S3 trigger

### Trigger a Processing Run

Upload your data file to the input S3 bucket:

```bash
aws s3 cp your_data_file.sql s3://search-keyword-performance-input/
```

Lambda will automatically trigger and write output to:

```
s3://search-keyword-performance-output/output/YYYY-MM-DD_SearchKeywordPerformance.tab
```

Download the result:

```bash
aws s3 cp s3://search-keyword-performance-output/output/2009-09-27_SearchKeywordPerformance.tab .
```

---

## Scalability (Files > 10 GB)

For files exceeding 10 GB, the current architecture would need adjustments:

1. **Streaming S3 reads** — Instead of loading the entire file into memory, use `boto3`'s streaming body and process line-by-line to keep memory usage constant regardless of file size.

2. **AWS Glue or EMR** — For very large files (10 GB+), a distributed processing framework like AWS Glue (PySpark) or EMR would parallelize processing across multiple nodes.

3. **S3 chunked processing with SQS** — Split large files into chunks using S3 multipart, queue chunks via SQS, and have multiple Lambda functions process chunks in parallel, then aggregate results.

4. **Lambda memory/timeout limits** — Lambda caps at 10 GB RAM and 15 min timeout. Files approaching this size should move to a container-based solution (AWS Fargate or ECS) with no such limits.
