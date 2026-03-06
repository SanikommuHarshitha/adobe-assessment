"""
AWS Lambda Handler for Search Keyword Performance Processor.
Reads hit-level data from S3, processes it, and writes output back to S3.

Bucket  : adobe-revenue-assessment
Input   : s3://adobe-revenue-assessment/search_keyword_performance_raw/
Output  : s3://adobe-revenue-assessment/search_keyword_performance_processed/
"""

import boto3
import json
import logging
import os

from processor import SearchKeywordProcessor

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

BUCKET_NAME   = os.environ.get("BUCKET_NAME")
INPUT_KEY     = os.environ.get("INPUT_KEY")
OUTPUT_PREFIX = os.environ.get("OUTPUT_PREFIX")


def lambda_handler(event, context):
    """
    Lambda entry point.
    Accepts an optional input_key in the event payload to process any file.
    Falls back to the INPUT_KEY environment variable if not provided in the event.

    Example event payload:
    {
        "input_key": "search_keyword_performance_raw/data[36][51].sql"
    }
    """
    input_key = event.get("input_key") or INPUT_KEY

    if not input_key:
        return {"statusCode": 400, "body": json.dumps({"error": "No input_key provided in event or environment variables"})}

    logger.info(f"Reading input from s3://{BUCKET_NAME}/{input_key}")

    try:
        # Read input file from S3
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=input_key)
        file_content = response["Body"].read().decode("utf-8")

        # Process the file
        processor = SearchKeywordProcessor()
        revenue_data = processor.process_file(file_content)
        date_str = processor.extract_date_from_content(file_content)
        filename, output_content = processor.generate_output(revenue_data, date_str)

        # Write output to S3
        output_key = f"{OUTPUT_PREFIX}/{filename}"
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=output_key,
            Body=output_content.encode("utf-8"),
            ContentType="text/tab-separated-values",
        )

        logger.info(f"Output written to s3://{BUCKET_NAME}/{output_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing complete",
                "input_file": f"s3://{BUCKET_NAME}/{input_key}",
                "output_file": f"s3://{BUCKET_NAME}/{output_key}",
                "keywords_found": len(revenue_data),
            }),
        }

    except Exception as e:
        logger.error(f"Error processing file: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
