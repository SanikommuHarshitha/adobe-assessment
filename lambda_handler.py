"""
AWS Lambda Handler for Search Keyword Performance Processor.
Triggered by S3 file upload. Reads input file, processes it, and writes output back to S3.
"""

import boto3
import json
import logging
import os

from processor import SearchKeywordProcessor

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

OUTPUT_BUCKET = os.environ.get("OUTPUT_BUCKET", "search-keyword-performance-output")


def lambda_handler(event, context):
    """
    Lambda entry point.
    Expects an S3 trigger event with the input file location.
    """
    logger.info(f"Received event: {json.dumps(event)}")

    try:
        # Extract S3 bucket and key from the trigger event
        record = event["Records"][0]["s3"]
        input_bucket = record["bucket"]["name"]
        input_key = record["object"]["key"]

        logger.info(f"Processing file: s3://{input_bucket}/{input_key}")

        # Download the file from S3
        response = s3_client.get_object(Bucket=input_bucket, Key=input_key)
        file_content = response["Body"].read().decode("utf-8")

        # Process the file
        processor = SearchKeywordProcessor()
        revenue_data = processor.process_file(file_content)
        date_str = processor.extract_date_from_content(file_content)
        filename, output_content = processor.generate_output(revenue_data, date_str)

        # Upload output to S3
        output_key = f"output/{filename}"
        s3_client.put_object(
            Bucket=OUTPUT_BUCKET,
            Key=output_key,
            Body=output_content.encode("utf-8"),
            ContentType="text/tab-separated-values",
        )

        logger.info(f"Output written to s3://{OUTPUT_BUCKET}/{output_key}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing complete",
                "output_file": f"s3://{OUTPUT_BUCKET}/{output_key}",
                "records_processed": len(revenue_data),
            }),
        }

    except KeyError as e:
        logger.error(f"Missing expected field in event: {e}")
        return {"statusCode": 400, "body": json.dumps({"error": str(e)})}
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
