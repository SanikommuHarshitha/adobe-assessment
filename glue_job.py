"""
AWS Glue Job - Search Keyword Performance Processor
Scalable alternative to the Lambda function for processing large files (10 GB+).

Glue Job Parameters (set in AWS Glue Console):
  --BUCKET_NAME   : S3 bucket name
  --INPUT_KEY     : S3 key of the input file
  --OUTPUT_PREFIX : S3 prefix for output files
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from urllib.parse import urlparse, parse_qs


# ── Glue job setup ────────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, ["JOB_NAME", "BUCKET_NAME", "INPUT_KEY", "OUTPUT_PREFIX"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET_NAME   = args["BUCKET_NAME"]
INPUT_KEY     = args["INPUT_KEY"]
OUTPUT_PREFIX = args["OUTPUT_PREFIX"]


# ── Constants ─────────────────────────────────────────────────────────────────

# The site being analyzed - referrers from this domain are internal (not search engines)
OWNED_DOMAIN        = "esshopzilla.com"

# Common search query parameter names used across search engines
SEARCH_QUERY_PARAMS = ["q", "p", "query", "search", "qs", "text", "keyword"]

# Per Appendix A: event 1 = Purchase. Revenue is only actualized when this event is present.
PURCHASE_EVENT      = "1"


# ── UDFs ──────────────────────────────────────────────────────────────────────

def extract_search_domain(referrer):
    """
    Extract the search engine domain from a referrer URL.
    Returns None if the referrer is internal or not a search engine.
    """
    if not referrer:
        return None
    try:
        parsed = urlparse(referrer)
        netloc = parsed.netloc.lower()
        if OWNED_DOMAIN in netloc:
            return None
        query_params = parse_qs(parsed.query)
        for param in SEARCH_QUERY_PARAMS:
            if param in query_params:
                return netloc.replace("www.", "", 1)
    except Exception:
        pass
    return None


def extract_search_keyword(referrer):
    """
    Extract the search keyword from a referrer URL.
    Keyword is normalized to lowercase.
    Returns None if no search keyword is found.
    """
    if not referrer:
        return None
    try:
        parsed = urlparse(referrer)
        netloc = parsed.netloc.lower()
        if OWNED_DOMAIN in netloc:
            return None
        query_params = parse_qs(parsed.query)
        for param in SEARCH_QUERY_PARAMS:
            if param in query_params:
                keyword = query_params[param][0].strip().lower()
                if keyword:
                    return keyword
    except Exception:
        pass
    return None


def extract_revenue(product_list):
    """
    Extract total revenue from the product_list field.
    Revenue is the 4th semicolon-delimited field per product.
    Multiple products are comma-separated and their revenues are summed.
    """
    if not product_list:
        return 0.0
    total = 0.0
    for product in product_list.split(","):
        fields = product.split(";")
        if len(fields) >= 4 and fields[3].strip():
            try:
                total += float(fields[3].strip())
            except ValueError:
                pass
    return total


def is_purchase(event_list):
    """
    Check if the hit contains a purchase event.
    Per Appendix A: event 1 = Purchase.
    """
    if not event_list:
        return False
    return PURCHASE_EVENT in [e.strip() for e in event_list.split(",")]


# Register UDFs with Spark
extract_domain_udf  = F.udf(extract_search_domain, StringType())
extract_keyword_udf = F.udf(extract_search_keyword, StringType())
extract_revenue_udf = F.udf(extract_revenue, "double")
is_purchase_udf     = F.udf(is_purchase, "boolean")


# ── Read input file from S3 ───────────────────────────────────────────────────

df = spark.read \
    .option("delimiter", "\t") \
    .option("header", "true") \
    .csv(f"s3://{BUCKET_NAME}/{INPUT_KEY}")


# ── Enrich each row with derived columns ─────────────────────────────────────

df = df.withColumn("search_domain",  extract_domain_udf(F.col("referrer"))) \
       .withColumn("search_keyword", extract_keyword_udf(F.col("referrer"))) \
       .withColumn("revenue",        extract_revenue_udf(F.col("product_list"))) \
       .withColumn("is_purchase",    is_purchase_udf(F.col("event_list")))


# ── Session-based attribution ─────────────────────────────────────────────────

# Extract rows where the visitor arrived from a search engine
search_hits = df.filter(
    F.col("search_domain").isNotNull() & F.col("search_keyword").isNotNull()
).select(
    "ip", "user_agent", "search_domain", "search_keyword",
    F.col("hit_time_gmt").cast("long").alias("hit_time")
)

# Extract rows where a purchase with revenue occurred
purchase_hits = df.filter(
    F.col("is_purchase") & (F.col("revenue") > 0)
).select(
    "ip", "user_agent", "revenue",
    F.col("hit_time_gmt").cast("long").alias("purchase_time")
)

# Join on session key — keep only search hits that occurred before the purchase
joined = purchase_hits.join(
    search_hits,
    on=["ip", "user_agent"],
    how="inner"
).filter(F.col("hit_time") < F.col("purchase_time"))

# For each purchase, take the most recent search hit before it
window = Window.partitionBy("ip", "user_agent", "purchase_time") \
               .orderBy(F.col("hit_time").desc())

attributed = joined.withColumn("rank", F.row_number().over(window)) \
                   .filter(F.col("rank") == 1) \
                   .drop("rank", "hit_time", "purchase_time")


# ── Aggregate total revenue by (domain, keyword) across all dates ─────────────

result = attributed.groupBy("search_domain", "search_keyword") \
                   .agg(F.round(F.sum("revenue"), 2).alias("Revenue"))


# ── Derive output filename date from latest date in the data ──────────────────

latest_date = df.select(
    F.max(F.to_date(F.col("date_time"), "yyyy-MM-dd HH:mm:ss")).alias("latest_date")
).collect()[0]["latest_date"]

output_filename = f"{latest_date}_SearchKeywordPerformance.tab"


# ── Write single output file to S3 ───────────────────────────────────────────

output = result.select(
    F.col("search_domain").alias("Search Engine Domain"),
    F.col("search_keyword").alias("Search Keyword"),
    F.col("Revenue")
).orderBy(F.col("Revenue").desc())

output_path = f"s3://{BUCKET_NAME}/{OUTPUT_PREFIX}/{output_filename}"

# coalesce(1) ensures a single output file instead of multiple Spark partitions
output.coalesce(1).write \
    .mode("overwrite") \
    .option("delimiter", "\t") \
    .option("header", "true") \
    .csv(output_path)

print(f"Output written to {output_path}")

job.commit()
