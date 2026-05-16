# Databricks notebook source
# MAGIC %md
# MAGIC # Part 1: Bronze Layer - TVMaze API Ingestion
# MAGIC Samed Budak | May 2026

# COMMAND ----------

import requests
import json
import time
from datetime import datetime
from pyspark.sql.functions import current_timestamp, lit, input_file_name

# COMMAND ----------

# Config
CATALOG = "tvmaze_catalog"
SCHEMA = "bronze"
ADLS_BASE_PATH = "abfss://tvmaze@<storage_account>.dfs.core.windows.net"
RAW_PATH = f"{ADLS_BASE_PATH}/raw"
BRONZE_PATH = f"{ADLS_BASE_PATH}/bronze"

TVMAZE_BASE_URL = "https://api.tvmaze.com"
MAX_SHOWS = 250
API_RATE_LIMIT_DELAY = 0.3  # ~20 req per 10 sec

# In prod, MAX_SHOWS would come from a widget or job parameter
# TODO: switch to Autoloader (cloudFiles) for streaming ingestion once in prod

# COMMAND ----------

# MAGIC %md
# MAGIC ## Unity Catalog setup

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"Catalog '{CATALOG}', schema '{SCHEMA}' ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## API ingestion with retry

# COMMAND ----------

def fetch_with_retry(url: str, max_retries: int = 3, backoff_factor: float = 1.0) -> dict:
    """GET with exponential backoff. Returns parsed JSON or None on failure."""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", backoff_factor * (2 ** attempt)))
                print(f"Rate limited. Waiting {retry_after}s before retry...")
                time.sleep(retry_after)
            elif response.status_code == 404:
                print(f"Not found: {url}")
                return None
            else:
                print(f"HTTP {response.status_code} for {url}")
                time.sleep(backoff_factor * (2 ** attempt))
        except requests.exceptions.RequestException as e:
            print(f"Request failed (attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(backoff_factor * (2 ** attempt))
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch shows

# COMMAND ----------

print("Fetching shows...")
shows_data = fetch_with_retry(f"{TVMAZE_BASE_URL}/shows?page=0")

if shows_data:
    print(f"Got {len(shows_data)} shows.")
else:
    raise Exception("Failed to fetch shows data from API.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch episodes

# COMMAND ----------

all_episodes = []
show_ids = [show["id"] for show in shows_data[:50]]  # first 50 for demo

print(f"Fetching episodes for {len(show_ids)} shows...")
for i, show_id in enumerate(show_ids):
    episodes = fetch_with_retry(f"{TVMAZE_BASE_URL}/shows/{show_id}/episodes")
    if episodes:
        for ep in episodes:
            ep["show_id"] = show_id
        all_episodes.extend(episodes)
    time.sleep(API_RATE_LIMIT_DELAY)
    if (i + 1) % 10 == 0:
        print(f"  {i + 1}/{len(show_ids)} done...")

print(f"Got {len(all_episodes)} episodes total.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fetch cast

# COMMAND ----------

all_cast = []

print(f"Fetching cast for {len(show_ids)} shows...")
for i, show_id in enumerate(show_ids):
    cast = fetch_with_retry(f"{TVMAZE_BASE_URL}/shows/{show_id}/cast")
    if cast:
        for member in cast:
            member["show_id"] = show_id
        all_cast.extend(cast)
    time.sleep(API_RATE_LIMIT_DELAY)
    if (i + 1) % 10 == 0:
        print(f"  {i + 1}/{len(show_ids)} done...")

print(f"Got {len(all_cast)} cast members total.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write raw JSON to ADLS Gen2

# COMMAND ----------

def write_raw_json(data: list, path: str, entity_name: str):
    # dump to ADLS so we always have an immutable copy to replay from
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"{path}/{entity_name}/{timestamp}"

    json_rdd = spark.sparkContext.parallelize(
        [json.dumps(record) for record in data]
    )
    df = spark.read.json(json_rdd)
    df.write.mode("overwrite").json(output_path)

    print(f"Raw {entity_name} -> {output_path}")
    return output_path

# COMMAND ----------

shows_raw_path = write_raw_json(shows_data, RAW_PATH, "shows")
episodes_raw_path = write_raw_json(all_episodes, RAW_PATH, "episodes")
cast_raw_path = write_raw_json(all_cast, RAW_PATH, "cast")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze Delta tables
# MAGIC We turn on mergeSchema so the API can add fields without breaking the pipeline.
# MAGIC PERMISSIVE mode captures malformed records in _corrupt_record instead of silently dropping them.

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_shows

# COMMAND ----------

df_shows_raw = (
    spark.read
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("multiLine", "false")
    .option("recursiveFileLookup", "true")
    .json(f"{RAW_PATH}/shows/")
)

# ingestion metadata
df_shows_bronze = (
    df_shows_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("tvmaze_api"))
    .withColumn("_source_file", input_file_name())
)

(
    df_shows_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_shows")
)

print(f"bronze_shows: {df_shows_bronze.count()} records")
df_shows_bronze.printSchema()
# uncomment to check for malformed records:
# df_shows_bronze.filter("_corrupt_record IS NOT NULL").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_episodes

# COMMAND ----------

df_episodes_raw = (
    spark.read
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("recursiveFileLookup", "true")
    .json(f"{RAW_PATH}/episodes/")
)

df_episodes_bronze = (
    df_episodes_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("tvmaze_api"))
    .withColumn("_source_file", input_file_name())
)

(
    df_episodes_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_episodes")
)

print(f"bronze_episodes: {df_episodes_bronze.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### bronze_cast

# COMMAND ----------

df_cast_raw = (
    spark.read
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .option("recursiveFileLookup", "true")
    .json(f"{RAW_PATH}/cast/")
)

df_cast_bronze = (
    df_cast_raw
    .withColumn("_ingestion_timestamp", current_timestamp())
    .withColumn("_source", lit("tvmaze_api"))
    .withColumn("_source_file", input_file_name())
)

(
    df_cast_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema", "true")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_cast")
)

print(f"bronze_cast: {df_cast_bronze.count()} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register in Unity Catalog and verify

# COMMAND ----------

tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
print("Tables in Unity Catalog:")
for t in tables:
    print(f"  - {t.tableName}")

# COMMAND ----------

# Cleaner view aliases for downstream consumers
spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.tv_shows AS
    SELECT * FROM {CATALOG}.{SCHEMA}.bronze_shows
""")

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.episodes AS
    SELECT * FROM {CATALOG}.{SCHEMA}.bronze_episodes
""")

spark.sql(f"""
    CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.cast AS
    SELECT * FROM {CATALOG}.{SCHEMA}.bronze_cast
""")

print("Views (tv_shows, episodes, cast) created.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Access control

# COMMAND ----------

# Grant read access to analysts role
spark.sql(f"GRANT USAGE ON CATALOG {CATALOG} TO `data_analysts`")
spark.sql(f"GRANT USAGE ON SCHEMA {CATALOG}.{SCHEMA} TO `data_analysts`")
spark.sql(f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.bronze_shows TO `data_analysts`")
spark.sql(f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.bronze_episodes TO `data_analysts`")
spark.sql(f"GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.bronze_cast TO `data_analysts`")

print("data_analysts granted SELECT on Bronze tables.")

# COMMAND ----------

spark.sql(f"SHOW GRANTS ON TABLE {CATALOG}.{SCHEMA}.bronze_shows").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick validation

# COMMAND ----------

for table_name in ["bronze_shows", "bronze_episodes", "bronze_cast"]:
    count = spark.table(f"{CATALOG}.{SCHEMA}.{table_name}").count()
    print(f"  {table_name}: {count:,} records")

print("\n--- Sample Shows ---")
spark.table(f"{CATALOG}.{SCHEMA}.bronze_shows").select("id", "name", "language", "genres", "premiered").show(5, truncate=False)

print("\n--- Sample Episodes ---")
spark.table(f"{CATALOG}.{SCHEMA}.bronze_episodes").select("id", "name", "season", "number", "airdate", "runtime").show(5, truncate=False)

print("\n--- Sample Cast ---")
spark.table(f"{CATALOG}.{SCHEMA}.bronze_cast").select("show_id", "person.name", "character.name").show(5, truncate=False)
