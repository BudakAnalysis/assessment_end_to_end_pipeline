# Databricks notebook source
# MAGIC %md
# MAGIC # Part 2: Silver Transformations
# MAGIC Samed Budak | May 2026

# COMMAND ----------

from pyspark.sql.functions import (
    col, explode_outer,
    coalesce, lit, trim, to_date,
    current_timestamp,
    broadcast, rand, concat,
)
from pyspark.sql.types import IntegerType, DoubleType, StringType

# COMMAND ----------

# Config
CATALOG = "tvmaze_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_SCHEMA}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten shows

# COMMAND ----------

df_bronze_shows = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_shows")

print("Bronze shows schema (nested):")
df_bronze_shows.printSchema()

# COMMAND ----------

# Flatten nested structs into a clean, typed table
df_silver_shows = (
    df_bronze_shows
    .select(
        col("id").cast(IntegerType()).alias("show_id"),
        col("name").alias("show_name"),
        col("type").alias("show_type"),
        col("language"),
        col("genres"),  # array - exploded separately for genre analytics
        col("status"),
        col("runtime").cast(IntegerType()),
        col("averageRuntime").cast(IntegerType()).alias("average_runtime"),
        col("premiered").alias("premiered_date"),
        col("ended").alias("ended_date"),
        col("officialSite").alias("official_site"),

        # Flatten schedule struct
        col("schedule.time").alias("schedule_time"),
        col("schedule.days").alias("schedule_days"),

        # Flatten rating struct
        col("rating.average").cast(DoubleType()).alias("rating_average"),

        # Flatten network struct
        col("network.id").cast(IntegerType()).alias("network_id"),
        col("network.name").alias("network_name"),
        col("network.country.name").alias("network_country"),
        col("network.country.code").alias("network_country_code"),

        # Flatten webChannel struct
        col("webChannel.id").cast(IntegerType()).alias("web_channel_id"),
        col("webChannel.name").alias("web_channel_name"),

        # Flatten externals
        col("externals.imdb").alias("imdb_id"),
        col("externals.thetvdb").cast(IntegerType()).alias("thetvdb_id"),

        # Flatten image
        col("image.medium").alias("image_url"),

        col("weight").cast(IntegerType()),
        col("_ingestion_timestamp")
    )
    # null defaults - don't want NULLs breaking downstream aggs
    .withColumn("show_name", coalesce(col("show_name"), lit("Unknown")))
    .withColumn("language", coalesce(col("language"), lit("Unknown")))
    .withColumn("status", coalesce(col("status"), lit("Unknown")))
    .withColumn("runtime", coalesce(col("runtime"), col("average_runtime"), lit(0)))
    .withColumn("rating_average", coalesce(col("rating_average"), lit(0.0)))
    .withColumn("premiered_date", to_date(col("premiered_date"), "yyyy-MM-dd"))
    .withColumn("ended_date", to_date(col("ended_date"), "yyyy-MM-dd"))
    .withColumn("_silver_timestamp", current_timestamp())
    .filter(col("show_id").isNotNull())
)

print(f"silver_shows: {df_silver_shows.count()} records")
df_silver_shows.show(5, truncate=False)
# many shows come back with rating 0 (unrated) - that's expected from the API

# COMMAND ----------

# MAGIC %md
# MAGIC ### Normalize genres into rows
# MAGIC explode_outer keeps shows that have no genre (instead of dropping them like explode would).

# COMMAND ----------

df_shows_genres = (
    df_silver_shows
    .select("show_id", "show_name", explode_outer(col("genres")).alias("genre"))
    .withColumn("genre", coalesce(col("genre"), lit("Unclassified")))
    .withColumn("genre", trim(col("genre")))
)

print(f"show_genres normalized: {df_shows_genres.count()} rows")
df_shows_genres.show(10)

# COMMAND ----------

(
    df_silver_shows.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_shows")
)

(
    df_shows_genres.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_show_genres")
)

print("silver_shows and silver_show_genres written.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten episodes

# COMMAND ----------

df_bronze_episodes = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_episodes")

df_silver_episodes = (
    df_bronze_episodes
    .select(
        col("id").cast(IntegerType()).alias("episode_id"),
        col("show_id").cast(IntegerType()),
        col("name").alias("episode_name"),
        col("season").cast(IntegerType()),
        col("number").cast(IntegerType()).alias("episode_number"),
        col("type").alias("episode_type"),
        col("airdate"),
        col("airtime"),
        col("runtime").cast(IntegerType()).alias("episode_runtime"),
        col("rating.average").cast(DoubleType()).alias("episode_rating"),
        col("image.medium").alias("episode_image_url"),
        col("_ingestion_timestamp")
    )
    .withColumn("episode_name", coalesce(col("episode_name"), lit("Untitled")))
    .withColumn("episode_runtime", coalesce(col("episode_runtime"), lit(0)))
    .withColumn("episode_rating", coalesce(col("episode_rating"), lit(0.0)))
    .withColumn("airdate", to_date(col("airdate"), "yyyy-MM-dd"))
    .withColumn("_silver_timestamp", current_timestamp())
    .filter(col("episode_id").isNotNull())
    .filter(col("show_id").isNotNull())
)

(
    df_silver_episodes.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_episodes")
)

print(f"silver_episodes: {df_silver_episodes.count()} records")
df_silver_episodes.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Flatten cast
# MAGIC Cast is two levels deep (person.country.name, character.image.medium).

# COMMAND ----------

df_bronze_cast = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.bronze_cast")

print("Bronze cast schema (deeply nested):")
df_bronze_cast.printSchema()

# COMMAND ----------

df_silver_cast = (
    df_bronze_cast
    .select(
        col("show_id").cast(IntegerType()),

        # Flatten person struct
        col("person.id").cast(IntegerType()).alias("person_id"),
        col("person.name").alias("cast_name"),
        col("person.country.name").alias("person_country"),
        col("person.birthday").alias("person_birthday"),
        col("person.gender").alias("person_gender"),
        col("person.image.medium").alias("person_image_url"),

        # Flatten character struct
        col("character.id").cast(IntegerType()).alias("character_id"),
        col("character.name").alias("character_name"),
        col("character.image.medium").alias("character_image_url"),

        # Voice/self indicators
        col("self").alias("is_self"),
        col("voice").alias("is_voice"),

        col("_ingestion_timestamp")
    )
    .withColumn("cast_name", coalesce(col("cast_name"), lit("Unknown")))
    .withColumn("character_name", coalesce(col("character_name"), lit("Unknown")))
    .withColumn("person_country", coalesce(col("person_country"), lit("Unknown")))
    .withColumn("_silver_timestamp", current_timestamp())
    .filter(col("person_id").isNotNull())
    .filter(col("show_id").isNotNull())
)

(
    df_silver_cast.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.silver_cast")
)

print(f"silver_cast: {df_silver_cast.count()} records")
df_silver_cast.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build fact_show_data
# MAGIC Denormalized fact table joining shows, episodes, genres, and cast.
# MAGIC Join order: start with the largest table (episodes), broadcast the small ones.

# COMMAND ----------

df_shows = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_shows")
df_episodes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_episodes")
df_cast = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_cast")
df_genres = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_show_genres")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Broadcast join
# MAGIC Shows (~250 rows) and genres (~500 rows) are tiny, so broadcast them
# MAGIC to avoid shuffling.

# COMMAND ----------

# Step 1: episodes x shows (broadcast shows)
df_show_episodes = (
    df_episodes
    .join(
        broadcast(df_shows.select("show_id", "show_name", "language", "runtime")),
        on="show_id",
        how="inner"
    )
)

print(f"Shows x Episodes: {df_show_episodes.count()} rows")

# COMMAND ----------

# Step 2: + genres (broadcast)
df_show_ep_genre = (
    df_show_episodes
    .join(
        broadcast(df_genres.select("show_id", "genre")),
        on="show_id",
        how="left"
    )
)

print(f"+ Genres: {df_show_ep_genre.count()} rows")

# COMMAND ----------

# Step 3: + cast (NOT broadcast - can be large)
df_fact = (
    df_show_ep_genre
    .join(
        df_cast.select("show_id", "cast_name", "character_name"),
        on="show_id",
        how="left"
    )
)

# COMMAND ----------

df_fact_show_data = (
    df_fact
    .select(
        col("show_id"),
        col("show_name"),
        col("language"),
        col("genre"),
        col("season"),
        col("episode_name"),
        col("airdate"),
        col("episode_runtime").alias("runtime"),
        col("cast_name"),
        col("character_name")
    )
    .withColumn("_fact_created_at", current_timestamp())
    .dropDuplicates()
)

print(f"fact_show_data: {df_fact_show_data.count()} rows")
df_fact_show_data.show(10, truncate=False)
# TODO: add incremental refresh with watermark (only process new records)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save fact table
# MAGIC Partitioned by language (moderate cardinality, ~20 values).

# COMMAND ----------

(
    df_fact_show_data.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("language")
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.fact_show_data")
)

print(f"fact_show_data saved to {CATALOG}.{SILVER_SCHEMA}.fact_show_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Salting for skewed joins
# MAGIC Some shows have way more episodes than others (e.g. long-running show: 700+, miniseries: 6).
# MAGIC Salting spreads hot keys across partitions so no single executor gets overloaded.

# COMMAND ----------

NUM_SALT_BUCKETS = 4

# Add a random salt to the big table (episodes)
df_episodes_salted = (
    df_episodes
    .withColumn("salt", (rand() * NUM_SALT_BUCKETS).cast(IntegerType()))
    .withColumn("salted_key", concat(col("show_id").cast(StringType()), lit("_"), col("salt").cast(StringType())))
)

# Replicate the small table for each bucket so the join still works
salt_df = spark.range(NUM_SALT_BUCKETS).withColumnRenamed("id", "salt")
df_shows_salted = (
    df_shows.select("show_id", "show_name", "language")
    .crossJoin(broadcast(salt_df))
    .withColumn("salted_key", concat(col("show_id").cast(StringType()), lit("_"), col("salt").cast(StringType())))
)

df_salted_result = (
    df_episodes_salted
    .join(df_shows_salted, on="salted_key", how="inner")
    .drop("salt", "salted_key")
)

print(f"Salted join result: {df_salted_result.count()} rows")
print("At this scale there's no difference, but with millions of rows and hot keys it prevents executor OOM.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### OPTIMIZE + ZORDER BY
# MAGIC Compact small files and co-locate data by join keys for better data skipping.

# COMMAND ----------

spark.sql(f"OPTIMIZE {CATALOG}.{SILVER_SCHEMA}.fact_show_data ZORDER BY (show_id, genre)")
print("OPTIMIZE + ZORDER done on fact_show_data")

# COMMAND ----------

spark.sql(f"OPTIMIZE {CATALOG}.{SILVER_SCHEMA}.silver_shows ZORDER BY (show_id)")
spark.sql(f"OPTIMIZE {CATALOG}.{SILVER_SCHEMA}.silver_episodes ZORDER BY (show_id, season)")
spark.sql(f"OPTIMIZE {CATALOG}.{SILVER_SCHEMA}.silver_cast ZORDER BY (show_id, person_id)")

print("All Silver tables optimized.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation

# COMMAND ----------

for table in ["silver_shows", "silver_episodes", "silver_cast", "silver_show_genres", "fact_show_data"]:
    cnt = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.{table}").count()
    print(f"  {table}: {cnt:,} records")

spark.table(f"{CATALOG}.{SILVER_SCHEMA}.fact_show_data").show(5, truncate=False)
