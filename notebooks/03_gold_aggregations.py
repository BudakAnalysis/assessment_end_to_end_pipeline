# Databricks notebook source
# MAGIC %md
# MAGIC # Part 3: Gold Layer - Business Analytics
# MAGIC Samed Budak | May 2026

# COMMAND ----------

from pyspark.sql.functions import (
    col, count, avg, sum as _sum, max as _max, min as _min,
    dense_rank, row_number, rank, percent_rank,
    round as _round, desc, asc, first, collect_list, concat_ws,
    current_timestamp, lit, when, coalesce
)
from pyspark.sql.window import Window

# COMMAND ----------

CATALOG = "tvmaze_catalog"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

# Load Silver tables
df_shows = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_shows")
df_episodes = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_episodes")
df_cast = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_cast")
df_genres = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.silver_show_genres")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Episodes per season

# COMMAND ----------

window_season_rank = Window.partitionBy("show_id").orderBy(desc("episode_count"))

df_episodes_per_season = (
    df_episodes
    .groupBy("show_id", "season")
    .agg(
        count("episode_id").alias("episode_count"),
        _round(avg("episode_runtime"), 1).alias("avg_episode_runtime"),
        _min("airdate").alias("season_start_date"),
        _max("airdate").alias("season_end_date"),
        _round(avg("episode_rating"), 2).alias("avg_season_rating")
    )
    .join(
        df_shows.select("show_id", "show_name"),
        on="show_id",
        how="inner"
    )
    # Rank seasons by ep count, running total across seasons
    .withColumn("season_rank_by_episodes", dense_rank().over(window_season_rank))
    .withColumn(
        "cumulative_episodes",
        _sum("episode_count").over(
            Window.partitionBy("show_id").orderBy("season").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
    )
    .withColumn("_gold_timestamp", current_timestamp())
    .orderBy("show_id", "season")
)

(
    df_episodes_per_season.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.gold_episodes_per_season")
)

print("gold_episodes_per_season done")
df_episodes_per_season.show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Average runtime per show

# COMMAND ----------

window_runtime_rank = Window.orderBy(desc("avg_runtime_minutes"))

df_avg_runtime = (
    df_episodes
    .filter(col("episode_runtime") > 0)
    .groupBy("show_id")
    .agg(
        _round(avg("episode_runtime"), 1).alias("avg_runtime_minutes"),
        _min("episode_runtime").alias("min_runtime_minutes"),
        _max("episode_runtime").alias("max_runtime_minutes"),
        count("episode_id").alias("total_episodes"),
        _round(avg("episode_rating"), 2).alias("avg_episode_rating")
    )
    .join(
        df_shows.select("show_id", "show_name", "language", "status", "rating_average"),
        on="show_id",
        how="inner"
    )
    .withColumn("runtime_rank", dense_rank().over(window_runtime_rank))
    .withColumn("runtime_percentile", _round(percent_rank().over(window_runtime_rank) * 100, 1))
    .withColumn("_gold_timestamp", current_timestamp())
    .orderBy("runtime_rank")
)

(
    df_avg_runtime.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.gold_avg_runtime_per_show")
)

print("gold_avg_runtime_per_show done")
df_avg_runtime.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Top cast members

# COMMAND ----------

window_cast_rank = Window.orderBy(desc("show_count"))

df_top_cast = (
    df_cast
    .groupBy("person_id", "cast_name", "person_country", "person_gender")
    .agg(
        count("show_id").alias("show_count"),
        collect_list("character_name").alias("characters_played"),
        first("person_image_url").alias("image_url")
    )
    .withColumn("cast_rank", dense_rank().over(window_cast_rank))
    .withColumn("cast_percentile", _round(percent_rank().over(window_cast_rank) * 100, 1))
    .withColumn("characters_list", concat_ws(", ", col("characters_played")))
    .drop("characters_played")
    .withColumn("_gold_timestamp", current_timestamp())
    .orderBy("cast_rank")
)

(
    df_top_cast.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.gold_top_cast_members")
)

print("gold_top_cast_members done")
df_top_cast.filter(col("cast_rank") <= 20).show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Genre statistics

# COMMAND ----------

window_genre_rank = Window.orderBy(desc("show_count"))

df_genre_stats = (
    df_genres
    .groupBy("genre")
    .agg(
        count("show_id").alias("show_count")
    )
    .join(
        # Avg rating per genre
        df_genres.join(
            df_shows.select("show_id", "rating_average"),
            on="show_id"
        )
        .groupBy("genre")
        .agg(
            _round(avg("rating_average"), 2).alias("avg_genre_rating")
        ),
        on="genre",
        how="left"
    )
    .withColumn("genre_rank", dense_rank().over(window_genre_rank))
    .withColumn("genre_share_pct", _round(
        col("show_count") / _sum("show_count").over(Window.partitionBy()) * 100, 1
    ))
    .withColumn("_gold_timestamp", current_timestamp())
    .orderBy("genre_rank")
)

(
    df_genre_stats.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.gold_genre_statistics")
)

print("genre stats done")
df_genre_stats.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show dashboard
# MAGIC Comprehensive KPI table that pulls from all dimensions. Designed to feed
# MAGIC a Databricks SQL dashboard or Power BI directly.

# COMMAND ----------

window_overall = Window.orderBy(desc("rating_average"))

df_show_dashboard = (
    df_shows
    .join(
        df_episodes.groupBy("show_id").agg(
            count("*").alias("total_episodes"),
            _max("season").alias("total_seasons"),
            _round(avg("episode_runtime"), 1).alias("avg_episode_runtime"),
            _round(avg("episode_rating"), 2).alias("avg_episode_rating")
        ),
        on="show_id",
        how="left"
    )
    .join(
        df_cast.groupBy("show_id").agg(
            count("person_id").alias("cast_size")
        ),
        on="show_id",
        how="left"
    )
    .join(
        df_genres.groupBy("show_id").agg(
            concat_ws(", ", collect_list("genre")).alias("genre_list")
        ),
        on="show_id",
        how="left"
    )
    .select(
        "show_id", "show_name", "language", "status",
        "rating_average", "premiered_date", "ended_date",
        "network_name", "genre_list",
        coalesce("total_episodes", lit(0)).alias("total_episodes"),
        coalesce("total_seasons", lit(0)).alias("total_seasons"),
        coalesce("avg_episode_runtime", lit(0)).alias("avg_episode_runtime"),
        coalesce("avg_episode_rating", lit(0)).alias("avg_episode_rating"),
        coalesce("cast_size", lit(0)).alias("cast_size")
    )
    .withColumn("overall_rank", dense_rank().over(window_overall))
    .withColumn("_gold_timestamp", current_timestamp())
)

(
    df_show_dashboard.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.gold_show_dashboard")
)

print("gold_show_dashboard done")
df_show_dashboard.orderBy("overall_rank").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Optimize Gold tables

# COMMAND ----------

spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.gold_episodes_per_season ZORDER BY (show_id)")
spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.gold_avg_runtime_per_show ZORDER BY (show_id)")
spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.gold_top_cast_members ZORDER BY (cast_rank)")
spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.gold_genre_statistics ZORDER BY (genre_rank)")
spark.sql(f"OPTIMIZE {CATALOG}.{GOLD_SCHEMA}.gold_show_dashboard ZORDER BY (overall_rank)")

print("All Gold tables optimized.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## What these tables are for
# MAGIC - **episodes_per_season** + **avg_runtime**: content format trends
# MAGIC - **top_cast**: which actors show up across the most shows
# MAGIC - **genre_statistics**: portfolio breakdown
# MAGIC - **show_dashboard**: single KPI table, plug straight into a SQL dashboard or Power BI
# MAGIC
# MAGIC TODO: add a scheduling/time-based Gold table once we have enough historical data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top 10 shows by rating with episode and cast metrics
# MAGIC SELECT
# MAGIC   show_name,
# MAGIC   rating_average,
# MAGIC   total_episodes,
# MAGIC   total_seasons,
# MAGIC   cast_size,
# MAGIC   genre_list,
# MAGIC   overall_rank
# MAGIC FROM tvmaze_catalog.gold.gold_show_dashboard
# MAGIC WHERE total_episodes > 0
# MAGIC ORDER BY overall_rank
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Genre distribution
# MAGIC SELECT
# MAGIC   genre,
# MAGIC   show_count,
# MAGIC   avg_genre_rating,
# MAGIC   genre_share_pct
# MAGIC FROM tvmaze_catalog.gold.gold_genre_statistics
# MAGIC ORDER BY genre_rank;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Most prolific actors
# MAGIC SELECT
# MAGIC   cast_name,
# MAGIC   show_count,
# MAGIC   person_gender,
# MAGIC   person_country,
# MAGIC   characters_list
# MAGIC FROM tvmaze_catalog.gold.gold_top_cast_members
# MAGIC WHERE cast_rank <= 15
# MAGIC ORDER BY cast_rank;
