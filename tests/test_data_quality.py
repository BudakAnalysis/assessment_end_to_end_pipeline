# tests/test_data_quality.py
# DQ checks for TVMaze medallion pipeline.

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, DoubleType, ArrayType, LongType

# Catalog/schema names - same as the notebooks use.
# In CI you'd override these via env vars or conftest parametrize.
CATALOG = "tvmaze_catalog"
BRONZE = "bronze"
SILVER = "silver"
GOLD = "gold"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for testing - no cluster needed."""
    return (
        SparkSession.builder
        .master("local[*]")
        .appName("tvmaze_dq_tests")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def bronze_shows(spark):
    return spark.table(f"{CATALOG}.{BRONZE}.bronze_shows")


@pytest.fixture(scope="session")
def bronze_episodes(spark):
    return spark.table(f"{CATALOG}.{BRONZE}.bronze_episodes")


@pytest.fixture(scope="session")
def silver_shows(spark):
    return spark.table(f"{CATALOG}.{SILVER}.silver_shows")


@pytest.fixture(scope="session")
def silver_episodes(spark):
    return spark.table(f"{CATALOG}.{SILVER}.silver_episodes")


@pytest.fixture(scope="session")
def silver_cast(spark):
    return spark.table(f"{CATALOG}.{SILVER}.silver_cast")


# ---------------------------------------------------------------------------
# Shows DQ
# ---------------------------------------------------------------------------

class TestShowsDQ:
    """Core data quality on shows tables."""

    def test_show_id_not_null(self, silver_shows):
        nulls = silver_shows.filter("show_id IS NULL").count()
        assert nulls == 0, f"Found {nulls} null show_ids"

    def test_show_id_unique(self, silver_shows):
        total = silver_shows.count()
        distinct = silver_shows.select("show_id").distinct().count()
        assert total == distinct, f"Dupes: {total - distinct}"

    def test_rating_range(self, silver_shows):
        """Rating should be 0-10 or null (not all shows are rated)."""
        bad = silver_shows.filter(
            "rating_average IS NOT NULL AND (rating_average < 0 OR rating_average > 10)"
        ).count()
        assert bad == 0, f"{bad} ratings outside 0-10"

    def test_runtime_positive(self, silver_shows):
        bad = silver_shows.filter("runtime IS NOT NULL AND runtime <= 0").count()
        assert bad == 0, f"{bad} shows with runtime <= 0"


# ---------------------------------------------------------------------------
# Episodes DQ
# ---------------------------------------------------------------------------

class TestEpisodesDQ:
    """Episode-level checks."""

    def test_episode_id_not_null(self, silver_episodes):
        nulls = silver_episodes.filter("episode_id IS NULL").count()
        assert nulls == 0

    def test_episode_show_ref(self, silver_episodes, silver_shows):
        """Every episode should point to a known show (referential integrity)."""
        show_ids = silver_shows.select("show_id")
        orphans = silver_episodes.join(show_ids, "show_id", "left_anti").count()
        assert orphans == 0, f"{orphans} orphan episodes"


# ---------------------------------------------------------------------------
# Cast DQ
# ---------------------------------------------------------------------------

class TestCastDQ:
    def test_person_id_not_null(self, silver_cast):
        nulls = silver_cast.filter("person_id IS NULL").count()
        assert nulls == 0

    def test_cast_show_ref(self, silver_cast, silver_shows):
        show_ids = silver_shows.select("show_id")
        orphans = silver_cast.join(show_ids, "show_id", "left_anti").count()
        assert orphans == 0, f"{orphans} cast rows with unknown show_id"


# ---------------------------------------------------------------------------
# Schema checks (Bronze + Silver combined)
# ---------------------------------------------------------------------------

class TestSchemas:
    """Validate column presence and types across layers.
    Merged Bronze/Silver Delta checks into one class to keep it compact.
    """

    # Subset of expected Bronze columns (API schema can grow - that's fine)
    BRONZE_SHOW_COLS = {"id", "name", "type", "language", "genres", "status",
                        "runtime", "premiered", "rating", "network",
                        "_ingestion_timestamp", "_source"}

    # Silver shows columns we care about (from 02_silver_transformations.py)
    SILVER_SHOW_COLS = {"show_id", "show_name", "show_type", "language",
                        "status", "runtime", "premiered_date", "rating_average",
                        "network_name", "network_country",
                        "_silver_timestamp"}

    def test_bronze_shows_columns(self, bronze_shows):
        actual = set(bronze_shows.columns)
        missing = self.BRONZE_SHOW_COLS - actual
        assert not missing, f"Missing Bronze cols: {missing}"

    def test_silver_shows_columns(self, silver_shows):
        actual = set(silver_shows.columns)
        missing = self.SILVER_SHOW_COLS - actual
        assert not missing, f"Missing Silver cols: {missing}"

    def test_silver_type_checks(self, silver_shows):
        """Quick sanity: a few key columns have expected types."""
        schema = {f.name: f.dataType for f in silver_shows.schema.fields}
        assert isinstance(schema["show_id"], (IntegerType, LongType))
        assert isinstance(schema["rating_average"], DoubleType)
        assert isinstance(schema["runtime"], (IntegerType, LongType))

    def test_delta_schema_evolution(self, spark):
        """
        Verify mergeSchema worked - if we added columns post-initial load
        the table should still be readable without errors.
        Just reading it is enough; if schema evolution broke, this would throw.
        """
        for table in [f"{CATALOG}.{BRONZE}.bronze_shows",
                      f"{CATALOG}.{SILVER}.silver_shows"]:
            df = spark.table(table)
            assert df.columns, f"{table} has no columns after schema evolution"


# ---------------------------------------------------------------------------
# Transformation logic spot-check
# ---------------------------------------------------------------------------

class TestTransformLogic:
    """Verify a couple of Silver/Gold transform outcomes."""

    def test_genre_normalisation(self, spark):
        """After explode_outer each row should have a single genre string."""
        genres = spark.table(f"{CATALOG}.{SILVER}.silver_show_genres")
        array_cols = [f.name for f in genres.schema.fields
                      if isinstance(f.dataType, ArrayType)]
        assert "genre" not in array_cols, "genre column should be scalar after explode"

    def test_gold_dashboard_has_episode_count(self, spark):
        """gold_show_dashboard should contain total_episodes from the agg join."""
        dashboard = spark.table(f"{CATALOG}.{GOLD}.gold_show_dashboard")
        assert "total_episodes" in dashboard.columns


# ---------------------------------------------------------------------------
# Failure simulation (shows the checks actually catch problems)
# ---------------------------------------------------------------------------

class TestFailureSimulation:
    """Inject bad data and verify our checks detect it."""

    def test_null_injection_detected(self, spark):
        """Create a df with null show_id and confirm our null check would flag it."""
        from pyspark.sql import Row
        bad_rows = [Row(show_id=None, show_name="Bad Show", rating_average=5.0)]
        bad_df = spark.createDataFrame(bad_rows)
        nulls = bad_df.filter("show_id IS NULL").count()
        assert nulls > 0, "Null injection should be detected"

