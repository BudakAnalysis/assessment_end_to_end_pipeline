-- Part 5: SQL Challenge
-- Samed Budak | May 2026
--
-- Prerequisites: set catalog + schema context before running.
-- In Databricks SQL Editor, use the dropdown. In a notebook:
--   USE CATALOG tvmaze_catalog;
--   USE SCHEMA silver;

-- ============================================================
-- SECTION 1: Window Functions
-- Rank episodes by rating within each season.
-- ============================================================

-- 1A: Episode rankings per season
WITH episode_rankings AS (
    SELECT
        s.show_name,
        e.season,
        e.episode_number,
        e.episode_name,
        e.episode_rating,
        e.episode_runtime,
        e.airdate,

        RANK()        OVER (PARTITION BY e.show_id, e.season ORDER BY e.episode_rating DESC) AS rating_rank,
        DENSE_RANK()  OVER (PARTITION BY e.show_id, e.season ORDER BY e.episode_rating DESC) AS rating_dense_rank,
        ROW_NUMBER()  OVER (PARTITION BY e.show_id, e.season ORDER BY e.episode_rating DESC) AS rating_row_num,

        -- Running avg across episodes in the season
        AVG(e.episode_rating) OVER (
            PARTITION BY e.show_id, e.season
            ORDER BY e.episode_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS running_avg_rating,

        LAG(e.episode_rating, 1)  OVER (PARTITION BY e.show_id, e.season ORDER BY e.episode_number) AS prev_episode_rating,
        LEAD(e.episode_rating, 1) OVER (PARTITION BY e.show_id, e.season ORDER BY e.episode_number) AS next_episode_rating,

        -- Rating delta from previous episode
        e.episode_rating - LAG(e.episode_rating, 1) OVER (
            PARTITION BY e.show_id, e.season ORDER BY e.episode_number
        ) AS rating_change,

        PERCENT_RANK() OVER (PARTITION BY e.show_id, e.season ORDER BY e.episode_rating) AS rating_percentile

    FROM silver_episodes e
    INNER JOIN silver_shows s ON e.show_id = s.show_id
    WHERE e.episode_rating > 0
)
SELECT *
FROM episode_rankings
WHERE rating_rank = 1  -- top-rated episode per season
ORDER BY show_name, season;


-- 1B: Season trajectory (did the finale beat the premiere?)
-- Window functions return same premiere/finale per partition row,
-- so wrap in CTE and deduplicate to one row per season.
WITH season_bounds AS (
    SELECT DISTINCT
        e.show_id,
        s.show_name,
        e.season,

        FIRST_VALUE(e.episode_rating) OVER (
            PARTITION BY e.show_id, e.season
            ORDER BY e.episode_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS season_premiere_rating,

        LAST_VALUE(e.episode_rating) OVER (
            PARTITION BY e.show_id, e.season
            ORDER BY e.episode_number
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS season_finale_rating

    FROM silver_episodes e
    INNER JOIN silver_shows s ON e.show_id = s.show_id
    WHERE e.episode_rating > 0
)
SELECT
    show_name,
    season,
    season_premiere_rating,
    season_finale_rating,
    CASE
        WHEN season_finale_rating > season_premiere_rating THEN 'Improved'
        ELSE 'Declined'
    END AS season_trajectory
FROM season_bounds
ORDER BY show_name, season;


-- ============================================================
-- SECTION 2: Joins and Aggregations
-- Full show report combining shows, episodes, cast, and genres.
-- ============================================================

-- 2A: Comprehensive show report
SELECT
    s.show_name,
    s.language,
    s.status,
    s.network_name,
    s.rating_average AS show_rating,

    COUNT(DISTINCT e.episode_id) AS total_episodes,
    COUNT(DISTINCT e.season)     AS total_seasons,
    ROUND(AVG(e.episode_runtime), 1) AS avg_runtime_min,
    ROUND(AVG(e.episode_rating), 2)  AS avg_episode_rating,
    MIN(e.airdate) AS first_aired,
    MAX(e.airdate) AS last_aired,

    COUNT(DISTINCT c.person_id) AS cast_size,
    CONCAT_WS(', ', COLLECT_SET(g.genre)) AS genres

FROM silver_shows s
LEFT JOIN silver_episodes e ON s.show_id = e.show_id
LEFT JOIN silver_cast c     ON s.show_id = c.show_id
LEFT JOIN silver_show_genres g ON s.show_id = g.show_id

GROUP BY
    s.show_id, s.show_name, s.language, s.status,
    s.network_name, s.rating_average

HAVING COUNT(DISTINCT e.episode_id) > 0
ORDER BY s.rating_average DESC
LIMIT 25;


-- 2B: Network comparison
SELECT
    COALESCE(s.network_name, s.web_channel_name, 'Unknown') AS network,
    COUNT(DISTINCT s.show_id)        AS total_shows,
    ROUND(AVG(s.rating_average), 2)  AS avg_show_rating,
    SUM(ep_stats.total_episodes)     AS total_episodes_produced,
    ROUND(AVG(ep_stats.avg_runtime), 1) AS avg_episode_runtime,
    ROUND(AVG(cast_stats.cast_size), 0) AS avg_cast_size

FROM silver_shows s

LEFT JOIN (
    SELECT show_id, COUNT(*) AS total_episodes, AVG(episode_runtime) AS avg_runtime
    FROM silver_episodes GROUP BY show_id
) ep_stats ON s.show_id = ep_stats.show_id

LEFT JOIN (
    SELECT show_id, COUNT(DISTINCT person_id) AS cast_size
    FROM silver_cast GROUP BY show_id
) cast_stats ON s.show_id = cast_stats.show_id

GROUP BY COALESCE(s.network_name, s.web_channel_name, 'Unknown')
HAVING COUNT(DISTINCT s.show_id) >= 3
ORDER BY avg_show_rating DESC;


-- ============================================================
-- SECTION 3: Performance Tuning
-- ============================================================

-- 3A: BEFORE - correlated subqueries (anti-pattern, kept for comparison)
/*
SELECT
    s.show_name,
    s.rating_average,
    (SELECT COUNT(*) FROM silver_episodes e WHERE e.show_id = s.show_id) AS ep_count,
    (SELECT AVG(episode_runtime) FROM silver_episodes e WHERE e.show_id = s.show_id) AS avg_rt,
    (SELECT COUNT(DISTINCT person_id) FROM silver_cast c WHERE c.show_id = s.show_id) AS cast_sz
FROM silver_shows s
WHERE s.rating_average > (
    SELECT AVG(rating_average) FROM silver_shows
)
ORDER BY s.rating_average DESC;
*/

-- 3B: AFTER - CTEs with pre-aggregation
WITH avg_rating AS (
    SELECT AVG(rating_average) AS overall_avg
    FROM silver_shows
    WHERE rating_average > 0
),

episode_stats AS (
    SELECT show_id, COUNT(*) AS episode_count, AVG(episode_runtime) AS avg_runtime
    FROM silver_episodes
    GROUP BY show_id
),

cast_stats AS (
    SELECT show_id, COUNT(DISTINCT person_id) AS cast_size
    FROM silver_cast
    GROUP BY show_id
)

SELECT
    s.show_name,
    s.rating_average,
    COALESCE(es.episode_count, 0) AS episode_count,
    ROUND(COALESCE(es.avg_runtime, 0), 1) AS avg_runtime,
    COALESCE(cs.cast_size, 0) AS cast_size
FROM silver_shows s
CROSS JOIN avg_rating ar
LEFT JOIN episode_stats es ON s.show_id = es.show_id
LEFT JOIN cast_stats cs    ON s.show_id = cs.show_id
WHERE s.rating_average > ar.overall_avg
ORDER BY s.rating_average DESC;


-- Performance explanation:
-- Correlated subqueries are a classic performance killer - they run the inner query
-- once per row in the outer SELECT. Replacing them with CTEs pre-aggregates and joins
-- in one pass. On Databricks, this also lets the optimizer pick broadcast joins for
-- small aggregates. ZORDER BY (show_id) on the episodes/cast tables gives us Delta
-- data skipping (min/max stats per file), so the JOINs only read relevant files.
-- The WHERE filter on rating_average applies at scan time via predicate pushdown,
-- reducing the row set before any joins happen.
