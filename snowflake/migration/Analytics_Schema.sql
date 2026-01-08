-- V1.1.2__Analytics_Schema.sql
-- Create ANALYTICS schema with sentiment analysis on news data
-- This migration sets up streams, procedures, tasks, and views for automated sentiment analysis

-- =====================================================
-- ANALYTICS SCHEMA CREATION
-- =====================================================

CREATE DATABASE IF NOT EXISTS BITCOIN_DATA;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- =====================================================
-- NEWS SENTIMENT TABLE
-- =====================================================

-- Table to store sentiment analysis results for each news article
CREATE OR REPLACE TABLE BITCOIN_DATA.ANALYTICS.NEWS_SENTIMENT (
    ID NUMBER,                              -- News article ID (FK to COINDESK.NEWS)
    PUBLISHED_ON NUMBER,                    -- Timestamp of publication
    PUBLISHED_DATETIME TIMESTAMP_TZ,        -- Converted datetime for easier querying
    TITLE STRING,                           -- News title
    SOURCE STRING,                          -- News source
    BODY STRING,                            -- Full article text
    -- Sentiment scores from Cortex
    SENTIMENT_SCORE FLOAT,                  -- Overall sentiment (-1 to 1)
    SENTIMENT_LABEL STRING,                 -- POSITIVE, NEUTRAL, NEGATIVE
    TITLE_SENTIMENT_SCORE FLOAT,           -- Sentiment of title only
    -- Metadata
    ANALYZED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (ID)
);

-- Hourly sentiment analysis results
CREATE OR REPLACE TABLE BTC_DATA.ANALYTICS.HOURLY_FNG (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    analysis_date DATE,
    analysis_hour INTEGER,
    datetime_hour TIMESTAMP,
    total_articles INTEGER,
    avg_sentiment_score FLOAT,
    fear_greed_score INTEGER,
    fear_greed_category STRING,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    source_article_ids ARRAY
);

-- Daily aggregated Fear & Greed Index 
CREATE OR REPLACE TABLE BTC_DATA.ANALYTICS.DAILY_FNG (
    id INTEGER AUTOINCREMENT PRIMARY KEY,
    analysis_date DATE,
    total_articles INTEGER,
    total_hours_analyzed INTEGER,
    avg_sentiment_score FLOAT,
    daily_fear_greed_score INTEGER,
    fear_greed_category STRING,
    hourly_scores ARRAY,
    processing_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
-- =====================================================
-- FEAR & GREED NEWS INDEX - HOURLY
-- =====================================================

-- Hourly aggregated sentiment index (0-100 scale, like traditional F&G index)
-- 0-24: Extreme Fear, 25-44: Fear, 45-55: Neutral, 56-75: Greed, 76-100: Extreme Greed
CREATE OR REPLACE TABLE BITCOIN_DATA.ANALYTICS.FNG_NEWS_INDEX_HOURLY (
    HOUR_START TIMESTAMP_TZ,               -- Start of the hour
    -- Article counts
    TOTAL_ARTICLES INTEGER,                -- Total articles in this hour
    POSITIVE_ARTICLES INTEGER,             -- Number of positive articles
    NEUTRAL_ARTICLES INTEGER,              -- Number of neutral articles
    NEGATIVE_ARTICLES INTEGER,             -- Number of negative articles
    -- Sentiment aggregations
    AVG_SENTIMENT_SCORE FLOAT,             -- Average sentiment score
    WEIGHTED_SENTIMENT_SCORE FLOAT,        -- Weighted by upvotes/engagement
    -- Fear & Greed Index (0-100)
    FNG_INDEX_VALUE FLOAT,                 -- 0-100 scale
    FNG_INDEX_LABEL STRING,                -- Extreme Fear, Fear, Neutral, Greed, Extreme Greed
    -- Metadata
    CALCULATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (HOUR_START)
);

-- =====================================================
-- FEAR & GREED NEWS INDEX - DAILY
-- =====================================================

-- Daily aggregated sentiment index
CREATE OR REPLACE TABLE BITCOIN_DATA.ANALYTICS.FNG_NEWS_INDEX_DAILY (
    DATE DATE,                             -- Date (UTC)
    -- Article counts
    TOTAL_ARTICLES INTEGER,                -- Total articles for the day
    POSITIVE_ARTICLES INTEGER,             -- Number of positive articles
    NEUTRAL_ARTICLES INTEGER,              -- Number of neutral articles
    NEGATIVE_ARTICLES INTEGER,             -- Number of negative articles
    -- Sentiment aggregations
    AVG_SENTIMENT_SCORE FLOAT,             -- Average sentiment score
    WEIGHTED_SENTIMENT_SCORE FLOAT,        -- Weighted by upvotes/engagement
    MIN_SENTIMENT_SCORE FLOAT,             -- Most negative score
    MAX_SENTIMENT_SCORE FLOAT,             -- Most positive score
    STDDEV_SENTIMENT_SCORE FLOAT,          -- Volatility of sentiment
    -- Fear & Greed Index (0-100)
    FNG_INDEX_VALUE FLOAT,                 -- 0-100 scale
    FNG_INDEX_LABEL STRING,                -- Extreme Fear, Fear, Neutral, Greed, Extreme Greed
    -- Daily change
    FNG_INDEX_CHANGE FLOAT,                -- Change from previous day
    FNG_INDEX_CHANGE_PCT FLOAT,            -- Percentage change from previous day
    -- Metadata
    CALCULATED_AT TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (DATE)
);

-- =====================================================
-- CREATE COINDESK SCHEMA AND NEWS TABLE
-- =====================================================

-- Create COINDESK schema
CREATE SCHEMA IF NOT EXISTS COINDESK;

-- Create NEWS table with same structure as source
CREATE OR REPLACE TABLE BITCOIN_DATA.COINDESK.NEWS (
	ID NUMBER(38,0) NOT NULL AUTOINCREMENT,
	GUID VARCHAR(16777216),
	PUBLISHED_ON NUMBER(38,0),
	IMAGEURL VARCHAR(16777216),
	TITLE VARCHAR(16777216),
	URL VARCHAR(16777216),
	SOURCE VARCHAR(16777216),
	BODY VARCHAR(16777216),
	TAGS VARCHAR(16777216),
	CATEGORIES VARCHAR(16777216),
	UPVOTES NUMBER(38,0),
	DOWNVOTES NUMBER(38,0),
	LANG VARCHAR(16777216),
	SOURCE_INFO VARIANT,
	FETCHED_AT TIMESTAMP_TZ(9) DEFAULT CURRENT_TIMESTAMP(),
	PRIMARY KEY (ID)
);

-- Stream to capture new inserts into COINDESK.NEWS
-- This enables continuous processing of news articles
CREATE OR REPLACE STREAM ANALYTICS.NEWS_STREAM
ON TABLE BITCOIN_DATA.COINDESK.NEWS
APPEND_ONLY = TRUE;

-- Populate NEWS table with historical data from shared database
INSERT INTO COINDESK.NEWS (GUID, PUBLISHED_ON, IMAGEURL, TITLE, URL, SOURCE, BODY, TAGS, CATEGORIES, UPVOTES, DOWNVOTES, LANG, SOURCE_INFO)
SELECT GUID, PUBLISHED_ON, IMAGEURL, TITLE, URL, SOURCE, BODY, TAGS, CATEGORIES, UPVOTES, DOWNVOTES, LANG, SOURCE_INFO
FROM BITCOIN_DATA_SHARED.COINDESK.NEWS;


-- STORED PROCEDURES FOR SENTIMENT ANALYSIS
-- Procedure 1: Analyze sentiment for new news articles using Cortex
CREATE OR REPLACE PROCEDURE BITCOIN_DATA.ANALYTICS.ANALYZE_NEWS_SENTIMENT()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_processed INT DEFAULT 0;
BEGIN
    -- Insert new articles with refined sentiment analysis labels
    INSERT INTO BITCOIN_DATA.ANALYTICS.NEWS_SENTIMENT (
        ID,
        PUBLISHED_ON,
        PUBLISHED_DATETIME,
        TITLE,
        SOURCE,
        BODY,
        SENTIMENT_SCORE,
        SENTIMENT_LABEL,
        TITLE_SENTIMENT_SCORE
    )
    SELECT
        n.ID,
        n.PUBLISHED_ON,
        TO_TIMESTAMP(n.PUBLISHED_ON),
        n.TITLE,
        n.SOURCE,
        n.BODY,
        -- Cortex sentiment analysis on body text
        SNOWFLAKE.CORTEX.SENTIMENT(n.BODY),
        -- Classify sentiment into 5 categories
        CASE
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.BODY) <= -0.6 THEN 'EXTREMELY FEAR'
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.BODY) <= -0.1 THEN 'FEAR'
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.BODY) < 0.1 THEN 'NEUTRAL'
            WHEN SNOWFLAKE.CORTEX.SENTIMENT(n.BODY) < 0.6 THEN 'GREED'
            ELSE 'EXTREMELY GREED'
        END,
        -- Sentiment of title only
        SNOWFLAKE.CORTEX.SENTIMENT(n.TITLE)
    FROM BITCOIN_DATA.ANALYTICS.NEWS_STREAM n
    WHERE METADATA$ACTION = 'INSERT'
    AND METADATA$ISUPDATE = FALSE;

    rows_processed := SQLROWCOUNT;

    RETURN 'Processed ' || rows_processed || ' news articles with refined labels';
END;
$$;

-- Procedure 2: Calculate hourly Fear & Greed Index
CREATE OR REPLACE PROCEDURE BITCOIN_DATA.ANALYTICS.CALCULATE_HOURLY_FNG_INDEX()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_calculated INT DEFAULT 0;
BEGIN
    -- Calculate hourly aggregations with refined label counts
    MERGE INTO BITCOIN_DATA.ANALYTICS.FNG_NEWS_INDEX_HOURLY AS target
    USING (
        SELECT
            DATE_TRUNC('HOUR', PUBLISHED_DATETIME) AS HOUR_START,
            COUNT(*) AS TOTAL_ARTICLES,
            SUM(CASE WHEN SENTIMENT_LABEL IN ('GREED', 'EXTREMELY GREED') THEN 1 ELSE 0 END) AS POSITIVE_ARTICLES,
            SUM(CASE WHEN SENTIMENT_LABEL = 'NEUTRAL' THEN 1 ELSE 0 END) AS NEUTRAL_ARTICLES,
            SUM(CASE WHEN SENTIMENT_LABEL IN ('FEAR', 'EXTREMELY FEAR') THEN 1 ELSE 0 END) AS NEGATIVE_ARTICLES,
            AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT_SCORE,
            ((AVG(SENTIMENT_SCORE) + 1) / 2) * 100 AS FNG_INDEX_VALUE
        FROM BITCOIN_DATA.ANALYTICS.NEWS_SENTIMENT
        WHERE PUBLISHED_DATETIME >= DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
        GROUP BY DATE_TRUNC('HOUR', PUBLISHED_DATETIME)
    ) AS source
    ON target.HOUR_START = source.HOUR_START
    WHEN MATCHED THEN UPDATE SET
        target.TOTAL_ARTICLES = source.TOTAL_ARTICLES,
        target.POSITIVE_ARTICLES = source.POSITIVE_ARTICLES,
        target.NEUTRAL_ARTICLES = source.NEUTRAL_ARTICLES,
        target.NEGATIVE_ARTICLES = source.NEGATIVE_ARTICLES,
        target.AVG_SENTIMENT_SCORE = source.AVG_SENTIMENT_SCORE,
        target.FNG_INDEX_VALUE = source.FNG_INDEX_VALUE,
        target.FNG_INDEX_LABEL = CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END,
        target.CALCULATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        HOUR_START,
        TOTAL_ARTICLES,
        POSITIVE_ARTICLES,
        NEUTRAL_ARTICLES,
        NEGATIVE_ARTICLES,
        AVG_SENTIMENT_SCORE,
        FNG_INDEX_VALUE,
        FNG_INDEX_LABEL
    ) VALUES (
        source.HOUR_START,
        source.TOTAL_ARTICLES,
        source.POSITIVE_ARTICLES,
        source.NEUTRAL_ARTICLES,
        source.NEGATIVE_ARTICLES,
        source.AVG_SENTIMENT_SCORE,
        source.FNG_INDEX_VALUE,
        CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END
    );

    rows_calculated := SQLROWCOUNT;

    RETURN 'Calculated hourly F&G index for ' || rows_calculated || ' hours';
END;
$$;

-- Procedure 3: Calculate daily Fear & Greed Index
CREATE OR REPLACE PROCEDURE BITCOIN_DATA.ANALYTICS.CALCULATE_DAILY_FNG_INDEX()
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE
    rows_calculated INT DEFAULT 0;
BEGIN
    -- Calculate daily aggregations with refined label counts
    MERGE INTO BITCOIN_DATA.ANALYTICS.FNG_NEWS_INDEX_DAILY AS target
    USING (
        WITH daily_stats AS (
            SELECT
                DATE_TRUNC('DAY', PUBLISHED_DATETIME)::DATE AS DATE,
                COUNT(*) AS TOTAL_ARTICLES,
                SUM(CASE WHEN SENTIMENT_LABEL IN ('GREED', 'EXTREMELY GREED') THEN 1 ELSE 0 END) AS POSITIVE_ARTICLES,
                SUM(CASE WHEN SENTIMENT_LABEL = 'NEUTRAL' THEN 1 ELSE 0 END) AS NEUTRAL_ARTICLES,
                SUM(CASE WHEN SENTIMENT_LABEL IN ('FEAR', 'EXTREMELY FEAR') THEN 1 ELSE 0 END) AS NEGATIVE_ARTICLES,
                AVG(SENTIMENT_SCORE) AS AVG_SENTIMENT_SCORE,
                MIN(SENTIMENT_SCORE) AS MIN_SENTIMENT_SCORE,
                MAX(SENTIMENT_SCORE) AS MAX_SENTIMENT_SCORE,
                STDDEV(SENTIMENT_SCORE) AS STDDEV_SENTIMENT_SCORE,
                ((AVG(SENTIMENT_SCORE) + 1) / 2) * 100 AS FNG_INDEX_VALUE
            FROM BITCOIN_DATA.ANALYTICS.NEWS_SENTIMENT
            WHERE PUBLISHED_DATETIME >= DATEADD(DAY, -30, CURRENT_TIMESTAMP())
            GROUP BY DATE_TRUNC('DAY', PUBLISHED_DATETIME)::DATE
        ),
        with_previous AS (
            SELECT
                *,
                LAG(FNG_INDEX_VALUE) OVER (ORDER BY DATE) AS prev_fng_value
            FROM daily_stats
        )
        SELECT
            *,
            CASE
                WHEN prev_fng_value IS NOT NULL
                THEN FNG_INDEX_VALUE - prev_fng_value
                ELSE NULL
            END AS FNG_INDEX_CHANGE,
            CASE
                WHEN prev_fng_value IS NOT NULL AND prev_fng_value != 0
                THEN ((FNG_INDEX_VALUE - prev_fng_value) / prev_fng_value) * 100
                ELSE NULL
            END AS FNG_INDEX_CHANGE_PCT
        FROM with_previous
    ) AS source
    ON target.DATE = source.DATE
    WHEN MATCHED THEN UPDATE SET
        target.TOTAL_ARTICLES = source.TOTAL_ARTICLES,
        target.POSITIVE_ARTICLES = source.POSITIVE_ARTICLES,
        target.NEUTRAL_ARTICLES = source.NEUTRAL_ARTICLES,
        target.NEGATIVE_ARTICLES = source.NEGATIVE_ARTICLES,
        target.AVG_SENTIMENT_SCORE = source.AVG_SENTIMENT_SCORE,
        target.MIN_SENTIMENT_SCORE = source.MIN_SENTIMENT_SCORE,
        target.MAX_SENTIMENT_SCORE = source.MAX_SENTIMENT_SCORE,
        target.STDDEV_SENTIMENT_SCORE = source.STDDEV_SENTIMENT_SCORE,
        target.FNG_INDEX_VALUE = source.FNG_INDEX_VALUE,
        target.FNG_INDEX_LABEL = CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END,
        target.FNG_INDEX_CHANGE = source.FNG_INDEX_CHANGE,
        target.FNG_INDEX_CHANGE_PCT = source.FNG_INDEX_CHANGE_PCT,
        target.CALCULATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        DATE,
        TOTAL_ARTICLES,
        POSITIVE_ARTICLES,
        NEUTRAL_ARTICLES,
        NEGATIVE_ARTICLES,
        AVG_SENTIMENT_SCORE,
        MIN_SENTIMENT_SCORE,
        MAX_SENTIMENT_SCORE,
        STDDEV_SENTIMENT_SCORE,
        FNG_INDEX_VALUE,
        FNG_INDEX_LABEL,
        FNG_INDEX_CHANGE,
        FNG_INDEX_CHANGE_PCT
    ) VALUES (
        source.DATE,
        source.TOTAL_ARTICLES,
        source.POSITIVE_ARTICLES,
        source.NEUTRAL_ARTICLES,
        source.NEGATIVE_ARTICLES,
        source.AVG_SENTIMENT_SCORE,
        source.MIN_SENTIMENT_SCORE,
        source.MAX_SENTIMENT_SCORE,
        source.STDDEV_SENTIMENT_SCORE,
        source.FNG_INDEX_VALUE,
        CASE
            WHEN source.FNG_INDEX_VALUE <= 24 THEN 'EXTREME FEAR'
            WHEN source.FNG_INDEX_VALUE <= 44 THEN 'FEAR'
            WHEN source.FNG_INDEX_VALUE <= 55 THEN 'NEUTRAL'
            WHEN source.FNG_INDEX_VALUE <= 75 THEN 'GREED'
            ELSE 'EXTREME GREED'
        END,
        source.FNG_INDEX_CHANGE,
        source.FNG_INDEX_CHANGE_PCT
    );

    rows_calculated := SQLROWCOUNT;

    RETURN 'Calculated daily F&G index for ' || rows_calculated || ' days';
END;
$$;

-- =====================================================
-- TASK FOR AUTOMATED PROCESSING
-- =====================================================

-- Task to run sentiment analysis every 5 minutes
CREATE OR REPLACE TASK ANALYTICS.TASK_ANALYZE_NEWS_SENTIMENT
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('ANALYTICS.NEWS_STREAM')
AS
    CALL ANALYTICS.ANALYZE_NEWS_SENTIMENT();

-- Task to calculate hourly F&G index after sentiment analysis
CREATE OR REPLACE TASK ANALYTICS.TASK_CALCULATE_HOURLY_FNG
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 1 * * * UTC'  -- Daily at 1 AM UTC
AS
    CALL ANALYTICS.CALCULATE_HOURLY_FNG_INDEX();

-- Task to calculate daily F&G index once per day
CREATE OR REPLACE TASK ANALYTICS.TASK_CALCULATE_DAILY_FNG
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = 'USING CRON 0 1 * * * UTC'  -- Daily at 1 AM UTC
AS
    CALL ANALYTICS.CALCULATE_DAILY_FNG_INDEX();

-- =====================================================
-- ENABLE TASKS
-- =====================================================

ALTER TASK ANALYTICS.TASK_ANALYZE_NEWS_SENTIMENT RESUME;
ALTER TASK ANALYTICS.TASK_CALCULATE_HOURLY_FNG RESUME;
ALTER TASK ANALYTICS.TASK_CALCULATE_DAILY_FNG RESUME;

-- =====================================================
-- VIEWS FOR EASY QUERYING
-- =====================================================

-- View: Latest sentiment trends
CREATE OR REPLACE VIEW BITCOIN_DATA.ANALYTICS.VW_LATEST_NEWS_SENTIMENT AS
SELECT
    ns.ID,
    ns.PUBLISHED_DATETIME,
    ns.TITLE,
    ns.SOURCE,
    ns.SENTIMENT_SCORE,
    ns.SENTIMENT_LABEL,
    ns.TITLE_SENTIMENT_SCORE,
    n.URL,
    n.UPVOTES,
    n.DOWNVOTES
FROM BITCOIN_DATA.ANALYTICS.NEWS_SENTIMENT ns
JOIN COINDESK.NEWS n ON ns.ID = n.ID
ORDER BY ns.PUBLISHED_DATETIME DESC;

-- View: Current F&G Index (latest hourly and daily)
CREATE OR REPLACE VIEW BITCOIN_DATA.ANALYTICS.VW_CURRENT_FNG_INDEX AS
SELECT
    'HOURLY' AS TIMEFRAME,
    h.HOUR_START AS TIMESTAMP,
    h.FNG_INDEX_VALUE,
    h.FNG_INDEX_LABEL,
    h.TOTAL_ARTICLES,
    h.POSITIVE_ARTICLES,
    h.NEUTRAL_ARTICLES,
    h.NEGATIVE_ARTICLES,
    NULL AS CHANGE_VALUE,
    NULL AS CHANGE_PCT
FROM BITCOIN_DATA.ANALYTICS.FNG_NEWS_INDEX_HOURLY h
WHERE h.HOUR_START = (SELECT MAX(HOUR_START) FROM BITCOIN_DATA.ANALYTICS.FNG_NEWS_INDEX_HOURLY)

UNION ALL

SELECT
    'DAILY' AS TIMEFRAME,
    d.DATE::TIMESTAMP_TZ AS TIMESTAMP,
    d.FNG_INDEX_VALUE,
    d.FNG_INDEX_LABEL,
    d.TOTAL_ARTICLES,
    d.POSITIVE_ARTICLES,
    d.NEUTRAL_ARTICLES,
    d.NEGATIVE_ARTICLES,
    d.FNG_INDEX_CHANGE AS CHANGE_VALUE,
    d.FNG_INDEX_CHANGE_PCT AS CHANGE_PCT
FROM BITCOIN_DATA.ANALYTICS.FNG_NEWS_INDEX_DAILY d
WHERE d.DATE = (SELECT MAX(DATE) FROM BITCOIN_DATA.ANALYTICS.FNG_NEWS_INDEX_DAILY);


-- Grant necessary permissions for ANALYTICS schema objects
USE ROLE ACCOUNTADMIN;

-- Grant EXECUTE TASK privilege to SYSADMIN role
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;

-- Grant EXECUTE MANAGED TASK privilege (for serverless tasks)
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE SYSADMIN;

-- Grant usage on the warehouse
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE SYSADMIN;

-- Grant privileges on ANALYTICS schema
GRANT USAGE ON SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL PROCEDURES IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL STREAMS IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL TASKS IN SCHEMA ANALYTICS TO ROLE SYSADMIN;

-- Grant privileges on COINDESK schema (needed for reading NEWS table)
GRANT USAGE ON SCHEMA COINDESK TO ROLE SYSADMIN;
GRANT SELECT ON ALL TABLES IN SCHEMA COINDESK TO ROLE SYSADMIN;

-- Grant privileges on NEWHEDGE schema (for completeness)
GRANT USAGE ON SCHEMA NEWHEDGE TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA NEWHEDGE TO ROLE SYSADMIN;

-- FUTURE GRANTS (for new objects)
-- Ensure future tables/views/procedures automatically get privileges
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE VIEWS IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE PROCEDURES IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE STREAMS IN SCHEMA ANALYTICS TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE TASKS IN SCHEMA ANALYTICS TO ROLE SYSADMIN;

GRANT SELECT ON FUTURE TABLES IN SCHEMA COINDESK TO ROLE SYSADMIN;
GRANT ALL PRIVILEGES ON FUTURE TABLES IN SCHEMA NEWHEDGE TO ROLE SYSADMIN;

USE ROLE SYSADMIN;
