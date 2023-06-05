import psycopg2
from private import db_details
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def create_database_and_tables():

    subreddits = ['cryptocurrency']

    con = psycopg2.connect(**db_details)
    # this or CREATE DATABASE cannot run inside a transaction block
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with con.cursor() as cur:
        # create reddit db
        # will store all tables inside this db
        cur.execute(
            'CREATE DATABASE "reddit"'
        )

    # what the hell, how do you create db and then
    # create table inside that db? The only way to
    # make a new connection? WTF?
    con = psycopg2.connect(**db_details, dbname='reddit')
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    with con.cursor() as cur:
        # create main tables
        # main table contains all submissions/comments
        cols = """
                _id VARCHAR PRIMARY KEY,
                created_utc INT,
                title VARCHAR,
                body VARCHAR,
                selftext VARCHAR
        """
        for subreddit in subreddits:
            cur.execute(
                f'DROP TABLE IF EXISTS {subreddit};'
                f'CREATE TABLE IF NOT EXISTS {subreddit} ({cols})'
            )

        # create topic tables
        # topic table contains selected entries
        # from corresponding subreddit main table.
        # Entries are selected if it contains a topic.
        # These tables are optimised for speed.
        # These will be queried by external APIs.

        # IMPORTANT!

        # instead of unix timestamp lets create postgresql
        # timestamp col so that we do not need to do this
        # conversion on every query (speed tests show this
        # cuts time from 450 to 280 ms for 10 mln rows)

        con = psycopg2.connect(**db_details, dbname='reddit')
        con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

        with con.cursor() as cur:

            # Let's set the id to unique identifier so that
            # when updating topic values we do not add new
            # records everytime we do the op over table with
            # already present records.
            cols = """
                    _id VARCHAR PRIMARY KEY,
                    created_utc INT,
                    is_comment BOOLEAN,
                    topic VARCHAR
            """
            for subreddit in [sub + '_' for sub in subreddits]:
                cur.execute(
                    f'DROP TABLE IF EXISTS {subreddit};'
                    f'CREATE TABLE IF NOT EXISTS {subreddit} ({cols})'
                )


def create_cron_jobs():

    cron_jobs = [
        {
            'name': 'hourly_data',
            'aggregation_helper': 'hour',
            'lookback_time_in_hours': 24,
            'cron_scedule': '5 * * * *'  # every hour at 1 minutes (at 1 minute, mentions are updated)
        },
        {
            'name': 'daily_data',
            'aggregation_helper': 'day',
            'lookback_time_in_hours': 24 * 24,
            'cron_scedule': '5 1 * * *'  # every day at 1 hour 5 min
        },
    ]

    # Inside the reddit database:
    # 1. Create functions to be run by cron inside the reddit database
    con = psycopg2.connect(**db_details, dbname='reddit')
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with con.cursor() as cur:

        for job in cron_jobs:

            query = f"""
                DROP TABLE IF EXISTS {job['name']};
                CREATE TABLE {job['name']} AS
                WITH last_period AS (
                    SELECT (
                        DATE_TRUNC('{job['aggregation_helper']}', CURRENT_TIMESTAMP AT TIME ZONE 'UTC') 
                        - INTERVAL '0 {job['aggregation_helper']}'
                    )::timestamp AS last_timestamp
                ),
                data_ AS (
                    SELECT * FROM cryptocurrency_
                    WHERE
                        created_utc <= EXTRACT(EPOCH FROM (SELECT last_timestamp FROM last_period))
                        AND created_utc > EXTRACT(EPOCH FROM (SELECT last_timestamp FROM last_period)) 
                        - ({job['lookback_time_in_hours']} * 60 * 60)
                )
                SELECT 
                    topic,
                    DATE_TRUNC('{job['aggregation_helper']}', TIMESTAMP 'epoch' + created_utc * INTERVAL '1 second') AS gran,
                    COUNT(*) AS count
                FROM data_
                GROUP BY gran, topic
                ORDER BY topic, gran DESC;
            """

            cur.execute(
                f"""
                CREATE OR REPLACE FUNCTION {job['name']}()
                RETURNS VOID AS
                $$
                BEGIN
                    {query}
                END;
                $$
                LANGUAGE plpgsql;
                """
            )

    # Inside the postgres database:
    # 2. Activate cron extensions, remove all cron jobs (if any)
    # 3. Create cron jobs
    con = psycopg2.connect(**db_details, dbname='postgres')
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with con.cursor() as cur:

        cur.execute(
            """
            CREATE EXTENSION IF NOT EXISTS pg_cron;
            TRUNCATE TABLE cron.job;
            TRUNCATE TABLE cron.job_run_details;
            """
        )

        for job in cron_jobs:
            cur.execute(
                f"""
                SELECT cron.schedule_in_database(
                    '{job['name']}',
                    '{job['cron_scedule']}',
                    'SELECT {job['name']}()',
                    'reddit'
                );
                """
            )


if __name__ == '__main__':
    # Be careful, running this will erase the db tables with data.
    # Commenting it out to make sure we don't run in accidentally
    # create_database_and_tables()

    # create cron jobs
    create_cron_jobs()
