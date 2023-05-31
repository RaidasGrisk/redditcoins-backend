import psycopg2
from private import db_details
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

if __name__ == '__main__':

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
        subreddits = ['cryptocurrency']
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

