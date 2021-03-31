"""
https://github.com/MagicStack/asyncpg
https://docs.timescale.com/latest/main
https://www.pgadmin.org/docs/pgadmin4/latest/container_deployment.html

https://mccarthysean.dev/timescale-dash-flask-part-1

"""

# to be able to launch this from terminal
import sys, os
sys.path.append(os.getcwd())
from private import mongo_details
import motor.motor_asyncio
import asyncio
import psycopg2
from psycopg2.extras import execute_values
# from pymongo import MongoClient


async def make_refactored_data_collection() -> None:

    # db connection
    db_client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_details)
    # db_client = MongoClient(**mongo_details)

    filter = {'metadata.topics.direct': {'$exists': True}}

    total_docs = await db_client.reddit['wallstreetbets'].count_documents(filter)
    cur = db_client.reddit['wallstreetbets'].find(filter)

    total_updates = 0
    docs = []
    async for doc in cur:

        topics = doc.get('metadata', {}).get('topics', {}).get('direct', [])
        for topic in topics:
            docs.append({
                'id': doc['_id'],
                'created_utc': doc['data']['created_utc'],
                'ups': doc['data']['ups'],
                'topics': topic
            })

        if len(docs) > 1000:
            _ = await db_client.reddit['wallstreetbets_'].insert_many(docs)
            docs = []
            total_updates += 1000

        if total_updates % 1000 == 0:
            print(f'updated {total_updates} out of {total_docs}')


async def migrate_data_to_timescaledb_small_set() -> None:

    # db connection
    db_client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_details)
    # db_client = MongoClient(**mongo_details)
    conn = psycopg2.connect(
        user='admin',
        password='pass',
        host='0.0.0.0',
        database='test'
    )

    filter = {'metadata.topics.direct': {'$exists': True}}

    total_docs = await db_client.reddit['wallstreetbets'].count_documents(filter)
    cur = db_client.reddit['wallstreetbets'].find(filter)

    total_updates = 0
    docs = []
    async for doc in cur:

        topics = doc.get('metadata', {}).get('topics', {}).get('direct', [])
        for topic in topics:
            docs.append({
                'id': doc['_id'],
                'created_utc': doc['data']['created_utc'],
                'ups': doc['data']['ups'],
                'topics': topic
            })

        if len(docs) > 1000:
            columns = ['id', 'created_utc', 'ups', 'topics']
            values = [[row.get(key) for key in columns] for row in docs]
            query = f"""
                INSERT INTO wallstreetbets_ ({', '.join(columns)}) VALUES %s
                """

            with conn:
                execute_values(conn.cursor(), query, values)
                conn.commit()

            docs = []
            total_updates += 1000

        if total_updates % 1000 == 0:
            print(f'updated {total_updates} out of {total_docs}')


async def migrate_data_to_timescaledb_all_set() -> None:

    # db connection
    db_client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_details)
    # db_client = MongoClient(**mongo_details)
    conn = psycopg2.connect(
        user='admin',
        password='pass',
        host='0.0.0.0',
        database='test'
    )

    filter = {}

    total_docs = await db_client.reddit['wallstreetbets'].count_documents(filter)
    cur = db_client.reddit['wallstreetbets'].find(filter)

    total_updates = 0
    docs = []
    async for doc in cur:

        docs.append({
            'id': doc['_id'],
            'created_utc': doc['data']['created_utc'],
            'ups': doc['data']['ups'],
        })

        if len(docs) > 1000:
            columns = ['id', 'created_utc', 'ups']
            values = [[row.get(key) for key in columns] for row in docs]
            query = f"""
                INSERT INTO wallstreetbets_full ({', '.join(columns)}) VALUES %s
                """

            with conn:
                execute_values(conn.cursor(), query, values)
                conn.commit()

            docs = []
            total_updates += 1000

        if total_updates % 1000 == 0:
            print(f'updated {total_updates} out of {total_docs}')

asyncio.run(make_refactored_data_collection())
asyncio.run(migrate_data_to_timescaledb_small_set())
asyncio.run(migrate_data_to_timescaledb_all_set())

# --------- #
def speed_experiment_1(
    subreddit: str,
    ticker: str,
    start: str,
    end: str,
    ups: int,
):

    # import inside fn, because timeit setup
    # is not working for some reason..?
    from pymongo import MongoClient
    from private import mongo_details
    from update_topics import date_string_to_timestamp

    db_client = MongoClient(**mongo_details)
    cur = db_client.reddit[subreddit].aggregate([
        {
            '$match': {'$and': [
                {'metadata.topics.direct': {'$in': [ticker]}} if ticker else {},
                {'data.created_utc': {'$gte': date_string_to_timestamp(start)}},
                {'data.created_utc': {'$lte': date_string_to_timestamp(end)}},
                {'data.ups': {'$gte': ups}},
            ]}
        },
        {
            '$group': {
                # double timestamp to date:
                # https://medium.com/idomongodb/mongodb-unix-timestamp-to-isodate-67741ab32078
                '_id': {'$toDate': {'$multiply': ['$data.created_utc', 1000]}},
                'volume': {'$sum': 1}
            }
        }
    ])

    print(len([i for i in cur]))
    return None


def speed_experiment_2(
    subreddit: str,
    ticker: str,
    start: str,
    end: str,
    ups: int,
):

    # import inside fn, because timeit setup
    # is not working for some reason..?
    from pymongo import MongoClient
    from private import mongo_details
    from other_ops.update_topics import date_string_to_timestamp

    db_client = MongoClient(**mongo_details)
    cur = db_client.reddit[subreddit].aggregate([
        {
            '$match': {'$and': [
                {'topics': {'$eq': ticker}} if ticker else {},
                {'created_utc': {'$gte': date_string_to_timestamp(start)}},
                {'created_utc': {'$lte': date_string_to_timestamp(end)}},
                {'ups': {'$gte': ups}}
            ]}
        },
        {
            '$group': {
                # double timestamp to date:
                # https://medium.com/idomongodb/mongodb-unix-timestamp-to-isodate-67741ab32078
                '_id': {'$toDate': {'$multiply': ['$created_utc', 1000]}},
                'volume': {'$sum': 1}
            }
        }
    ])

    print(len([i for i in cur]))
    return None


def speed_experiment_3(
    subreddit: str,
    ticker: str,
    start: str,
    end: str,
    ups: int,
):

    # import inside fn, because timeit setup
    # is not working for some reason..?
    import psycopg2
    from other_ops.update_topics import date_string_to_timestamp

    conn = psycopg2.connect(
        user='admin',
        password='pass',
        host='0.0.0.0',
        database='test'
    )

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT time_bucket('1 hours', to_timestamp(created_utc)) as granularity, COUNT(*)
            FROM {subreddit}
            WHERE topics='{ticker}'
            AND created_utc >= {date_string_to_timestamp(start)}
            AND created_utc < {date_string_to_timestamp(end)}
            AND ups > {ups}
            GROUP BY granularity
            ORDER BY granularity DESC;

        """)
        result = cur.fetchall()

    print(len([i for i in result]))
    return None

# ----------- #
import timeit

setup = """
import sys, os
sys.path.append(os.getcwd())
from __main__ import speed_experiment_1
from __main__ import speed_experiment_2
from __main__ import speed_experiment_3
"""

ticker = 'GME'
start = '2017-01-01'
end = '2022-01-01'
ups = 0

# ~20 secs
print(timeit.timeit(
    stmt=f"""speed_experiment_1(
                subreddit='wallstreetbets',
                ticker='{ticker}',
                start='{start}',
                end='{end}',
                ups={ups},
            )""",
    number=1,
    setup=setup
))

# ~1.4 secs / 0.8 secs (with indexes)
print(timeit.timeit(
    stmt=f"""speed_experiment_2(
                subreddit='wallstreetbets_',
                ticker='{ticker}',
                start='{start}',
                end='{end}',
                ups={ups},
            )""",
    number=1,
    setup=setup
))

# 0.3 secs (no indices)
print(timeit.timeit(
    stmt=f"""speed_experiment_3(
                subreddit='wallstreetbets_',
                ticker='{ticker}',
                start='{start}',
                end='{end}',
                ups={ups},
            )""",
    number=1,
    setup=setup
))
