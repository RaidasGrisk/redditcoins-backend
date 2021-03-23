'''
TODO:
1. Work on indexing the db
2. Work on limiting the query results
3. Think of better db structure to lower compute cost

'''

from private import mongo_details
import motor.motor_asyncio
import pandas as pd
import time
import asyncio
from typing import Union


def date_string_to_timestamp(s: str) -> int:
    # using pandas instead of datetime to be able to infer str format
    tuple = pd.to_datetime(s, infer_datetime_format=True).timetuple()
    return int(time.mktime(tuple))


async def get_timeseries_df(
        subreddit: str,
        ticker: Union[str, None],
        start: str,
        end: str,
        ups: int,
        submissions: bool,
        comments: bool,
        granularity: str
) -> pd.Series:

    # db connection
    db_client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_details)

    # cast str dates to ts as in db
    start = date_string_to_timestamp(start)
    end = date_string_to_timestamp(end)

    # agg. very likely can be improved
    # serves just as temp solution
    cur = db_client.reddit[subreddit].aggregate([
        {
            '$match': {'$and': [
                {'metadata.topics.direct': {'$in': [ticker]}} if ticker else {},
                {'data.created_utc': {'$gte': start}},
                {'data.created_utc': {'$lte': end}},
                {'data.ups': {'$gte': ups}},
                # TODO: think of a way to properly combine these two opts
                {'data.title': {'$exists': True if submissions else False}},
                {'data.parent_id': {'$exists': True if comments else False}},
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

    # convert to DataFrame to be able to
    # simply group by specified granularity
    df = pd.DataFrame([i async for i in cur]).set_index('_id')
    # granularity options: 'Y', 'M', 'W', 'D', 'H', '5H', 'min', '30min', etc
    df_ = df.groupby(pd.Grouper(freq=granularity)).sum()

    # modify index attributes
    # by default index values are datetime64[ns]
    # upon conversion to json (pd.to_json)
    # these values are converted to timestamps: int
    # to prevent this, lets cast these values to str
    df_.index = df_.index.astype(str)

    # when aggregating the index key must be set to _id
    # after aggregation rename it to time for later use
    df_.index = df_.index.rename('time')

    return df_


def test() -> None:

    df = asyncio.run(get_timeseries_df(
        subreddit='wallstreetbets',
        ticker='GME',
        start='2017-03-01',
        end='2021-03-20',
        ups=10,
        submissions=False,
        comments=True,
        granularity='1H'
    ))

    print(df.reset_index().to_dict(orient='records'))


'''
Other filter options to try out to improve speed

[
    {  # match specific fields
        '$match': {'$and': [
            {'data.ups': {'$gte': 10}},
            {'data.title': {'$exists': True}},
            {'metadata.topics.direct': {'$in': ['crypto']}}
        ]}
    },
    {'$project': {
        # double timestamp to date:
        # https://medium.com/idomongodb/mongodb-unix-timestamp-to-isodate-67741ab32078
       'date': {'date': {'$toDate': {'$multiply': ['$data.created_utc', 1000]}}}
    }},
    {  # group by
        '$group': {
            '_id': {
                'year': {'$year': '$date.date'},
                'month': {'$month': '$date.date'},
                'day': {'$dayOfMonth': '$date.date'},
                'hour': {'$hour': '$date.date'},
                'minute': {'$minute': '$date.date'}
            },
            'total': {'$sum': 1}
        }
    }
]


[
    {  # match specific fields
        '$match': {'$and': [
            {'data.ups': {'$gte': 10}},
            {'data.title': {'$exists': True}},
            {'metadata.topics.direct': {'$in': ['crypto']}}
        ]}
    },
    {  # group by
        '$group': {
            '_id': {
                "$subtract": [
                    '$data.created_utc',
                    {'$mod': ['$data.created_utc', 1000 * 60 * 60]}
                ]
            },
            'total': {'$sum': 1}
        }
    }
]
'''