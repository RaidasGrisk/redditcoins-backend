from private import mongo_details
from pymongo import MongoClient
import pandas as pd
import time


def date_string_to_timestamp(s: str) -> int:
    # using pandas instead of datetime to be able to infer str format
    tuple = pd.to_datetime(s, infer_datetime_format=True).timetuple()
    return int(time.mktime(tuple))


def get_timeseries_df(
        start: str,
        end: str,
        ticker: str,
        ups: int,
        submissions: bool,
        comments: bool,
        granularity: str
) -> pd.Series:

    # db connection
    db_client = MongoClient(**mongo_details)

    # cast str dates to ts as in db
    start = date_string_to_timestamp(start)
    end = date_string_to_timestamp(end)

    with db_client:

        # count by date
        cur = db_client.reddit.data.aggregate([
            {  # match specific fields
                '$match': {'$and': [
                    {'metadata.topics.direct': {'$in': [ticker]}},
                    {'data.created_utc': {'$gte': start}},
                    {'data.created_utc': {'$lte': end}},
                    {'data.ups': {'$gte': ups}},
                    # TODO: think of a way to properly combine these two opts
                    {'data.title': {'$exists': True if submissions else False}},
                    {'data.parent_id': {'$exists': True if comments else False}},
                ]}
            },
            {  # group by
                '$group': {
                    # double timestamp to date:
                    # https://medium.com/idomongodb/mongodb-unix-timestamp-to-isodate-67741ab32078
                    '_id': {'$toDate': {'$multiply': ['$data.created_utc', 1000]}},
                    'total': {'$sum': 1}
                }
            }
        ])

    # convert to DataFrame to be able to
    # simply group by specified granularity
    df = pd.DataFrame(cur).set_index('_id')
    # granularity options: 'Y', 'M', 'W', 'D', 'H', '5H', 'min', '30min', etc
    df_ = df.groupby(pd.Grouper(freq=granularity)).sum()
    return df_['total']


def test():

    ts_df = get_timeseries_df(
        start='2021-03-01',
        end='2021-03-20',
        ticker='GME',
        ups=10,
        submissions=False,
        comments=True,
        granularity='1H'
    )
    
    # convert to str, else to_json will convert it to ts
    ts_df.index = ts_df.index.astype(str)
    ts_df.to_json()


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