from private import mongo_details
from pymongo import MongoClient
import pandas as pd


def main() -> None:

    # db connection
    db_client = MongoClient(**mongo_details)

    cur = db_client.reddit.data.aggregate([
        {

            # match specific topics
            '$match': {
                '$or': [
                    {
                        'metadata.topics.direct': {
                            '$in': ['GME']
                        }
                    },
                    {
                        'metadata.topics.indirect': {
                            '$in': ['GME']
                        }
                    }
                ]
            }
        },

        # group by
        {
            '$group': {
                # double timestamp to date:
                # https://medium.com/idomongodb/mongodb-unix-timestamp-to-isodate-67741ab32078
                '_id': {
                    '$toDate': {'$multiply': ['$data.created_utc', 1000]}
                },
                'total': {
                    '$sum': 1
                }
            }
        }
    ])

    df = pd.DataFrame(cur).set_index('_id')
    df.groupby(pd.Grouper(freq='H')).sum().plot()