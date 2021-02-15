"""
TODO: create a script to init the dbs and collections after db init

FYI:
wallstreetbets gives ~ 90KB of data every h (not filtered)
this amounts to 2MB every 24h, 800MB a year
"""

from private import mongo_details
from pymongo import MongoClient
from get_reddit_data import get_reddit_data
import datetime
from typing import List


# TODO: make each doc of similar structure, now subs and comms differ.
#       for example make sure that:
#       subs have title and body
#       comms have just body

def filter_before_db_push(batch_flattened: List[dict]) -> List[dict]:

    def is_not_removed_or_deleted(item: dict) -> bool:
        # keys correspond with keys in item
        # values are lists of strs that we want to skip if
        # at least one of it is equal to val of item

        # submissions have title and selftext; comments have body
        # as we do not differentiate between subs and comms
        # have to check both cases
        skip_params = {
            'body': ['[removed]', '[deleted]'],
            'selftext': ['[removed]', '[deleted]']
        }

        for key, vals in skip_params.items():
            for val in vals:
                if val == item.get(key, None):
                    return False

        return True

    batch_filtered = list(filter(is_not_removed_or_deleted, batch_flattened))
    return batch_filtered


def main() -> None:

    # db connection
    db_client = MongoClient(**mongo_details)
    db_client.list_database_names()

    # pull and push data
    subreddit = 'wallstreetbets'
    end = datetime.datetime.now()
    start = datetime.datetime.now() - datetime.timedelta(days=2)
    delta = datetime.timedelta(hours=6)

    for batch in get_reddit_data(
            subreddit=subreddit,
            start=start,
            end=end,
            delta=delta
    ):

        # now batch = [[{sub}, {comm}, {comm} ..], [{sub}, {comm}, {comm} ...], ...]
        # lets make it flat and insert whole batch into db
        flatten_list = lambda list_: [i for j in list_ for i in
                                      flatten_list(j)] if type(list_) is list else [list_]
        batch_flattened = flatten_list(batch)

        if batch_flattened:

            # inset into db
            # TODO: solve _id/id issue and then check if doc exists
            db_client.reddit.data.insert_many(
                filter_before_db_push(batch_flattened)
            )

            print('Approx ts of last reddit item inserted into db:')
            ts = batch_flattened[0]['created_utc']
            print(f'{ts}')
