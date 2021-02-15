"""
TODO: create a script to init the dbs and collections after db init

FYI:
wallstreetbets gives ~ 90KB of data every h (not filtered)
this amounts to 2MB every 24h, 800MB a year
"""

from private import mongo_details
from pymongo import MongoClient, UpdateOne
from get_reddit_data import get_reddit_data
import datetime
from typing import List


def filter_removed_or_deleted(func):

    def wrapper(batch: List[dict]) -> List[dict]:

        def is_not_removed_or_deleted(item: dict) -> bool:

            # submissions have title and selftext; comments have body
            # as we do not differentiate between subs and comms
            # have to check both cases

            # keys correspond to keys in an item
            # values are lists of strs that we want to skip if
            # at least one of it is equal to val of item
            skip_params = {
                'body': ['[removed]', '[deleted]'],

                # if we modify keys (modify_submission_key_names)
                # before running this then we can omit the following line
                'selftext': ['[removed]', '[deleted]']
            }

            for key, vals in skip_params.items():
                for val in vals:
                    if val == item.get(key, None):
                        return False

            return True

        batch_filtered = list(filter(is_not_removed_or_deleted, batch))
        return func(batch_filtered)

    return wrapper


def add_proper_ids(func):

    def wrapper(batch: List[dict]) -> List[dict]:
        for item in batch:
            if item.get('id', None):
                item['_id'] = item.pop('id')
        return func(batch)

    return wrapper


def modify_submission_key_names(func):
    # up to this point, submission and comment structure is different
    # lets make it more similar by making sure that
    # subs selftext -> body (as it is in comments)

    def wrapper(batch: List[dict]) -> List[dict]:
        for item in batch:
            if item.get('selftext', None):
                item['body'] = item.pop('selftext')
        return func(batch)

    return wrapper


# helper function to be decorated with functions
# that filter, modify and etc. before pushing to db
@filter_removed_or_deleted
@add_proper_ids
@modify_submission_key_names
def before_db_push(batch: List[dict]) -> List[dict]:
    return batch


def main() -> None:

    # db connection
    db_client = MongoClient(**mongo_details)
    db_client.list_database_names()

    # pull and push data
    subreddit = 'wallstreetbets'
    end = datetime.datetime.now()
    start = datetime.datetime.now() - datetime.timedelta(days=7)
    delta = datetime.timedelta(hours=12)

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
        batch = flatten_list(batch)

        # filter, edit, etc. before push
        batch = before_db_push(batch)

        # inset into db
        # can not just do insert_many as this will not let
        # update already existing docs, so the following solution
        result = db_client.reddit.data.bulk_write([
            UpdateOne({'_id': item['_id']}, {'$set': item}, upsert=True) for item in batch
        ])

        # del to limit printing space and exclude data inserted
        del result.bulk_api_result['upserted']
        print(result.bulk_api_result)
