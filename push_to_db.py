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


def main() -> None:

    # db connection
    db_client = MongoClient(**mongo_details)
    db_client.list_database_names()

    # pull and push data
    subreddit = 'wallstreetbets'
    end = datetime.datetime.now()
    start = datetime.datetime.now() - datetime.timedelta(days=365*3)

    for batch in get_reddit_data(
            subreddit=subreddit,
            start=start,
            end=end):

        # unpack batch to a list of dicts
        # where each dict is either: submission or comment
        # should this be done inside get_reddit_data / get_submission_data?
        flatten_list = lambda list_: [i for j in list_ for i in
                                      flatten_list(j)] if type(list_) is list else [list_]
        batch_flattened = flatten_list(batch)
        # [print(i) for i in batch_flattened]

        if batch_flattened:

            # inset into db
            # TODO: solve _id/id issue and then check if doc exists
            # TODO: make each doc of similar structure, now subs and comms differ.
            db_client.reddit.data.insert_many(batch_flattened)

            ts = batch_flattened[0]['created_utc']
            print(f'Last approx TS of submission {ts}')
