from private import mongo_details
from pymongo import MongoClient, UpdateOne
from get_reddit_data import get_reddit_data
import datetime
from typing import List, Callable


# The following decorators takes in a list of dicts and
# modify / filter out the list. It would be nicer if
# these would take in single dict. Not sure how to do this
# as some decorators are modifying dicts (more in terms of map)
# and others filtering. In other words, some modify list len
# some do not. So it would be simpler, but tricky to implement.


def filter_removed_or_deleted(func) -> Callable:

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


def add_proper_ids(func) -> Callable:

    def wrapper(batch: List[dict]) -> List[dict]:
        for item in batch:
            if item.get('id', None):
                item['_id'] = item.pop('id')
        return func(batch)

    return wrapper


def modify_submission_key_names(func) -> Callable:
    # up to this point, submission and comment structure is different
    # lets make it more similar by making sure that
    # subs selftext -> body (as it is in comments)

    def wrapper(batch: List[dict]) -> List[dict]:
        for item in batch:
            if 'selftext' in item:
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


def split_date_interval_to_chunks(
        start: datetime.datetime,
        end: datetime.datetime,
        delta: datetime.timedelta
) -> List[List[datetime.datetime]]:

    # takes in start and end,
    # splits the period by delta into
    # [[start_, end_], [start_, end_], ...]

    last_start, last_end = start, start
    time_chunks = []
    while last_end < end:
        last_end = last_start + delta
        time_chunks.append([last_start, last_end])
        last_start += delta

    return time_chunks


def main(
        subreddit='wallstreetbets',
        start=datetime.datetime.now() - datetime.timedelta(days=22),
        end=datetime.datetime.now(),
        delta=datetime.timedelta(hours=4)
) -> None:

    # db connection
    db_client = MongoClient(**mongo_details)

    # make time intervals

    # querying PushshiftAPI for long date intervals results in weird outputs
    # so the following solution to split each interval to fixed time chunks

    # Warning:
    # Make sure to set delta not too large as
    # data from ids in the range of single delta
    # will be fetched asynchronously.
    # so if delta = 1 day and 200 subs are made in one day,
    # it will make approx 200 * 1 sub requests at once
    # (not including additional requests to get comments).

    intervals = split_date_interval_to_chunks(
        start=start,
        end=end,
        delta=delta
    )

    for start_, end_ in intervals:

        batch = get_reddit_data(subreddit=subreddit, start=start_, end=end_)

        # now batch = [[{sub}, {comm}, {comm} ..], [{sub}, {comm}, {comm} ...], ...]
        # lets make it flat and insert whole batch into db
        flatten = lambda list_: [i for j in list_ for i in
                                 flatten(j)] if type(list_) is list else [list_]
        batch = flatten(batch)

        # filter, edit, etc. before push
        batch = before_db_push(batch)

        # skip if get_reddit_data returns an empty list
        # else pymongo.errors.InvalidOperation: No operations to execute
        # deal with missing data later
        if batch:
            with db_client:
                # inset into db
                # can not just do insert_many as this will not let
                # update already existing docs, so the following solution
                result = db_client.reddit.data.bulk_write([
                    UpdateOne(
                        {'_id': item['_id']},
                        {'$set': {'data': item}},
                        upsert=True
                    ) for item in batch
                ])

        if result:
            # pop to limit printing space and exclude data inserted
            result.bulk_api_result.pop('upserted', None)
            print(start_.date(), end_.date(), result.bulk_api_result)
        else:
            print(start_.date(), end_.date(), 'Failed to get data')


if __name__ == '__main__':

    # parser = argparse.ArgumentParser()
    # parser.add_argument('subreddit', type=str, default='wallstreetbets')
    # parser.add_argument('start', type=str, default='2020-02-01')
    # parser.add_argument('end', type=str, default='2020-03-01')
    # parser.add_argument('delta', type=str, default='12 hours')
    # args = parser.parse_args()

    main()
