import psycopg2
from psycopg2.extras import execute_values
from get_reddit_data import RedditData
import datetime
from typing import List, Callable
import argparse
from private import db_details


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


def push_data_to_db(subreddit: str, data: List[List[dict]], conn) -> None:

    # data = [[{sub}, {comm}, {comm} ..], [{sub}, {comm}, {comm} ...], ...]
    # lets make it flat and insert whole batch into db
    flatten = lambda list_: [i for j in list_ for i in
                             flatten(j)] if type(list_) is list else [list_]
    data = flatten(data)

    # filter, edit, etc. before push
    data = before_db_push(data)

    # skip if after post-processing no items are left
    # or if reddit apis returned no data
    # else pymongo.errors.InvalidOperation: No operations to execute
    # deal with missing data later
    if data:
        # upsert https://schinckel.net/2019/12/13/asyncpg-and-upserting-bulk-data/
        # https://stackoverflow.com/questions/54946697/psycopg2-inserting-list-of-dictionaries-into-posgresql-database-too-many-exec
        # refactor dict to a list of values
        columns = ['_id', 'created_utc', 'ups', 'num_comments', 'title', 'body']
        values = [[row.get(key) for key in columns] for row in data]
        query = f"""
            INSERT INTO {subreddit} ({', '.join(columns)}) VALUES %s
            ON CONFLICT (_id)
            DO UPDATE SET ({', '.join(columns[1:])})
                = ({", ".join(['EXCLUDED.' + col for col in columns[1:]])}) 
            """
        with conn:
            execute_values(conn.cursor(), query, values)
            conn.commit()


def get_historical_data(
        subreddit='wallstreetbets',
        start=datetime.datetime.now() - datetime.timedelta(days=22),
        end=datetime.datetime.now(),
        delta=datetime.timedelta(hours=12)
) -> None:

    # db connection
    conn = psycopg2.connect(**db_details, dbname='reddit')
    reddit = RedditData()

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
    # ok this has been updated to 5000 concurrent requests.
    # lets keep it this way, so we do not hammer pushshift.

    intervals = split_date_interval_to_chunks(
        start=start,
        end=end,
        delta=delta
    )

    for start_, end_ in intervals:

        batch = reddit.get_data(subreddit=subreddit, start=start_, end=end_)
        push_data_to_db(subreddit, batch, conn)
        print(start_.date(), end_.date(), len(batch))


def get_new_data(subreddit='wallstreetbets', limit=100):

    # db connection
    conn = psycopg2.connect(**db_details, dbname='reddit')
    reddit = RedditData()

    batch = reddit.get_data_new(subreddit=subreddit, limit=limit)
    push_data_to_db(subreddit, batch, conn)
    print(
        datetime.datetime.utcfromtimestamp(batch[0][0]['created_utc']).strftime('%Y-%m-%d'),
        datetime.datetime.utcfromtimestamp(batch[-1][0]['created_utc']).strftime('%Y-%m-%d'),
        len(batch)
    )


# python reddit_to_db.py --start 2020-03-17 --end 2020-03-18 --delta 12
# python reddit_to_db.py --subreddit satoshistreetbets --limit 100
if __name__ == '__main__':

    # set default values
    start = datetime.datetime.now() - datetime.timedelta(hours=12)
    end = datetime.datetime.now()

    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('--subreddit', type=str, default='wallstreetbets')
    parser.add_argument('--start', type=str, default=start.strftime('%Y-%m-%d'))
    parser.add_argument('--end', type=str, default=end.strftime('%Y-%m-%d'))
    parser.add_argument('--delta', type=int, default=12)
    parser.add_argument('--limit', type=int, default=None)
    args = parser.parse_args()

    # deal with args format
    args_dict = vars(args)
    args_dict['start'] = datetime.datetime.strptime(args_dict['start'], '%Y-%m-%d')
    args_dict['end'] = datetime.datetime.strptime(args_dict['end'], '%Y-%m-%d')
    args_dict['delta'] = datetime.timedelta(hours=args_dict['delta'])

    # run the thing
    # if limit is provided, fetch new data only
    # otherwise fetch historical data
    if args_dict['limit']:
        get_new_data(args_dict['subreddit'], args_dict['limit'])
    else:
        args_dict.pop('limit', None)
        get_historical_data(**args_dict)
