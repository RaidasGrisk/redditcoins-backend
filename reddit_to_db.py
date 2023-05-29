"""
Okay, so instead of pulling the data from reddit every x minutes
we can actually capture the stream of reddit data and push to db.
This way we will capture more comments and the code base is actually simpler.

So this file replaces both get_reddit_data.py and reddit_to_db.py

To run this just do the following (it will run forever):
python3 reddit_to_db_2.py --subreddit cryptocurrency

TODO: https://www.reddit.com/r/redditdev/comments/ohgtid/simultaneously_streaming_comments_submissions/
TODO: https://stackoverflow.com/questions/55299564/join-multiple-async-generators-in-python

TODO: set up new db
TODO: setup VM with data capture and push to new DB
TODO: migrate old data to new db
TODO: create db jobs (daily / hourly / web_data)
TODO: create API
TODO: vercel website

"""

import asyncpraw
import asyncio
import psycopg
from typing import Union
import time
from private import reddit_details, db_details
import argparse


def filter_and_fix_comment(comment) -> Union[dict, None]:
    # first lets filter out admin comments
    # maybe lets not do this, this is fine
    # admin_ids = ['haa5lmp']

    # for some reason the comment returned by the api
    # has an attr num_comments that gives the number
    # of comments in the parent comment or submission
    # so lets set it to None for now.
    comment['num_comments'] = None

    return comment


def parse_comment(comment: asyncpraw.reddit.Comment) -> dict:
    attrs = [
        'id',
        'created_utc',
        'ups',
        'num_comments',
        'title',
        'body',
    ]
    return {key: getattr(comment, key, None) for key in attrs}


async def comment_stream(subreddit: str):
    reddit = asyncpraw.Reddit(**reddit_details)
    async with reddit:
        subreddit = await reddit.subreddit(subreddit)
        async for comment in subreddit.stream.comments():
            yield comment


async def main(subreddit: str):
    # define db query
    columns = ['_id', 'created_utc', 'ups', 'num_comments', 'title', 'body']
    query = f"""
        INSERT INTO {subreddit} ({', '.join(columns)}) VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (_id)
        DO UPDATE SET ({', '.join(columns[1:])})
            = ({", ".join(['EXCLUDED.' + col for col in columns[1:]])}) 
        """
    count = 0

    # it is recommended to use global connection_pool object
    # and connect to the db once and push transactions through it
    # but cant install the required dependencies.
    # So lets just wrap everything around a single connection for now.
    async with await psycopg.AsyncConnection.connect(**db_details, dbname='reddit') as aconn:
        async with aconn:
            # now lets deal with getting the comment data stream
            # and pushing it into the db with the connection above
            async for comment in comment_stream(subreddit):
                comment_ = parse_comment(comment)
                comment_ = filter_and_fix_comment(comment_)
                if comment_:
                    await aconn.cursor().execute(query, tuple(comment_.values()))
                    await aconn.commit()

                    count += 1
                    if count % 1000 == 0 or count == 1:
                        print(
                            time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                            'pushed:', count
                        )


if __name__ == "__main__":

    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('--subreddit', type=str, default='cryptocurrency')
    args = parser.parse_args()
    args_dict = vars(args)

    while True:
        try:
            asyncio.run(main(subreddit=args_dict['subreddit']))
        except Exception as e:
            print(
                'Failed to run the main loop. Time:',
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                'Error: ', e
            )
            time.sleep(60)
