"""
TODO: set up new db
TODO: setup VM with data stream and push to new DB
TODO: migrate old data to new db
TODO: create db jobs (daily / hourly / web_data)
TODO: create API
TODO: vercel website
"""

import asyncpraw
import asyncio
import psycopg
import time
from private import reddit_details, db_details
from aiostream import stream


def parse_item(item: asyncpraw.reddit.Comment | asyncpraw.reddit.Submission) -> dict:
    # the item can be either a comment or a submission
    # comments have body attr, submissions have title and selftext attrs
    # lets gather all of them and set it to None if it does not exist
    attrs = [
        'id',
        'created_utc',
        'title',  # only submissions
        'body',  # only comments
        'selftext',  # only submissions
    ]
    return {key: getattr(item, key, None) for key in attrs}


async def reddit_stream(reddit_details, subreddit):
    # Does the following:
    # 1. create comment stream
    # 2. create submission stream
    # 3. merge both streams into a single stream
    # 4. yield stream items

    async def comment_stream(subreddit: asyncpraw.reddit.Subreddit) -> asyncpraw.Reddit.comment:
        async for comment in subreddit.stream.comments():
            yield comment

    async def submission_stream(subreddit: asyncpraw.reddit.Subreddit) -> asyncpraw.Reddit.submission:
        async for submission in subreddit.stream.submissions():
            yield submission

    reddit = asyncpraw.Reddit(**reddit_details)
    async with reddit:
        subreddit_ = await reddit.subreddit(subreddit)
        merged_stream = stream.merge(comment_stream(subreddit_), submission_stream(subreddit_))
        async with merged_stream.stream() as streamer:
            async for item in streamer:
                yield item


async def main(reddit_details: dict, subreddit: str = 'cryptocurrency') -> None:

    # define db query
    columns = ['_id', 'created_utc', 'title', 'body', 'selftext']
    query = f"""
        INSERT INTO {subreddit} ({', '.join(columns)}) VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (_id)
        DO UPDATE SET ({', '.join(columns[1:])})
            = ({", ".join(['EXCLUDED.' + col for col in columns[1:]])}) 
        """
    count = 0

    # it is recommended to use global connection_pool object
    # and connect to the db once and push transactions through it
    # but cant install the required dependencies.
    # So lets just wrap everything around a single connection for now.
    # TODO: https://www.psycopg.org/psycopg3/docs/advanced/pool.html
    async with await psycopg.AsyncConnection.connect(**db_details, dbname='reddit') as aconn:
        async with aconn:
            streamer = reddit_stream(reddit_details, subreddit)
            async for item in streamer:
                if item:
                    item_ = parse_item(item)
                    await aconn.cursor().execute(query, tuple(item_.values()))
                    await aconn.commit()

                    count += 1
                    if count % 1000 == 0 or count == 1:
                        print(
                            time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                            'pushed:', count
                        )


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main(reddit_details=reddit_details, subreddit='cryptocurrency'))
        except Exception as e:
            print(
                'Failed to run the main loop. Time:',
                time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()),
                'Error: ', e
            )
            time.sleep(10)
