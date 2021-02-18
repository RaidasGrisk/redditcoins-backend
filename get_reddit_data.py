"""
Reddit structure:
    subreddit (r/learnpython)
        - submission
            - comment
                - comment
                    - comment
                - comment
                - comment
                        - ...

-
Backstory and context:

PRAW :
    - no functionality to get all data (limits to 1000 or something)
    - no functionality to query by time (! this is serious)
    + always up to date data

Solution to these limits is pushshift.io
https://pypi.org/project/pushshift.py/
https://pypi.org/project/pushshift.py/

PushshiftAPI limits:
    - looks like recent comments are sometimes not available on PushshiftAPI
    - the api gets delayed at times (ranging from a few hours to days).
        Check the following for created_utc
        https://api.pushshift.io/reddit/search/comment/?subreddit=askreddit
        More on this issue:
        https://www.reddit.com/r/pushshift/comments/fg3arm/observing_high_ingestion_delays_30_min_for_recent/
        https://www.reddit.com/r/pushshift/comments/gqzrky/delay_in_getting_comments/
    + can search by time

Solution to these limits is a combination of PRAW and PushshiftAPI:
    get submission ids on PushshiftAPI (filtered by time)
    for each id get data (submission and comments) on PRAW

Hard to tell if this is a good solution.
Querying twice (and more) instead of once is not optimal?

"""

from private import reddit_details
from psaw import PushshiftAPI
from typing import List
import asyncpraw
import datetime
import asyncio

# init
# this should prob go into a class as an attr
# leave it simple for now, so its easier to debug
reddit = asyncpraw.Reddit(**reddit_details)
api = PushshiftAPI()


# sometimes pushshiftAPI db has delay
# check when was the last comment added
def check_time_since_last_comment_in_pushshiftAPI() -> None:
    time_last_comment = list(api.search_comments(limit=1))[0].created_utc
    time_diff = datetime.datetime.now() - \
                datetime.datetime.fromtimestamp(time_last_comment)
    print(time_diff)


# ------ #

def get_submission_ids(subreddit: str, before: int, after: int, **kwargs: any) -> List[str]:
    """
    :param subreddit: subreddit name as on the app
    :param before: int (!) timestamp
    :param after: int (!) timestamp
    :return: list of ids
    """
    subs = api.search_submissions(
            # me sure these are ints, as else the request will stall
            after=after,
            before=before,
            filter=['id'],
            subreddit=subreddit,
            # must include sort='asc' / sort='desc' as else will face this issue:
            # https://github.com/dmarx/psaw/issues/27
            sort='asc',
            **kwargs
    )
    return [sub.id for sub in subs]


async def get_submission(reddit: asyncpraw.Reddit, id: str) -> List[dict]:
    """
    :param id: a submission id
    :return: list of dicts with: where each item is submission + its comment/s
    """

    def parse_submission(submission: asyncpraw.reddit.Submission) -> dict:
        # parse the fields selected, we do not need all of it
        # [print("'", i, "'", ',', sep='') for i in dir(submission) if not i.startswith('_')]

        # TODO: consider _id field use in local db. Is the id field
        #  returned from reddit is unique? if yes, use it as _id.

        attrs = [
            'id',
            'created_utc',
            'subreddit_name_prefixed',
            'num_comments',
            'total_awards_received',
            'ups',
            'view_count',
            'title',
            'selftext',
        ]

        return {key: getattr(submission, key) for key in attrs}

    def parse_comment(comment: asyncpraw.reddit.Comment) -> dict:
        # parse the fields selected, we do not need all of it
        # [print("'", i, "'", ',', sep='') for i in dir(comment) if not i.startswith('_')]
        attrs = [
            'id',
            'link_id',
            'parent_id',
            'is_root',
            'depth',
            'created_utc',
            'total_awards_received',
            'ups',
            'body',
        ]

        return {key: getattr(comment, key) for key in attrs}

    sub = await reddit.submission(id)
    sub_data = parse_submission(sub)

    comments = await sub.comments()
    if comments:

        # deal with comments that are not loaded into the request
        # this will result in additional network request
        # TODO: try except status for http size limit hit
        await comments.replace_more(limit=0)

        # comments.list() returns a list where all top level
        # comments are listed first, then second level comments and so on
        # this is not a problem as we can parse parent_id and depth level
        comments = comments.list()
        comments_ = [parse_comment(com) for com in await comments]  # why Union!?

    return [sub_data] if not comments else [sub_data] + comments_


def get_submissions(sub_ids: list, reddit_details: dict) -> list:

    async def fetch_submissions(sub_ids, reddit_details):
        # make each sub_id request into a task and
        # gather all tasks together

        # have to include init of reddit object inside the async loop
        # else async loop raise an error. Should improve this fix :/
        reddit_session = asyncpraw.Reddit(**reddit_details)

        # use context or else the session above will not be closed
        # and warning/errors will pop for each request or session
        async with reddit_session as reddit:
            tasks = set()
            for id in sub_ids:
                tasks.add(
                    asyncio.create_task(get_submission(reddit, id))
                )
            return await asyncio.gather(*tasks)

    # run all tasks and return list of results
    return asyncio.run(fetch_submissions(sub_ids, reddit_details))


# ------- #
# combine methods to make a final function


def get_reddit_data(start: datetime.datetime,
                    end: datetime.datetime,
                    subreddit: str
) -> List[List[dict]]:

    # returns List[List[dict]] where:
    # [[{sub}, {comm}, {comm} ..], [{sub}, {comm}, {comm} ...], ...]

    # Issues:
    # How to properly filter out removed/deleted submissions.
    # As of now, there seems to be no way to check if sub is
    # deleted/removed before the first request to PushshiftAPI
    # (get_submission_ids). This info is only available after
    # the second request to PRAW (get_submission).

    submission_params = {
        'subreddit': subreddit,
        'after': int(start.timestamp()),
        'before': int(end.timestamp()),
        'num_comments': '>5',  # 1st might be admin removal notice
    }

    # pull a list of submission ids from PushshiftAPI
    # pull submission details from PRAW
    sub_ids = get_submission_ids(**submission_params)
    data_batch = get_submissions(sub_ids, reddit_details)

    return data_batch


def test_and_print() -> None:

    data = get_reddit_data(
        start=datetime.datetime.now() - datetime.timedelta(hours=6),
        end=datetime.datetime.now(),
        subreddit='wallstreetbets',
    )

    # check some stats
    total = 0
    for sub in data:
        total += len(sub)
    print(f'# subs: {len(data)}, # subs+comments {total}', )

    # here we would push chunks to local db
    import json
    print(json.dumps(data[0][0], indent=4))
