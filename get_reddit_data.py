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
import praw
import datetime

# init
reddit = praw.Reddit(**reddit_details)

api = PushshiftAPI(reddit)


# sometimes pushshiftAPI db has delay
# check when was the last comment added
def check_time_since_last_comment_in_pushshiftAPI() -> None:
    time_last_comment = list(api.search_comments(limit=1))[0].created_utc
    time_diff = datetime.datetime.now() - \
                datetime.datetime.fromtimestamp(time_last_comment)
    print(time_diff)


# ------ #

def get_submission_ids(subreddit: str, before: int, after: int, **kwargs: any) -> list:
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


def get_submission_data(id: str) -> list:
    """
    :param id: a submission id
    :return: list of dicts with: where each item is submission + its comment/s
    """

    def parse_submission(submission: praw.reddit.Submission) -> dict:
        # parse the fields selected, we do not need all of it
        # [print("'", i, "'", ',', sep='') for i in dir(submission) if not i.startswith('_')]

        # TODO: consider _id field use in local db. Is the id field
        #  returned from reddit is unique? if yes, use it as _id.

        attrs = [
            # 'STR_FIELD',
            # 'all_awardings',
            # 'allow_live_comments',
            # 'approved_at_utc',
            # 'approved_by',
            # 'archived',
            # 'author',
            # 'author_flair_background_color',
            # 'author_flair_css_class',
            # 'author_flair_richtext',
            # 'author_flair_template_id',
            # 'author_flair_text',
            # 'author_flair_text_color',
            # 'author_flair_type',
            # 'author_fullname',
            # 'author_patreon_flair',
            # 'author_premium',
            # 'awarders',
            # 'banned_at_utc',
            # 'banned_by',
            # 'can_gild',
            # 'can_mod_post',
            # 'category',
            # 'clear_vote',
            # 'clicked',
            # 'comment_limit',
            # 'comment_sort',
            # 'comments',
            # 'content_categories',
            # 'contest_mode',
            # 'created',
            'created_utc',
            # 'crosspost',
            # 'delete',
            # 'disable_inbox_replies',
            # 'discussion_type',
            # 'distinguished',
            # 'domain',
            'downs',
            # 'downvote',
            # 'duplicates',
            # 'edit',
            # 'edited',
            # 'enable_inbox_replies',
            # 'flair',
            # 'fullname',
            # 'gild',
            # 'gilded',
            # 'gildings',
            # 'hidden',
            # 'hide',
            # 'hide_score',
            'id',
            # 'id_from_url',
            # 'is_crosspostable',
            # 'is_meta',
            # 'is_original_content',
            # 'is_reddit_media_domain',
            # 'is_robot_indexable',
            # 'is_self',
            # 'is_video',
            # 'likes',
            # 'link_flair_background_color',
            # 'link_flair_css_class',
            # 'link_flair_richtext',
            # 'link_flair_template_id',
            # 'link_flair_text',
            # 'link_flair_text_color',
            # 'link_flair_type',
            # 'locked',
            # 'mark_visited',
            # 'media',
            # 'media_embed',
            # 'media_only',
            # 'mod',
            # 'mod_note',
            # 'mod_reason_by',
            # 'mod_reason_title',
            # 'mod_reports',
            # 'name',
            # 'no_follow',
            'num_comments',
            # 'num_crossposts',
            # 'num_duplicates',
            # 'num_reports',
            # 'over_18',
            # 'parent_whitelist_status',
            # 'parse',
            # 'permalink',
            # 'pinned',
            # 'pwls',
            # 'quarantine',
            # 'removal_reason',
            # 'removed_by',
            # 'removed_by_category',
            # 'reply',
            # 'report',
            # 'report_reasons',
            # 'save',
            # 'saved',
            # 'score',
            # 'secure_media',
            # 'secure_media_embed',
            'selftext',
            # 'selftext_html',
            # 'send_replies',
            # 'shortlink',
            # 'spoiler',
            # 'stickied',
            # 'subreddit',  # this is an object
            # 'subreddit_id',
            'subreddit_name_prefixed',
            # 'subreddit_subscribers',
            # 'subreddit_type',
            # 'suggested_sort',
            # 'thumbnail',
            # 'thumbnail_height',
            # 'thumbnail_width',
            'title',
            # 'top_awarded_type',
            'total_awards_received',
            # 'treatment_tags',
            # 'unhide',
            # 'unsave',
            'ups',
            # 'upvote',
            # 'upvote_ratio',
            # 'url',
            # 'user_reports',
            'view_count',
            # 'visited',
            # 'whitelist_status',
            # 'wls',
        ]

        return {key: getattr(submission, key) for key in attrs}

    def parse_comment(comment: praw.reddit.Comment) -> dict:
        # parse the fields selected, we do not need all of it
        # [print("'", i, "'", ',', sep='') for i in dir(comment) if not i.startswith('_')]
        attrs = [
            # 'MISSING_COMMENT_MESSAGE',
            # 'STR_FIELD',
            # 'all_awardings',
            # 'approved_at_utc',
            # 'approved_by',
            # 'archived',
            # 'associated_award',
            # 'author',
            # 'author_flair_background_color',
            # 'author_flair_css_class',
            # 'author_flair_richtext',
            # 'author_flair_template_id',
            # 'author_flair_text',
            # 'author_flair_text_color',
            # 'author_flair_type',
            # 'author_fullname',
            # 'author_patreon_flair',
            # 'author_premium',
            # 'awarders',
            # 'banned_at_utc',
            # 'banned_by',
            # 'block',
            'body',
            # 'body_html',
            # 'can_gild',
            # 'can_mod_post',
            # 'clear_vote',
            # 'collapse',
            # 'collapsed',
            # 'collapsed_because_crowd_control',
            # 'collapsed_reason',
            # 'comment_type',
            # 'controversiality',
            # 'created',
            'created_utc',
            # 'delete',
            'depth',
            # 'disable_inbox_replies',
            # 'distinguished',
            'downs',
            # 'downvote',
            # 'edit',
            # 'edited',
            # 'enable_inbox_replies',
            # 'fullname',
            # 'gild',
            # 'gilded',
            # 'gildings',
            'id',
            # 'id_from_url',
            'is_root',
            # 'is_submitter',
            # 'likes',
            'link_id',
            # 'locked',
            # 'mark_read',
            # 'mark_unread',
            # 'mod',
            # 'mod_note',
            # 'mod_reason_by',
            # 'mod_reason_title',
            # 'mod_reports',
            # 'name',
            # 'no_follow',
            # 'num_reports',
            # 'parent',
            'parent_id',
            # 'parse',
            # 'permalink',
            # 'refresh',
            # 'removal_reason',
            # 'replies',
            # 'reply',
            # 'report',
            # 'report_reasons',
            # 'save',
            # 'saved',
            'score',
            # 'score_hidden',
            # 'send_replies',
            # 'stickied',
            # 'submission',
            # 'subreddit',
            # 'subreddit_id',
            # 'subreddit_name_prefixed',
            # 'subreddit_type',
            # 'top_awarded_type',
            'total_awards_received',
            # 'treatment_tags',
            # 'uncollapse',
            # 'unsave',
            'ups',
            # 'upvote',
            # 'user_reports',
        ]

        return {key: getattr(comment, key) for key in attrs}

    # pull data using praw
    sub = reddit.submission(id)

    # parse submission
    sub_data = parse_submission(sub)

    if sub._comments:

        # deal with comments that are not loaded into the request
        # this will result in additional network request
        sub.comments.replace_more(limit=5)

        # sub.comments.list() returns a list where all top level
        # comments are listed first, then second level comments and so on
        # this is not a problem as we can parse parent_id and depth level
        comments = sub.comments.list()
        comments_ = [parse_comment(com) for com in comments]  # why Union!?

    return [sub_data] if not sub._comments else [sub_data] + comments_

# ------- #
# combine methods to make a final function


def get_reddit_data(start: datetime.datetime,
                    end: datetime.datetime,
                    subreddit: str
                    ) -> list:

    # Issues:

    # How to properly filter out removed/deleted submissions.
    # As of now, there seems to be no way to check if sub is
    # deleted/removed before the first request to PushshiftAPI
    # (get_submission_ids). This info is only available after
    # the second request to PRAW (get_submission_data).

    # querying PushshiftAPI for long date intervals results in weird outputs
    # so the following solution to split each interval to fixed time chunks

    def split_date_interval_to_chunks(
            start: datetime.datetime,
            end: datetime.datetime,
            delta: datetime.timedelta
    ) -> list:

        last_start, last_end = start, start
        time_chunks = []
        while last_end < end:
            last_end = last_start + delta
            time_chunks.append([last_start, last_end])
            last_start += delta

        return time_chunks

    # make time intervals
    intervals = split_date_interval_to_chunks(
        start=start,
        end=end,
        delta=datetime.timedelta(minutes=60)
    )

    # TODO: should consider async because the following is very slow
    # https://praw.readthedocs.io/en/latest/getting_started/ \
    # multiple_instances.html#discord-bots-and-asynchronous-environments

    # pull data
    for start_, end_ in intervals:

        submission_params = {
            'subreddit': subreddit,
            'after': int(start_.timestamp()),
            'before': int(end_.timestamp()),
            'num_comments': '>5',  # 1st might be admin removal notice
        }

        # pull a list of submission ids
        sub_ids = get_submission_ids(**submission_params)

        # pull submission data
        data_batch = []
        for sub_id in sub_ids:
            data = get_submission_data(sub_id)
            data_batch.append(data)

        yield data_batch
        del data_batch


def test():

    # the following is a generator yielding data chunks
    data = get_reddit_data(
        start=datetime.datetime.now() - datetime.timedelta(minutes=60*4),
        end=datetime.datetime.now(),
        subreddit='wallstreetbets'
    )

    import json
    for chunk in data:

        # here we would push chunks to local db
        print(json.dumps(chunk, indent=4))


# ------- #
# ------- #
# PushshiftAPI
# debug and try other methods

#
# def debug_and_test_other_ways():
#
#     # helpers
#     def parse_timestamp(ts: float) -> str:
#         str_format = '%Y-%m-%d %H:%M:%S'
#         return datetime.datetime.fromtimestamp(ts).strftime(str_format)
#
#     # get submissions: PushshiftAPI
#     subs = api.search_submissions(filter=['id'], subreddit='wallstreetbets', limit=2)
#     [print(sub.d_) for sub in list(subs)]
#
#     # the following seems to be returning only 1 day old comments?
#     coms = api.search_comments(subreddit='wallstreetbets', limit=2)
#     [print(parse_timestamp(com.created), '|', com.id, '|', com.body[:50], '|', '|') for com in list(coms)]
#
#     # ------- #
#     # ------- #
#     # PRAW
#     # debug and try other methods
#
#     # recurse through the comments of a comment
#     def parse_comments(comment, depth=0):
#         print(' '*depth, parse_timestamp(comment.created), comment.id, comment.body[:10])
#         for rep in comment.replies:
#             depth += 1
#             parse_comments(rep, depth)
#
#     for sub in reddit.subreddit('learnpython').hot(limit=2):
#
#         time = parse_timestamp(sub.created)
#         print(time, '|', sub.id, '|', sub.title[:5], '|', sub.selftext[:5])
#
#         # deal with comments that are not loaded into the request
#         # this will result in addition network request
#         sub.comments.replace_more(limit=5)
#
#         # this should be recursive because some comments will have
#         # many comments that will be commented (actually, replied to)
#         # Also it can be done in other ways as shown:
#         # https://praw.readthedocs.io/en/latest/tutorials/comments.html
#         for com in sub.comments:
#             parse_comments(com, depth=1)
#