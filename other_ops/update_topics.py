"""
TODO:
 1. improve regex logic (maybe with a trie). Do in single pass.
 2. current speed bottleneck is due to db conn update_one fn.
    consider update_many
"""

# to be able to launch this from terminal
import sys, os
sys.path.append(os.getcwd())

from private import db_details
import asyncpg
import asyncio
import argparse
import datetime, time
import re
from typing import Set
from other_ops.topics import get_topics


def get_regex_pattern(text_parts: Set[str]) -> re.Pattern:
    """
    https://stackoverflow.com/questions/6713310/regex-specify-space-or-start-of-string-and-space-or-end-of-string
    (^|\s) would match space or start of string and
    ($|\s) for space or end of string. Together it's:
    (^|\s)stackoverflow($|\s)

    for multiple possible matches modify the
    middle as so: (stackoverflow|flow|stack)
    lets add more or cases in front of the ticker
    for example matching $ticker (^|\s|[$])
    so now the following triggers a match:
    'tsla', ' tsla', ' tsla ', '$tsla', '$tsla '
    """

    # make regex string by inserting possible text_parts
    text_parts_joined = "|".join(text_parts)
    regex_pattern = f'(^|\s|[$])({text_parts_joined})($|\s)'
    regex_pattern = re.compile(regex_pattern)
    return regex_pattern


def debug_regex_matching() -> None:

    text_parts = {'FB', 'facebook'}
    full_texts = ['FBfoo fooFB fooFBbar FBfooFB FBFBFB',
                 'FB', 'foo facebook', 'foo $FB', 'foo$FB']

    pattern = get_regex_pattern(text_parts)
    for full_text in full_texts:
        if pattern.search(full_text):
            print('Match:', full_text, text_parts)
        else:
            print('No match:', full_text, text_parts)


async def update_topics(
        subreddit: str,
        start: int = None,
        end: int = None
) -> None:

    # get topics and do things to increase the speed
    topics = get_topics()

    # the following is to increase the speed perf:
    # before looping over each topic, combine all
    # possible matches into a single re pattern
    # will search this pattern first, and if found
    # will do a full scan over each topic separately
    all_topics_re_string = {topic for topics in topics.values() for topic in topics}
    all_topics_re_string = get_regex_pattern(all_topics_re_string)
    all_topics_re_pattern = re.compile(all_topics_re_string)

    # pre compile every re pattern so that
    # we do this only once before the main loop
    topics_re_compiled = {topic: get_regex_pattern(values) for topic, values in topics.items()}

    conn = await asyncpg.connect(**db_details, database='reddit')

    # fetch records
    select_sql = ''.join([
        f'SELECT * FROM {subreddit} ',
        f'WHERE created_utc >= {start} ' if start else '',
        f'AND created_utc < {end} ' if end else '',
        f'ORDER BY created_utc ASC'
    ])

    total_count_sql = ''.join([
        f'SELECT COUNT(*) FROM {subreddit} ',
        f'WHERE created_utc >= {start} ' if start else '',
        f'AND created_utc < {end} ' if end else '',
    ])

    total_docs_scanned = 0
    total_updates = 0
    total_docs_ = await conn.fetch(total_count_sql)
    total_docs = total_docs_[0]['count']
    print(f'Total {total_docs}')

    async with conn.transaction():
        async for doc in conn.cursor(select_sql):

            # convert record to dict
            doc = dict(doc)

            # maybe not the best solution, but lets concat all text
            # to single string here, so we do this only once per doc
            doc_text = ' | '.join(
                [doc.get(key) for key in ['title', 'body'] if doc.get(key)]
            )

            # loop over topics
            # when simply looping over each topic and looking
            # for matches, when then number of topics increase
            # the speed is reduced dramatically.
            # here's an idea how to speed things up:
            # instead of looping over each topic, concat
            # topic_vals together and do single regex search
            # per doc. Then if match is hit, find the
            # corresponding keys.
            if all_topics_re_pattern.search(doc_text):
                for topic_key, topic_re_pattern in topics_re_compiled.items():
                    if topic_re_pattern.search(doc_text):

                        # add topic
                        new_doc = {
                            '_id': doc['_id'] + '_' + topic_key,
                            'created_time': datetime.datetime.utcfromtimestamp(doc["created_utc"]).strftime('%Y-%m-%d %H:%M:%S'),
                            'ups': doc['ups'],
                            'is_comment': doc.get('title') == None,
                            'topic': topic_key
                        }

                        insert_sql = f"""
                            INSERT INTO {subreddit + '_'} (
                                _id, created_time, ups, is_comment, topic
                            ) VALUES {
                                new_doc['_id'], 
                                new_doc['created_time'], 
                                new_doc['ups'], 
                                new_doc['is_comment'], 
                                new_doc['topic']
                            }
                            ON CONFLICT (_id)
                            DO UPDATE SET (created_time, ups, is_comment, topic)
                                = (
                                    EXCLUDED.created_time, EXCLUDED.ups, 
                                    EXCLUDED.is_comment, EXCLUDED.topic
                                ) 
                            """

                        # TODO: https://magicstack.github.io/asyncpg/current/api/index.html#asyncpg.connection.Connection.executemany
                        await conn.execute(insert_sql)
                        total_updates += 1

            total_docs_scanned += 1
            if total_docs_scanned % 10000 == 0:
                print(
                    f'Total {total_docs} '
                    f'Scanned: {total_docs_scanned} '
                    f'updated {total_updates} '
                    f'last date {datetime.datetime.fromtimestamp(doc["created_utc"])}'
                )

# asyncio.run(update_topics(subreddit='satoshistreetbets', start=1585602000, end=1695688400))


async def wipe_topics(
        subreddit: str,
        start: int = None,
        end: int = None
) -> None:

    conn = await asyncpg.connect(**db_details, database='reddit')
    delete_sql = ''.join([
        f'SELECT * FROM {subreddit + "_"} ',
        f'WHERE created_utc >= {start} ' if start else '',
        f'AND created_utc < {end} ' if end else '',
    ])
    _ = await conn.execute(delete_sql)
    print(_)


def date_string_to_timestamp(s: str) -> int:
    date = datetime.datetime.strptime(s, '%Y-%m-%d')
    tuple = date.timetuple()
    return int(time.mktime(tuple))


if __name__ == '__main__':

    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('--subreddit', type=str, default='cryptocurrency')
    parser.add_argument('--start', type=str, default=None)
    parser.add_argument('--end', type=str, default=None)
    parser.add_argument('--wipe_topics', type=bool, default=False)
    args = parser.parse_args()

    # deal with args format
    args_dict = vars(args)
    if args_dict['start']:
        args_dict['start'] = date_string_to_timestamp(args_dict['start'])
    if args_dict['end']:
        args_dict['end'] = date_string_to_timestamp(args_dict['end'])

    print(args_dict)

    # lets not run these two one after another
    # lets run wipe if True else run update
    # the logic is rather bad but get on with it
    if args_dict.pop('wipe_topics'):
        # pop another arg as it is not
        # part of wipe_topics function
        args_dict.pop('topics_type', None)
        asyncio.run(wipe_topics(**args_dict))
    else:
        asyncio.run(update_topics(**args_dict))