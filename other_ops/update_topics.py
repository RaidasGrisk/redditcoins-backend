"""
TODO:
 1. improve regex logic (maybe with a trie). Do in single pass.
 2. current speed bottleneck is due to db conn update_one fn.
    consider update_many
"""
import sys
import os
sys.path.append(os.getcwd())

import time
from private import db_details
import asyncpg
import asyncio
import argparse
import datetime
import re
from typing import Set, List
from other_ops.topics import get_topics


def get_regex_pattern(coin_names: [str], coin_text: [str]) -> re.Pattern:
    # TODO: explain the two parts

    coin_name_regex_pattern = r'(^|\s|[$])(' + '|'.join(coin_names) + ')($|,|.|!|\s)'
    coin_text_regex_pattern = r'(?i:\b(?:' + '|'.join(coin_text) + r')\b)'

    # TODO: not very readable, but must do this else will match all cases.
    if not coin_names:
        return re.compile(coin_text_regex_pattern)
    if not coin_text:
        return re.compile(coin_name_regex_pattern)
    if coin_names and coin_text:
        return re.compile(coin_name_regex_pattern + '|' + coin_text_regex_pattern)


def debug_regex_matching() -> None:

    coin_names = ['ETH']
    coin_text = ['ethereum']

    positive = [
        'ethereum',
        'kjhasd ETHEREUM adsasd',
        'kjhagsd $ETH',
        'ETH',
        ' ETH',
        'ETH ',
        ' ETH ',
        '$ETH ',
        'ETH.'

        'asd EthereuM asd',
        'ethereum',
        ' ethereum',
        'ethereum ',
        'ethereum,',
    ]

    negative = [
        'akjshdETHjashdk',
        'kjahsdETH',
        'kjahsdETH ',
        'aETH',
        '-ETH',
        '.ETH',

        'ethereuma',
        'aethereum',
        'ADSethereum',
        'ethereumDDD',
        '1ethereum',
    ]

    pattern = get_regex_pattern(coin_names, coin_text)
    for item in positive:
        if pattern.search(item):
            print('Match:', item)
        else:
            print('No match:', item)

    for item in negative:
        if pattern.search(item):
            print('Match:', item)
        else:
            print('No match:', item)


async def update_topics(
        subreddit: str = 'cryptocurrency',
        start: int = None,
        end: int = None,
        topics_to_update: List[str] = None
) -> None:

    # get topics and do things to increase the speed
    topics = get_topics()

    # often I add a single or two coins and have to rescan
    # db for matches. Update only selected coins to increase speed
    if topics_to_update:
        topics = {topic: topics[topic] for topic in topics_to_update}

    # the following is to increase the speed perf:
    # before looping over each topic, combine all
    # possible matches into a single re pattern
    # will search this pattern first, and if found
    # will do a full scan over each topic separately

    coin_names = []
    for coin in topics.values():
        coin_names.extend(coin['name'])

    coin_text = []
    for coin in topics.values():
        coin_text.extend(coin['other'])

    regex_pattern = get_regex_pattern(coin_names, coin_text)

    # pre compile every re pattern so that
    # we do this only once before the main loop
    topics_re_compiled = {topic: get_regex_pattern(values['name'], values['other']) for topic, values in topics.items()}

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

    insert_sql = f"""
        INSERT INTO {subreddit + '_'} (
            _id, created_utc, is_comment, topic
        ) VALUES ($1, $2, $3, $4)
        ON CONFLICT (_id)
        DO UPDATE SET (created_utc, is_comment, topic)
            = (
                EXCLUDED.created_utc, EXCLUDED.is_comment, EXCLUDED.topic
            ) 
    """
    insert_rows = []
    async with conn.transaction():
        async for doc in conn.cursor(select_sql):

            # convert record to dict
            doc = dict(doc)

            # maybe not the best solution, but lets concat all text
            # to single string here, so we do this only once per doc
            doc_text = ' | '.join(
                [doc.get(key) for key in ['title', 'body', 'selftext'] if doc.get(key)]
            )

            # As the number of topics increase in size the speed is reduced dramatically.
            # Here's an idea how to speed things up: instead of looping over each topic, concat
            # topic_vals together and do single regex search per doc. Then if match is hit,
            # find the corresponding keys.
            if regex_pattern.search(doc_text):
                for topic_key, topic_re_pattern in topics_re_compiled.items():
                    if topic_re_pattern.search(doc_text):

                        # add topic
                        new_doc = {
                            '_id': doc['_id'] + '_' + topic_key,
                            'created_utc': doc['created_utc'],
                            'is_comment': doc.get('title') is None,
                            'topic': topic_key
                        }
                        insert_rows.append(new_doc)

                        if len(insert_rows) >= 500:
                            # Perform batch insert
                            values = [(row['_id'], row['created_utc'], row['is_comment'], row['topic']) for row in
                                      insert_rows]
                            await conn.executemany(insert_sql, values)
                            insert_rows = []

                        total_updates += 1

            total_docs_scanned += 1
            if total_docs_scanned % 100 == 0:
                print(
                    f'Total {total_docs} '
                    f'Scanned: {total_docs_scanned} '
                    f'updated {total_updates} '
                    f'last date {datetime.datetime.fromtimestamp(doc["created_utc"])}'
                )

        # finish any remaining rows
        if insert_rows:
            values = [(row['_id'], row['created_utc'], row['is_comment'], row['topic']) for row in insert_rows]
            await conn.executemany(insert_sql, values)


def date_string_to_timestamp(s: str) -> int:
    date = datetime.datetime.strptime(s, '%Y-%m-%d %H:%M:%S')
    tuple = date.timetuple()
    return int(time.mktime(tuple))


if __name__ == '__main__':

    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('--subreddit', type=str, default='cryptocurrency')
    parser.add_argument('--start', type=str, default=None)
    parser.add_argument('--end', type=str, default=None)
    parser.add_argument('--topics_to_update', type=str, nargs='+', default=[])
    args = parser.parse_args()

    # deal with args format
    args_dict = vars(args)
    if args_dict['start']:
        args_dict['start'] = date_string_to_timestamp(args_dict['start'])
    if args_dict['end']:
        args_dict['end'] = date_string_to_timestamp(args_dict['end'])

    asyncio.run(update_topics(**args_dict))
