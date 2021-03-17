# to be able to launch this from terminal
import sys, os
sys.path.append(os.getcwd())

from private import mongo_details
import motor.motor_asyncio
import asyncio
import argparse
import datetime, time
from topics import topics


async def update_topics(start: int = None, end: int = None) -> None:

    # db connection
    db_client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_details)
    total_updates = 0
    total_docs = 0

    # move through all the docs and do the thing
    time_filter = {
        '$and': [
            {'data.created_utc': {'$gte': start}} if start else {},
            {'data.created_utc': {'$lte': end}} if end else {}
        ]}
    print(time_filter)
    cur = db_client.reddit.data.find(time_filter)
    async for doc in cur:
        new_topic_present = False
        first_doc_pass = True

        # maybe not the best solution, but lets concat all text
        # to single string here, so we do this only once per doc
        doc_text = str()
        text_fields_to_check = ['title', 'selftext', 'body']
        for field in text_fields_to_check:
            if field in doc['data']:
                doc_text += '//' + doc['data'][field].lower()

        # loop over topics
        for topic_key, topic_vals in topics.items():
            for topic_val in topic_vals:

                # check if topic is present in doc_text
                if topic_val in doc_text:

                    # check if metadata fields exist and add if not
                    # this will only trigger for new docs
                    # use first_doc_pass to check only once during
                    # the first pass of topic find
                    if first_doc_pass:
                        first_doc_pass = False
                        # also check if direct key is in metadata
                        # this is due to different format before
                        if 'metadata' not in doc or \
                                'direct' not in doc['metadata']:
                            doc['metadata'] = {
                                'topics': {
                                    'direct': [],
                                    'indirect': []
                                }
                            }

                    # add topic
                    if topic_key not in doc['metadata']['topics']['direct']:
                        doc['metadata']['topics']['direct'] += [topic_key]
                        new_topic_present = True

        # update the doc
        # should better use db_client.reddit.data.bulk_write ?
        if new_topic_present:
            _ = await db_client.reddit.data.update_one(
                {'_id': doc['_id']},
                {'$set': {'metadata': doc['metadata']}}
            )
            total_updates += 1

        total_docs += 1
        if total_docs % 10000 == 0:
            print(f'total docs scanned: {total_docs}')

    print(f'total docs: {total_docs} total updates: {total_updates}')


def date_string_to_timestamp(s: str) -> int:
    date = datetime.datetime.strptime(s, '%Y-%m-%d')
    tuple = date.timetuple()
    return int(time.mktime(tuple))


if __name__ == '__main__':

    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('--start', type=str, default=None)
    parser.add_argument('--end', type=str, default=None)
    args = parser.parse_args()

    # deal with args format
    args_dict = vars(args)
    if args_dict['start']:
        args_dict['start'] = date_string_to_timestamp(args_dict['start'])
    if args_dict['end']:
        args_dict['end'] = date_string_to_timestamp(args_dict['end'])

    print(args_dict)

    asyncio.run(update_topics(**args_dict))