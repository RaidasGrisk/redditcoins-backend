# to be able to launch this from terminal
import sys, os
sys.path.append(os.getcwd())

from private import mongo_details
import motor.motor_asyncio
import asyncio
import argparse
import datetime, time
import re
from other_ops.topics import main as get_topics


def does_match(full_text, text_parts):
    """
    lets do some matching following this logic
    https://stackoverflow.com/questions/6713310/regex-specify-space-or-start-of-string-and-space-or-end-of-string
    (^|\s) would match space or start of string and ($|\s) for space or end of string. Together it's:
    (^|\s)stackoverflow($|\s)

    for multiple possible matches modify the middle as so: (stackoverflow|flow|stack)
    lets add more or cases in front of the ticker for example matching $ticker (^|\s|[$])
    so now the following triggers a match: 'tsla', ' tsla', ' tsla ', '$tsla', '$tsla '
    """

    # make regex string by inserting possible text_parts
    text_parts_joined = "|".join(text_parts)
    regex_string = f'(^|\s|[$])({text_parts_joined})($|\s)'
    return re.search(regex_string, full_text)


def match_debug_test():

    text_parts = {'fb', 'facebook'}
    full_texts = ['fbfoo foofb foofbbar fbfoofb fbfbfb',
                 'fb', 'foo fb', 'foo $fb', 'foo$fb']

    for full_text in full_texts:
        if does_match(full_text, text_parts):
            print('Match:', full_text, text_parts)
        else:
            print('No match:', full_text, text_parts)


async def update_topics(start: int = None, end: int = None) -> None:

    # db connection
    db_client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_details)
    total_updates = 0
    total_docs_scanned = 0

    # get topics to look for
    topics = get_topics()

    # move through all the docs and do the thing
    time_filter = {
        '$and': [
            {'data.created_utc': {'$gte': start}} if start else {},
            {'data.created_utc': {'$lte': end}} if end else {}
        ]}

    total_docs = await db_client.reddit.data.count_documents(time_filter)
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
                doc_text += '//' + doc['data'][field]

        # loop over topics
        # TODO: speed perf bottleneck right here ↓
        #  if there are many topics >1000 the speed is
        #  misserable, decent speed only with 100< topics
        for topic_key, topic_vals in topics.items():

            # check if topic is present in doc_text
            if does_match(doc_text, topic_vals):

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

        total_docs_scanned += 1
        if total_docs_scanned % 10000 == 0:
            print(f'Scanned: {total_docs_scanned} '
                  f'out of {total_docs} '
                  f'updated {total_updates}')


async def wipe_topics(start: int = None, end: int = None) -> None:

    # db connection
    db_client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_details)

    filter = {
        '$and': [
            {'data.created_utc': {'$gte': start}} if start else {},
            {'data.created_utc': {'$lte': end}} if end else {},
            # the following filter only docs with already set topics
            {'metadata.topics': {'$exists': True}}
        ]}

    result = await db_client.reddit.data.update_many(
        filter,
        {'$set': {'metadata.topics': {'direct': [], 'indirect': []}}}
    )

    print(f'matched: {result.matched_count} modified {result.modified_count}')



def date_string_to_timestamp(s: str) -> int:
    date = datetime.datetime.strptime(s, '%Y-%m-%d')
    tuple = date.timetuple()
    return int(time.mktime(tuple))


if __name__ == '__main__':

    # parse args
    parser = argparse.ArgumentParser()
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
        asyncio.run(wipe_topics(**args_dict))
    else:
        asyncio.run(update_topics(**args_dict))