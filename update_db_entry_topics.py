from private import mongo_details
from pymongo import MongoClient


# TODO: use regex for speed and accuracy
topics = {
    'GME': ['gme', 'gamestop', 'gamestonk'],
    'AMD': ['amd'],
    'PLTR': ['pltr', 'palantir'],
    'SPY': ['spy'],
    'DOGE': ['dogecoin'],
    'crypto': ['coin', 'cypto', 'mining'],
    'UVXY': ['uvxy'],
    'SDOW': ['sdow'],
    'SQQQ': ['sqqq'],
    'SRTY': ['srty']
}


def update_topics() -> None:

    # db connection
    db_client = MongoClient(**mongo_details)
    total_updates = 0

    # move through all the docs in batches and do the thing
    cur = db_client.reddit.data.find({})
    for doc in cur:

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
                        if 'metadata' not in doc:
                            doc['metadata'] = {}
                        if 'topics' not in doc['metadata']:
                            doc['metadata']['topics'] = []

                    # add topic
                    if topic_key not in doc['metadata']['topics']:
                        doc['metadata']['topics'] += [topic_key]
                        new_topic_present = True

        # update the doc
        # should better use db_client.reddit.data.bulk_write ?
        if new_topic_present:
            _ = db_client.reddit.data.update_one(
                {'_id': doc['_id']},
                {'$set': {'metadata': doc['metadata']}}
            )
            total_updates += 1

    print(total_updates)


def update_topics_of_child_docs() -> None:
    '''
    Lets say we've the following:

    (doc1) submission with topics = ['bar', 'baz']
    (doc2) comment of submission with topics = ['bar']
    (doc3) comment of comment with topics = ['foo']

    This should do the following:
    doc1 topics = ['bar', 'baz']
    doc2 topics = set(doc1 topics + ['bar'])
    doc3 topics = set(doc2 topics + ['foo'])

    Should topics be differenciated?
    Say direct and inherited..?

    :return:
    '''
    pass