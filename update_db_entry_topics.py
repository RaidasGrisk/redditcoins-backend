from private import mongo_details
from pymongo import MongoClient


# TODO: use regex for speed and accuracy
topics = {
    'GME': ['gme', 'gamestop', 'gamestonk'],
    'AMC': ['amc'],
    'PLTR': ['pltr', 'palantir'],
    'SPY': ['spy'],
    'DOGE': ['dogecoin'],
    'crypto': ['coin', 'cypto', 'mining'],
    'UVXY': ['uvxy'],
    'SDOW': ['sdow'],
    'SQQQ': ['sqqq'],
    'SRTY': ['srty'],
    'WSB': ['wsb']
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

    :return:
    '''

    # db connection
    db_client = MongoClient(**mongo_details)

    # loop over submissions
    sub_query = {

        # only submissions do have titles
        # comments have only body
        'data.title': {
            '$exists': True
        },

        # target only subs with comments
        # else there is nothing to update
        'data.num_comments' : {
            '$gt': 0
        },
    }

    sub_projection = {
        '_id': 1,
        'metadata.topics': 1
    }

    sub_cursor = db_client.reddit.data.find(
        filter=sub_query,
        projection=sub_projection
    )

    for sub in sub_cursor:
        print(sub)

        # 1. pull whole submission
        # link_id - submission id
        # parent_id - parent comment

        com_query = {
            'data.link_id': {
                '$eq': 't3_' + sub['_id']
            }
        }

        com_projection = {
            '_id': 1,
            'data.link_id': 1,
            'data.parent_id': 1,
            'data.depth': 1,
            'metadata.topics': 1
        }

        com_cursor = db_client.reddit.data.find(
            filter=com_query,
            projection=com_projection
        )

        full_sub = [sub] + [com for com in com_cursor]
        [print(i) for i in full_sub]
        print('\n'*2)


# --------- #
def test():

    full_sub = [
        {'_id': 'kzppqo', 'metadata': {'topics': {'direct': ['AAA', 'BBB']}}},

        {'_id': 'gjpbtca', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't3_kzppqo', 'depth': 0}, 'metadata': {'topics': {'direct': ['A', 'A2']}}},
        {'_id': 'gjp7m98', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't3_kzppqo', 'depth': 0}, 'metadata': {'topics': {'direct': ['B', 'B2']}}},
        {'_id': 'gjp7m4m', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't3_kzppqo', 'depth': 0}, 'metadata': {'topics': {'direct': ['C']}}},

        {'_id': 'gjpco5t', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpbtca', 'depth': 1}},
        {'_id': 'gjpoypf', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpbtca', 'depth': 1}},
        {'_id': 'gjpcgvk', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp7m98', 'depth': 1}},
        {'_id': 'gjpqlvw', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp7m98', 'depth': 1}},
        {'_id': 'gjxdsvn', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp7m98', 'depth': 1}},

        {'_id': 'gjpcpwz', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpco5t', 'depth': 2}, 'metadata': {'topics': {'direct': ['D', 'D1']}}},
        {'_id': 'gjpilaj', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpco5t', 'depth': 2}},
        {'_id': 'gjpgtd5', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpcgvk', 'depth': 2}, 'metadata': {'topics': {'direct': ['E']}}},
        {'_id': 'gjp89sa', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpcgvk', 'depth': 2}},
        {'_id': 'gjpjtgi', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpg7bp', 'depth': 2}},

        {'_id': 'gjp8ipl', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpgtd5', 'depth': 3}},
        {'_id': 'gjp7ire', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpgtd5', 'depth': 3}},
        {'_id': 'gjpb08s', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpgtd5', 'depth': 3}},

        {'_id': 'gjp8skb', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp8ipl', 'depth': 4}, 'metadata': {'topics': {'direct': ['GME']}}},
        {'_id': 'gjp9wjt', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp8ipl', 'depth': 4}},
        {'_id': 'gjp7q3l', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp8ipl', 'depth': 4}},
        {'_id': 'gjp8gv5', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp7ire', 'depth': 4}},
        {'_id': 'gjpc7xn', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpb08s', 'depth': 4}},
        {'_id': 'gjpccr3', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpb08s', 'depth': 4}},

        {'_id': 'gjpa65i', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp8skb', 'depth': 5}},
        {'_id': 'gjpici3', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp8skb', 'depth': 5}},
        {'_id': 'gjp8ngh', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp8skb', 'depth': 5}},
        {'_id': 'gjpk1yl', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpc7xn', 'depth': 5}, 'metadata': {'topics': {'direct': ['GME']}}},
        {'_id': 'gjq6tjb', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpc7xn', 'depth': 5}},

        {'_id': 'gjpadfy', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpa65i', 'depth': 6}},
        {'_id': 'gjpo23k', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpa65i', 'depth': 6}},
        {'_id': 'gjp8vyt', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjp8ngh', 'depth': 6}},
        {'_id': 'gjplrdk', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpk1yl', 'depth': 6}},
        {'_id': 'gjpo4vc', 'data': {'link_id': 't3_kzppqo', 'parent_id': 't1_gjpo2k0', 'depth': 6}},

    ]


    def recurse(full_sub, topics_accum=set([])):

        # assuming that full_sub list
        # is sorted by data.depth field

        # the recursion part:

        # check if have children if have then
        # add topics to accumulated list [[]]

        # check if have children if have then
        # add topics to accumulated list [[], []]

        # check if have children if not
        # pop last topics to my indirect topics
        # and move up the ladder to parent

        parent_id = full_sub[0]['_id']
        for child_candidate in full_sub[1:]:
            if parent_id in child_candidate['data']['parent_id']:
                print(len(full_sub), parent_id, child_candidate['_id'])

                # check if parent has topics
                if full_sub[0].get('metadata', {}).get('topics', {}).get('direct', None):
                    [topics_accum.add(topic) for topic in full_sub[0]['metadata']['topics']['direct']]
                    print(topics_accum)

        if len(full_sub[1:]) > 1:
            recurse(full_sub[1:], topics_accum)

    [print(i) for i in full_sub]

