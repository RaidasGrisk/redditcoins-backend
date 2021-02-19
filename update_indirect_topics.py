from private import mongo_details
from pymongo import MongoClient
import motor.motor_asyncio
import asyncio


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

    # pull whole submission
    # link_id - submission id
    # parent_id - parent comment
    for sub in sub_cursor:

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

        # at this point full submission
        # is stored in a list. Now the hard part.


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

    def has_children(item, full_sub):
        # assuming full_sub is sorted by depth
        # iter thought docs below and check if
        # current item _id is in any parent_id
        for _, item_ in enumerate(full_sub):
            if item_['data']['parent_id'][3:] == item['_id']:
                return True
        return False

    def get_topics(item):
        # this is badly implemented :/
        topics = []
        if item.get('metadata', None):
            if item['metadata'].get('topics', None):
                if item['metadata']['topics'].get('direct', None):
                    topics.append(item['metadata']['topics']['direct'])
                if item['metadata']['topics'].get('indirect', None):
                    topics.append(item['metadata']['topics']['indirect'])
            return topics[0]

    def recurse():

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

        topic_tree = []
        for index, item in enumerate(full_sub):

            # to increase speed check only items
            # below the current one, like this: full_sub[index + 1:]
            if has_children(item, full_sub[index + 1:]):
                topics = get_topics(item)
                if topics:
                    topic_tree.append(topics)
                    print(item['_id'], 'current topics', topic_tree)

            # does not have children
            # how do we move back the same track to root?
            else:
                print(item['_id'], topic_tree.pop())
