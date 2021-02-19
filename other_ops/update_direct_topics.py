from private import mongo_details
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


async def update_topics() -> None:

    # db connection
    db_client = motor.motor_asyncio.AsyncIOMotorClient(**mongo_details)
    total_updates = 0
    total_docs = 0

    # move through all the docs and do the thing
    cur = db_client.reddit.data.find({})
    for doc in await cur.to_list(None):

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
            _ = await db_client.reddit.data.update_one(
                {'_id': doc['_id']},
                {'$set': {'metadata': doc['metadata']}}
            )
            total_updates += 1

        total_docs += 1
    print(f'total docs: {total_docs} total updates: {total_updates}')


if __name__ == '__main__':
    asyncio.run(update_topics())