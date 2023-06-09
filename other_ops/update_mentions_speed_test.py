"""
TODO:
 1. improve regex logic (maybe with a trie). Do in single pass.
 2. current speed bottleneck is due to db conn update_one fn.
    consider update_many
"""

# to be able to launch this from terminal
from other_ops.topics import get_topics
from other_ops.update_topics import get_regex_pattern
import timeit

topics = get_topics()

coin_names = []
for coin in topics.values():
    coin_names.extend(coin['name'])

coin_text = []
for coin in topics.values():
    coin_text.extend(coin['other'])

regex_pattern = get_regex_pattern(coin_names, coin_text)
topics_re_compiled = {topic: get_regex_pattern(values['name'], values['other']) for topic, values in topics.items()}

doc_texts = [
    'But is ETH 100% decetralized? i might be mistaken so correct me if i am, but i heard its mostly on same servers',
    'Litmus test: What can the new Eth, MATIC, BNB, AVAX, FLOW, ',
    'Soon ai will generate porn with keywords',
    'BTC is to crypto what USD is (was?) to the worl',
    'I believe that MATIC can survive bear markets. But sometimes Crypto might surprise you. Doge is a sh',
] + ['asd asd asd asd asd asd asd asd asd asd'] * 100

for doc_text in doc_texts:
    if regex_pattern.search(doc_text):
        for topic_key, topic_re_pattern in topics_re_compiled.items():
            if topic_re_pattern.search(doc_text):
                continue


# ---------------------- #


SETUP_CODE = '''
from other_ops.topics import get_topics
from other_ops.update_topics import get_regex_pattern
'''

TEST_CODE = '''
topics = get_topics()

coin_names = []
for coin in topics.values():
    coin_names.extend(coin['name'])

coin_text = []
for coin in topics.values():
    coin_text.extend(coin['other'])

regex_pattern = get_regex_pattern(coin_names, coin_text)
topics_re_compiled = {topic: get_regex_pattern(values['name'], values['other']) for topic, values in topics.items()}

doc_texts = [
    'But is ETH 100% decetralized? i might be mistaken so correct me if i am, but i heard its mostly on same servers',
    'Litmus test: What can the new Eth, MATIC, BNB, AVAX, FLOW, ',
    'Soon ai will generate porn with keywords',
    'BTC is to crypto what USD is (was?) to the worl',
    'I believe that MATIC can survive bear markets. But sometimes Crypto might surprise you. Doge is a sh',
] + ['asd asd asd asd asd asd asd asd asd asd'] * 1000

for doc_text in doc_texts:
    if regex_pattern.search(doc_text):
        for topic_key, topic_re_pattern in topics_re_compiled.items():
            if topic_re_pattern.search(doc_text):
                continue
'''

# timeit.repeat statement
times = timeit.timeit(
    setup=SETUP_CODE,
    stmt=TEST_CODE,
    number=10
)

# printing minimum exec. time
print(times)
