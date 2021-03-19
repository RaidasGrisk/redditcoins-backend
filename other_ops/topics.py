import pandas as pd
import re


# TODO: function naming and cleaning logic below suck.
# TODO: why only nasdaq, should also include NYSE and others?
def nasdaq_topics() -> dict:

    def get_nasdaq_file() -> pd.DataFrame:
        url = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
        tickers = pd.read_csv(url, sep='|')
        # exclude last line as it contains timestamp
        return tickers[:-1]

    file = get_nasdaq_file()

    def clean_name(name):
        name = name.split('-')[0]
        name = name.replace('Company', '')
        name = name.replace('Inc', '')
        name = name.replace('Corporation', '')
        name = name.replace('Corp', '')
        name = name.replace('Ltd', '')
        name = name.replace('N.A.', '')
        name = name.replace('A/S', '')
        name = name.replace('A/S', '')
        name = name.replace('A/S', '')
        name = name.replace('A/S', '')
        name = name.replace('A/S', '')
        name = name.replace('Group', '')
        name = re.sub(r'\W+', ' ', name)
        name = name.strip()
        return name

    file['Security Name'] = file['Security Name'].apply(lambda x: clean_name(x))

    topics = {}
    for ticker, name in zip(file['Symbol'].tolist(), file['Security Name']):
        topics[ticker] = {ticker.upper(), name.lower()}

    return topics


def clean_bad_tickers_and_words(topics: dict) -> dict:
    # remove some tickers because im not sure how
    # to fix them :D these create weird cases that
    # are hard to solve and require unique exceptions
    tickers_to_rm = ['VS', 'UK', 'SO', 'PS', 'PI',
                     'ON', 'HA', 'GO', 'EH', 'CD']

    texts_to_rm = ['on', '']

    for ticker in tickers_to_rm:
        topics.pop(ticker, None)

    # remove strings that are too often found
    # in random text and thus matches with
    # every doc as a legit topic.
    for ticker, texts in topics.items():
        for text in texts.copy():
            # remove if text we are searching
            # for is single character or in exceptions
            if len(text) == 1 or text in texts_to_rm:
                topics[ticker].remove(text)

    return topics


def main() -> dict:

    # keys are topic names/tickers to be saved in docs
    # values are strings to be searched for in text fields of docs
    # IMPORTANT: make ticker values upper case as would be expected in text
    topics = {

        # general topics
        'SPY': {'SPY'},
        'DOGE': {'DOGE, dogecoin'},
        'CRYPTO': {'coin', 'cypto', 'mining'},

        # some are not in nasdaq
        'GME': {'GME', 'gamestop', 'gamestonk'},
        'PLTR': {'PLTR', 'palantir'},

        # test stocks
        'GOOGL': {'GOOGL'},
        'AMZN': {'AMZN'},
        'AAPL': {'AAPL'},
        'NFLX': {'NFLX'},
        'MSFT': {'MSFT'},
        'TSLA': {'TSLA'},
        'NVDA': {'NVDA'},
        'TECH': {'TECH'},
        'INTC': {'INTC'},
        'BABA': {'BABA'},
        'PYPL': {'PYPL'},
        'CSCO': {'CSCO'},
        'MTCH': {'MTCH'},
        'ADBE': {'ADBE'},
        'DBX': {'DBX'},
        'QQQ': {'QQQ'},
        'CRM': {'CRM'}

    }

    topics_nasdaq = {} #nasdaq_topics()
    topics_nasdaq = clean_bad_tickers_and_words(topics_nasdaq)

    # do topics | topics_nasdaq for 3.9 py
    return {**topics, **topics_nasdaq}


if __name__ == '__main__':
    main()
