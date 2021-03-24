import pandas as pd


# TODO: function naming and cleaning logic below suck.
# TODO: why only nasdaq, should also include NYSE and others?
def nasdaq_topics() -> dict:

    def get_nasdaq_file() -> pd.DataFrame:
        url = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'
        tickers = pd.read_csv(url, sep='|')
        # exclude last line as it contains timestamp
        return tickers[:-1]

    file = get_nasdaq_file()

    topics = {}
    for ticker in file['Symbol'].tolist():
        topics[ticker] = {ticker.upper()}

    return topics


def clean_bad_tickers_and_words(topics: dict) -> dict:
    # remove some tickers because im not sure how
    # to fix them :D these create weird cases that
    # are hard to solve and require unique exceptions
    tickers_to_rm = ['VS', 'UK', 'SO', 'PS', 'PI',
                     'ON', 'HA', 'GO', 'EH', 'CD', 'Z']

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


def get_stock_topics() -> dict:

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

    topics_nasdaq = nasdaq_topics()
    topics_nasdaq = clean_bad_tickers_and_words(topics_nasdaq)

    # do topics | topics_nasdaq for 3.9 py
    return {**topics, **topics_nasdaq}


def get_crypto_topics() -> dict:
    from other_ops.crypto_tickers import crypto_tickers

    # some keys in the dict contain weird
    # chars which collide with regex logic
    # clean it up before returning
    for key in crypto_tickers.copy().keys():
        if not key.isalnum():
            clean_key = ''.join(filter(str.isalnum, key))
            crypto_tickers[clean_key] = crypto_tickers.pop(key)

    return {key: {key} for key in crypto_tickers.keys()}


def get_topics(topics_type: str = 'stock') -> dict:

    # topics is either stock or crypto
    if topics_type == 'stock':
        return get_stock_topics()
    elif topics_type == 'crypto':
        return get_crypto_topics()