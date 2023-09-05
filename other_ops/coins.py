import requests


def coinbase_coins():
    r = requests.get('https://api.pro.coinbase.com/currencies')
    coinbase_coins = {}
    for coin in r.json():
        if coin['details']['type'] == 'crypto':
            coinbase_coins[coin['id']] = {
                'name': [coin['id']],
                'other': [coin['name']]
            }
    return coinbase_coins


def manual_coins():
    return {
        'ETH': {
            'name': ['ETH'],
            'other': ['Ethereum']
        },
        'BTC': {
            'name': ['BTC'],
            'other': ['Bitcoin']
        },
        'XRP': {
            'name': ['XRP'],
            'other': ['Ripple']
        },
        'XLM': {
            'name': ['XLM'],
            'other': ['Stellar']
        },
        'ADA': {
            'name': ['ADA'],
            'other': ['Cardano']
        },
        'DOGE': {
            'name': ['DOGE'],
            'other': ['Dogecoin']
        },
        'DOT': {
            'name': ['DOT'],
            'other': ['Polkadot']
        },
        'NEO': {
            'name': ['NEO'],
            'other': ['Neo']
        },
        'CEL': {
            'name': ['CEL'],
            'other': ['Celsius']
        },
        'NANO': {
            'name': ['NANO'],
            'other': ['Nano']
        },
        'LINK': {
            'name': ['LINK'],
            'other': ['Chainlink']
        },
        'XMR': {
            'name': ['XMR'],
            'other': ['Monero']
        },
        'USDT': {
            'name': ['USDT'],
            'other': ['Tether']
        },
        'LTC': {
            'name': ['LTC'],
            'other': ['Litecoin']
        },
        'BNB': {
            'name': ['BNB'],
            'other': []
        },
        'NEM': {
            'name': ['XEM', 'NEM'],
            'other': []
        },
        'TRON': {
            'name': ['TRX', 'TRON'],
            'other': []
        },
        'DASH': {
            'name': ['DASH'],
            'other': ['Dash']
        },
        'ZEC': {
            'name': ['ZEC'],
            'other': ['Zcash']
        },
        'BTG': {
            'name': ['BTG'],
            'other': []
        },
        'EOS': {
            'name': ['EOS', 'EOSIO'],
            'other': []
        },
        'VET': {
            'name': ['VET'],
            'other': ['VeChain']
        },
        'DAI': {
            'name': ['DAI'],
            'other': ['MakerDao']
        },
        'SHIB': {
            'name': ['SHIBA INU', 'SHIB', 'SHIBA'],
            'other': []
        },
        'ICP': {
            'name': ['ICP'],
            'other': ['Internet computer']
        },
        'IOTA': {
            'name': ['IOTA'],
            'other': ['Miota']
        },
        'LTO': {
            'name': ['LTO'],
            'other': ['LTO network']
        },
        'SOL': {
            'name': ['SOL'],
            'other': ['Solana']
        },
        'MOON': {
            'name': ['MOON'],
            'other': ['Moons']
        },
        'USDC': {
            'name': ['USDC'],
            'other': ['USD coin']
        },
        'THETA': {
            'name': ['THETA'],
            'other': []
        },
        'FIL': {
            'name': ['FIL'],
            'other': ['Filecoin']
        },
        'KSM': {
            'name': ['KSM'],
            'other': ['Kusama']
        },
        'CAKE': {
            'name': ['CAKE'],
            'other': ['PancakeSwap', 'Pancake']
        },
        'KLAY': {
            'name': ['KLAY'],
            'other': ['Klaytn']
        },
        'AMP': {
            'name': ['AMP'],
            'other': []
        },
        'ERG': {
            'name': ['ERG'],
            'other': ['Ergo']
        },
        'FTM': {
            'name': ['FTM'],
            'other': ['Fantom']
        },
        'LUNA': {
            'name': ['LUNA'],
            'other': ['Terra']
        },
        'AVAX': {
            'name': ['AVAX'],
            'other': ['Avalanche']
        },
        'ONE': {
            'name': [],
            'other': ['Harmony']
        },
        'CKB': {
            'name': ['CKB'],
            'other': ['Nervos']
        },
        'CRO': {
            'name': ['CRO'],
            'other': ['Cronos']
        },
        'QNT': {
            'name': ['QNT'],
            'other': ['Quant']
        },
        'MINA': {
            'name': ['MINA'],
            'other': ['Mina protocol']
        },
        'OSMO': {
            'name': ['OSMO'],
            'other': ['Osmosis']
        },
        'JUNO': {
            'name': ['JUNO'],
            'other': []
        },
        'OP': {
            'name': [],
            'other': ['Optimism']
        },
        # 'NFT': {
        #     'name': ['NFT'],
        #     'other': ['non fungible token, non-fungible token']
        # },
        'FTT': {
            'name': ['FTT'],
            'other': []
        },
        'BONK': {
            'name': ['BONK'],
            'other': []
        },
        'T': {
            'name': [],
            'other': ['Threshold']
        },
        'PRO': {
            'name': [],
            'other': ['Propy']
        },
        'PEPE': {
            'name': ['PEPE'],
            'other': []
        },
        'AIDOGE': {
            'name': ['AIDOGE'],
            'other': []
        },
        'BRICK': {
            'name': ['BRICK', 'BRICKS'],
            'other': []
        },
        'BAN': {
            'name': ['BAN'],
            'other': ['Banano']
        },
    }


def get_coins() -> dict:
    # in case of duplicate key in dicts
    # latest dict overwrites previous dicts
    return {**coinbase_coins(), **manual_coins()}
