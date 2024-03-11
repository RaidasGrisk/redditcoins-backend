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
        'XMR': {
            'name': ['XMR'],
            'other': ['Monero']
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
        'MOON': {
            'name': ['MOON'],
            'other': ['Moons']
        },
        'THETA': {
            'name': ['THETA'],
            'other': []
        },
        'FIL': {
            'name': ['FIL'],
            'other': ['Filecoin']
        },
        'CAKE': {
            'name': ['CAKE'],
            'other': ['PancakeSwap', 'Pancake']
        },
        'KLAY': {
            'name': ['KLAY'],
            'other': ['Klaytn']
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
        'CONE': {
            'name': ['CONE'],
            'other': ['BitCone']
        },
    }


def get_coins() -> dict:
    # in case of duplicate key in dicts
    # latest dict overwrites previous dicts
    return {**coinbase_coins(), **manual_coins()}
