import requests


def coinbase_coins():
    r = requests.get('https://api.pro.coinbase.com/currencies')
    coinbase_coins = {}
    for coin in r.json():
        if coin['details']['type'] == 'crypto':
            coinbase_coins[coin['id']] = {coin['id'], coin['name']}
    return coinbase_coins

def manual_coins():
    return {
        'ETH': {'Ethereum', 'ETH'},
        'BTC': {'BTC', 'Bitcoin'},
        'XRP': {'XRP', 'Ripple'},
        'XLM': {'XLM', 'Stellar'},
        'ADA': {'ADA', 'Cardano'},
        'DOGE': {'DOGE', 'Dogecoin'},
        'DOT': {'Polkadot', 'DOT'},
        'NEO': {'Neo', 'NEO'},
        'CEL': {'CEL', 'Celsius'},
        'NANO': {'Nano', 'NANO'},
        'LINK': {'Chainlink', 'LINK'},
        'XMR': {'Monero', 'XMR'},
        'USDT': {'Tether', 'USDT'},
        'LTC': {'LTC', 'Litecoin'},
        'BNB': {'BNB'},
        'NEM': {'XEM', 'NEM'},
        'TRON': {'TRX', 'TRON'},
        'DASH': {'DASH', 'Dash'},
        'ZEC': {'ZEC', 'Zcash'},
        'BTG': {'BTG'},
        'EOS': {'EOS', 'EOSIO'},
        'VET': {'VET', 'VeChain'},
        'DAI': {'MakerDao', 'DAI'},
        'SHIB': {'SHIBA INU', 'SHIB', 'SHIBA'},
        'ICP': {'Internet computer', 'ICP'},
        'IOTA': {'IOTA', 'Miota'},
        'LTO': {'LTO network', 'LTO'},
        'SOL': {'Solana', 'SOL'},
        'MOON': {'Moons', 'MOON'},
        'USDC': {'USD coin', 'USDC'},
        'THETA': {'THETA'},
        'FIL': {'Filecoin', 'FIL'},
        'KSM': {'Kusama', 'KSM'},
        'CAKE': {'PancakeSwap', 'Pancake', 'CAKE'},
        'KLAY': {'Klaytn', 'KLAY'},
        'AMP': {'AMP'},
        'ERG': {'ERG', 'Ergo'},
        'FTM': {'FTM', 'Fantom'},
        'LUNA': {'LUNA', 'Terra'},
        'AVAX': {'AVAX', 'Avalanche'},
        'ONE': {'Harmony'},
        'CKB': {'CKB', 'Nervos'},
        'CRO': {'CRO', 'Cronos'},
        'QNT': {'QNT', 'Quant'},
        'MINA': {'MINA', 'Mina protocol'},
        'OSMO': {'OSMO', 'Osmosis'},
        'JUNO': {'JUNO'},
        'OP': {'Optimism'},
        'NFT': {'NFT', 'non fungible token, non-fungible token'},
        'FTT': {'FTT'},
        'BONK': {'BONK'},
        'T': {'Threshold'},
        'PRO': {'Propy'},
        'PEPE': {'PEPE'},
        'AIDOGE': {'AIDOGE'},
    }


def get_topics() -> dict:
    # in case of duplicate key in dicts
    # latest dict overwrites previous dicts
    return {**coinbase_coins(), **manual_coins()}
