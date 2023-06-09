"""
Posts a comment to the daily with a table of most mentioned coins
In prod this is added to a crantab
10 2,8,14,20 * * * cd /app && python3 other_ops/post_bot.py
"""

# to be able to launch this from terminal and crontab
import sys
import os
sys.path.append(os.getcwd())

import asyncio
import asyncpraw
from private import reddit_login
import requests
from datetime import datetime, timedelta


def is_data_correct(data):
    """
    In case something goes wrong, check if the response is valid.
    Issues had happened before, lets not spam reddit with gibberish.
    """

    # check if popular coins are in the data
    coins_to_check = ['BTC', 'ETH', 'ADA']
    if not all(coin in data['cryptocurrency'].keys() for coin in coins_to_check):
        return False
    
    # check if volume is not too low
    btc_vol = data['cryptocurrency']['BTC']['data'][0]['volume']
    eth_vol = data['cryptocurrency']['ETH']['data'][0]['volume']
    if btc_vol < 80 or eth_vol < 30:
        return False
    
    # check if date is correct
    # the date should be today - 1 day.
    data_date = data['cryptocurrency']['BTC']['data'][0]['time']
    correct_date = datetime.utcnow() - timedelta(days=1)
    if correct_date.strftime('%Y-%m-%d') != data_date:
        return False

    # if none of the above ifs hit than return true
    return True


def make_comment():

    # get current data
    url = 'https://redditcoins.app/api/volume/market_summary?gran=daily'
    resp = requests.get(url)
    data = resp.json()

    # check if returned data is correct
    if not is_data_correct(data):
        print('something is wrong with the data')
        return False

    # parse data into df
    data_dict = {
        coin: data['cryptocurrency'][coin]['data'][0]['volume']
        for coin in data['cryptocurrency']
    }
    sorted_coins = sorted(data_dict, key=data_dict.get, reverse=True)

    # parse data into string (comment)
    # messy due to \n and reddit markdown
    # '  \n' makes new line
    # '\n\n' makes new paragraph
    current_date = data['cryptocurrency']['BTC']['data'][0]['time']
    comment = (
        f'''Most mentions on r/cc ({current_date}):\n\n'''
        '''||Mentions|  \n'''
        '''|:-|:-|  \n'''
    )

    for coin in sorted_coins[:20]:
        comment += f'|{coin}|{str(data_dict[coin])}|  \n'

    # for the final line lets add the source and link to the app
    comment += '\n[Data source and app](https://www.redditcoins.app/)'

    return comment


async def post_comment_on_reddit(comment):
    if comment:
        reddit = asyncpraw.Reddit(**reddit_login)
        subreddit = await reddit.subreddit('cryptocurrency')
        async with reddit:
            async for submission in subreddit.hot(limit=50):
                # find the submission we want. Not very convenient,
                # but can't quickly find the patter I should be using
                # so lets just look for partial title match
                if 'Daily General Discussion' in submission.title:
                    await submission.reply(comment)
                    return

if __name__ == "__main__":
    asyncio.run(
        post_comment_on_reddit(make_comment())
    )
