"""
Posts a comment to the daily with
a table of most mentioned coins

Add this to crontab to run every x hours.
0 */2 * * * python3 /home/mrraidas/reddit-to-db/other_ops/post_bot.py
"""

# to be able to launch this from terminal and crontab
import sys, os
sys.path.append(os.getcwd())

import asyncio
import asyncpraw
from private import reddit_login
import requests
import pandas as pd
from datetime import datetime, timedelta


def is_data_correct(data):
    """
    check if the response is valid
    and if should continue to make the post.
    This is just in case something goes wrong.
    So we do not spam reddit with gibberish.
    """

    # check if popular coins are in the data
    coins_to_check = ['BTC', 'ETH', 'ADA']
    if not all(coin in data['cryptocurrency'].keys() for coin in coins_to_check):
        return False
    
    # check if volume is not to low
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

    # none of the above ifs hit than return true
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
    coin_data = pd.Series(data_dict).sort_values(ascending=False)[:20]

    # parse df into string (comment)
    # messy due to \n and reddit markdown
    # '  \n' makes new line
    # '\n\n' makes new paragraph
    current_date = data['cryptocurrency']['BTC']['data'][0]['time']
    comment = (
        f'''Most coin mentions on r/cc ({current_date}):\n\n'''
        '''|Coin|Mentions|  \n'''
        '''|:-|:-|  \n'''
    )

    for coin, value in coin_data.iteritems():
        comment += f'|{coin}|{str(value)}|  \n'

    # for the final line lets add the source and link to the app
    comment += '\n[Data source and app](https://www.redditcoins.app/)'

    return comment


async def main(comment):
    if comment:
        reddit = asyncpraw.Reddit(**reddit_login)
        subreddit = await reddit.subreddit('cryptocurrency')
        async with reddit:
            async for submission in subreddit.hot(limit=20):
                # find the submission we want. Not very convenient,
                # but can't quickly find the patter I should be using
                # so lets just look for partial title match
                if 'Daily General Discussion' in submission.title:
                    await submission.reply(comment)
                    return

if __name__ == "__main__":
    asyncio.run(
        main(make_comment())
    )
