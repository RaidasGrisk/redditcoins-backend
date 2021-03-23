'''
https://pydantic-docs.helpmanual.io/usage/schema/#schema-customization
'''

from fastapi import FastAPI, Query, Path
from pydantic import BaseModel
from typing import List, Optional

from app.db_to_timeseries import get_timeseries_df
from other_ops.topics import main as get_topics

# ok we need to parse ticker names and validate
# that provided ticker is indeed valid and exists in db
# TODO: this is too long and rendered docs are fucked
valid_tickers = '|'.join(list(get_topics().keys()) + ['None'])

# valid granularity values
# TODO: this is not a complete list but lets keep
#  it short and simple for now. Not sure if this
#  impacts the response speed. Most likely the biggest
#  bottleneck is mongo query agg time. Fix that.
granularities = ['Y', 'M', 'W', 'D', 'H', '6H', '2H']
valid_granularity = '|'.join(granularities)

# valid subreddits
subreddits = ['wallstreetbets']
valid_subreddits = '|'.join(subreddits)


app = FastAPI(
    title='Ticker data from Reddit',
    description='Get ticker mention counts / sentiment from reddit subs',
    version='0.0.1'
)


@app.get('/')
def ping():
    return {'message': 'Hey there!'}


# the output data model example and validator
class DataModelOut(BaseModel):
    data: List[dict] = [
        {'time': '2021-01-01', 'volume': 1},
        {'time': '2021-01-02', 'volume': 2}
    ]


@app.get('/volume/{subreddit}/{ticker}', response_model=DataModelOut)
async def vol(
        ticker: str = Path(
            ...,
            title='ticker',
            description='The name of the ticker e.g. NVDA, TSLA, GME. <br><br>'
                        'There is a special case: when ticker is set to NONE <br>'
                        'total number of submissions / comments is returned <br>'
                        'irrespective of ticker mentions. Useful for data scaling.',
            # regex=f'({valid_tickers})',
            include_in_schema=False
        ),
        subreddit: str = Path(
            'wallstreetbets',
            description='The subreddit to fetch data from',
            regex=f'{valid_subreddits}'
        ),
        start: str = Query(
            ...,
            description='The start of the time range to fetch data for, e.g. 2021-01-01'
        ),
        end: str = Query(
            ...,
            description='The end of the time range to fetch data for, e.g. 2021-02-01'
        ),
        ups: Optional[int] = Query(
            10,
            description='Include subs/comments with more than specified number of ups'
        ),
        submissions: bool = Query(
            True,
            description='Include submissions'
        ),
        comments: bool = Query(
            False,
            description='Include comments'
        ),
        granularity: str = Query(
            'D',
            description='Granularity of the data fetched',
            regex=f'({valid_granularity})',
        )
) -> dict:
    print(subreddit)
    df = await get_timeseries_df(
        # okay, why do we need this NONE thing?
        # the thing is, we want to be able to get the
        # volume of all the tickers combined together.
        # This let us scale the data: NVDA_vol / NONE_vol.
        # Now we can know that NVDA subs account for X%
        # of total subs during a period. This info is way
        # more valuable in ML models than raw counts NVDA_vol.
        ticker=None if ticker == 'NONE' else ticker,
        start=start,
        end=end,
        ups=ups,
        submissions=submissions,
        comments=comments,
        granularity=granularity
    )

    return {
        'data': df.reset_index().to_dict(orient='records')
    }


@app.get('/sentiment/{subreddit}/{ticker}')
async def sentiment():
    pass