'''
https://pydantic-docs.helpmanual.io/usage/schema/#schema-customization
'''

from fastapi import FastAPI, Query, Path
from pydantic import BaseModel
from typing import List, Optional

from app.db_to_timeseries import get_timeseries_df
from other_ops.topics import topics

# ok we need to parse ticker names and validate
# that provided ticker is indeed valid and exists in db
# TODO: this is too long and rendered docs are fucked
valid_tickers = '|'.join(topics.keys())

# valid granularity values
# TODO: this is not a complete list but lets keep
#  it short and simple for now. Not sure if this
#  impacts the response speed. Most likely the biggest
#  bottleneck is mongo query agg time. Fix that.
granularities = ['Y', 'M', 'W', 'D', 'H', '6H', '2H']
valid_granularity = '|'.join(granularities)


app = FastAPI()


@app.get('/')
def ping():
    return {'message': 'Hey there!'}


# the output data model example and validator
class DataModelOut(BaseModel):
    data: List[dict] = [
        {'time': '2021-01-01', 'volume': 1},
        {'time': '2021-01-02', 'volume': 2}
    ]


@app.get('/{ticker}', response_model=DataModelOut)
async def timeseries(
        ticker: str = Path(
            ...,
            title='ticker',
            description='The name of the ticker e.g. NVDA, TSLA, GME',
            # regex=f'({valid_tickers})',
            include_in_schema=False
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

    df = await get_timeseries_df(
        start=start,
        end=end,
        ticker=ticker,
        ups=ups,
        submissions=submissions,
        comments=comments,
        granularity=granularity
    )

    return {
        'data': df.reset_index().to_dict(orient='records')
    }