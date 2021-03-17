'''
https://github.com/mjhea0/awesome-fastapi#boilerplate
'''

from fastapi import FastAPI

from db_to_timeseries import get_timeseries_df

app = FastAPI()


@app.get("/")
def ping():
    return {'Hi': '!'}


@app.get("/{ticker}")
def read_item(
        start: str,
        end: str,
        ticker: str,
        ups: int,
        submissions: bool,
        comments: bool,
        granularity: str
) -> object:

    df = get_timeseries_df(
        start=start,
        end=end,
        ticker=ticker,
        ups=ups,
        submissions=submissions,
        comments=comments,
        granularity=granularity
    )

    # convert to str, else to_json will convert it to ts
    df.index = df.index.astype(str)

    return df.to_json()