# Basic structure

| Part  | Repo  |
|---|---|
| DB | this repo  |
| BACKEND | this repo  |
| API | [redditcoins-api](https://github.com/RaidasGrisk/reddit-coin-app)  |
| WEB | [redditcoins-web](https://github.com/RaidasGrisk/reddit-coin-web)  |

![](https://i.imgur.com/nNZYXje.png)

```
+---------+-----------------------------------------+
| DB      | Postgress instance on google cloud      |
| BACKEND | Small VM on google cloud                |
| API     | Serverless RUN instance on google cloud |
| WEB     | Vuejs deployed on Vercel                |
+---------+-----------------------------------------+
```

# DB structure

```
DB tables:
+-----------------+------------------------------------------+
| table           | description                              |
+-----------------+------------------------------------------+
| cryptocurrency  | raw comment data                         |
| cryptocurrency_ | mention data                             |
| daily_data      | daily summary of mentions (db cron job)  |
| hourly_data     | hourly summary of mentions (db cron job) |
+-----------------+------------------------------------------+

table cryptocurrency
raw reddit data

+-----------+-------------+---------------+--------------------------------+
|    _id    | created_utc |     title     |              body              |
+-----------+-------------+---------------+--------------------------------+
| "glbpa8l" |  1611974403 |               | "Waiting for testimony"        |
| "glb60wl" |  1611964950 |               | "Let’s do it people!!!!"       |
| "glbnofh" |  1611973612 |               | "Idc about 1$ I want 10 cents" |
| "glb7ds2" |  1611965600 |               | "EXACTLYYYYYY"                 |
| "glb035r" |  1611962228 |               | "Doge is the way"              |
| "glbudtv" |  1611976898 |               | "How long will that take?"     |
| "glbpqkg" |  1611974626 |               | "Bruh"                         |
| "glb2bj9" |  1611963233 |               | "virgins."                     |
| "glbu97h" |  1611976835 |               | "🤚"                           |
| "l8hfq0"  |  1611995690 | "r/dogecoin?" |                                |
+-----------+-------------+---------------+--------------------------------+


table cryptocurrency_
coin mentions
+---------------+-------------+------------+-------+
|      _id      | created_utc | is_comment | topic |
+---------------+-------------+------------+-------+
| "gp8o9ms_XRP" |  1611974403 | true       | "XRP" |
| "gp8o9ms_XLM" |  1611964950 | true       | "XLM" |
| "gp8ovit_ADA" |  1611964950 | true       | "ADA" |
| "gp8ps64_ADA" |  1611964950 | true       | "ADA" |
| "gp8s1b9_ADA" |  1611964950 | true       | "ADA" |
| "gp8tfyl_ADA" |  1611964950 | true       | "ADA" |
| "gp8u28x_DNT" |  1611964950 | true       | "DNT" |
| "gp8wxuk_ADA" |  1611964950 | true       | "ADA" |
| "gp90hov_BTC" |  1611964950 | true       | "BTC" |
+---------------+-------------+------------+-------+

```

# Init local postgredb and interface
```
docker-compose up
python init_db.py  # better do manually
```

Credentials are stored inside ```./private.py``` (make this file before running the above command).

```
reddit_details = {
    'client_id': 'CLIENT_ID',
    'client_secret': 'CLIENT_SECRET',
    'user_agent': 'my user agent'
}

db_details = {
    'host': '0.0.0.0',
    'port': 27017,
    'user': 'admin',
    'password': 'pass',
}
```

# Production

Run the ```Dockerfile``` that does everything (except, create the db).

# TODOs

- [x] set up new db
- [x] setup VM with data stream and push to new DB
- [x] migrate old data to new db: pg_dump -U <USERNAME> -h <IP ADDRESS> -p <PORT> -n <TABLE> -d <DATABASE> > db_dump.sql
- [x] create db jobs (daily / hourly / web_data)
- [x] create API
- [x] vercel website
