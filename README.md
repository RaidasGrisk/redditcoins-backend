# Init postgredb and interface
```
docker-compose -f docker-compose.yml up
python init_db.py
```
Db connection and interface details are stored in ```docker-compose.yml```.

# Pull and push data to db

```
# historical data
python reddit_to_db.py \
    --subreddit satoshistreetbets \
    --start 2021-03-01 \
    --end 2021-04-01 \
    --delta 12

# new data
python reddit_to_db.py \
    --subreddit satoshistreetbets \
    --limit 100
    
# data stream
python reddit_to_db_2.py \
    --subreddit satoshistreetbets \
    --limit 100
```

Reddit and db credentials are stored inside ```./private.py``` (make this file before running the above command).

```
# https://www.reddit.com/dev/api/
reddit_details = {
    'client_id': 'CLIENT_ID',
    'client_secret': 'CLIENT_SECRET',
    'user_agent': 'my user agent'
}

db_details = {
    'host': '0.0.0.0',
    'port': 27017,
    'user': 'admin',
    'password': 'temp-pass',
}
```

# DB doc structure

 ```
table {subreddit}
raw reddit data

+-----------+-------------+-----+--------------+---------------+--------------------------------+
|    _id    | created_utc | ups | num_comments |     title     |              body              |
+-----------+-------------+-----+--------------+---------------+--------------------------------+
| "glbpa8l" |  1611974403 |   1 |              |               | "Waiting for testimony"        |
| "glb60wl" |  1611964950 |   2 |              |               | "Let’s do it people!!!!"       |
| "glbnofh" |  1611973612 |   3 |              |               | "Idc about 1$ I want 10 cents" |
| "glb7ds2" |  1611965600 |   1 |              |               | "EXACTLYYYYYY"                 |
| "glb035r" |  1611962228 |   1 |              |               | "Doge is the way"              |
| "glbudtv" |  1611976898 |   2 |              |               | "How long will that take?"     |
| "glbpqkg" |  1611974626 |   2 |              |               | "Bruh"                         |
| "glb2bj9" |  1611963233 |   1 |              |               | "virgins."                     |
| "glbu97h" |  1611976835 |   8 |              |               | "🤚"                           |
| "l8hfq0"  |  1611995690 | 206 |           22 | "r/dogecoin?" |                                |
+-----------+-------------+-----+--------------+---------------+--------------------------------+


table {subreddit_}
topic (ticker) mentions
+---------------+-----------------------+-----+------------+-------+
|      _id      |     created_time      | ups | is_comment | topic |
+---------------+-----------------------+-----+------------+-------+
| "gp8o9ms_XRP" | "2021-02-28 23:11:05" |   1 | true       | "XRP" |
| "gp8o9ms_XLM" | "2021-02-28 23:11:05" |   1 | true       | "XLM" |
| "gp8ovit_ADA" | "2021-02-28 23:15:53" |   1 | true       | "ADA" |
| "gp8ps64_ADA" | "2021-02-28 23:22:55" |   1 | true       | "ADA" |
| "gp8s1b9_ADA" | "2021-02-28 23:41:16" |   2 | true       | "ADA" |
| "gp8tfyl_ADA" | "2021-02-28 23:53:01" |   1 | true       | "ADA" |
| "gp8u28x_DNT" | "2021-02-28 23:58:35" |   1 | true       | "DNT" |
| "gp8wxuk_ADA" | "2021-03-01 00:24:54" |   2 | true       | "ADA" |
| "gp90hov_BTC" | "2021-03-01 00:58:31" |   0 | true       | "BTC" |
+---------------+-----------------------+-----+------------+-------+

```

# To start the whole thing do this
Start this script to capture the stream and push it to db (this runs forever)
```
python3 -u reddit_to_db_2.py --subreddit cryptocurrency
```

Add this to crontab to update the topics every x mins
```
*/10 * * * * /home/mrraidas/reddit-to-db/topics_update.sh
```