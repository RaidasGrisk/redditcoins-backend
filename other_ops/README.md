# Update topics

```
# stock sub/topics
python other_ops/update_topics.py \
    --subreddit wallstreetbets \
    --topics_type stock \
    --start 2021-03-01 \
    --end 2021-04-01
   
# crypto sub/topics
python other_ops/update_topics.py \
    --subreddit satoshistreetbets \
    --topics_type crypto \
    --start 2021-03-01 \
    --end 2021-04-01
```

```
python3 /home/mrraidas/reddit-to-db/reddit_to_db.py --subreddit satoshistreetbets --limit 1000 >> /home/mrraidas/logs/reddit_to_db.log 2>&1 && cd /home/mrraidas/reddit-to-db/ && python3 /home/mrraidas/reddit-to-db/other_ops/update_topics.py --subreddit satoshistreetbets --topics_type crypto --start 2021-04-12 >> /home/mrraidas/logs/update_topics.log 2>&1
```