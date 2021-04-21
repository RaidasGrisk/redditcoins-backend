#!/bin/bash

# this script is added to crontab
# and is ran every 10 minutes
# before deploying this make sure
# that ROOT_DIR is correct on deploy machine

#ROOT_DIR="/home/mrraidas/"
ROOT_DIR="/home/raidas/Desktop"
SUB_DIR=$ROOT_DIR"/reddit-to-db"
LOGS_DIR=$ROOT_DIR"/logs"

cd $SUB_DIR
DATE=$(date -d "1 days ago" '+%Y-%m-%d')

for subreddit in satoshistreetbets cryptocurrency;
do
  python3 reddit_to_db.py \
  --subreddit $subreddit \
  --limit 1000 \
  >> $LOGS_DIR"/reddit_to_db.log" 2>&1 &&

  python3 other_ops/update_topics.py \
  --subreddit $subreddit \
  --start $DATE \
  >> $LOGS_DIR"/update_topics.log" 2>&1
done
