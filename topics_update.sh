#!/bin/bash

# this script is added to crontab
# and is ran every x minutes
# before deploying this make sure
# that ROOT_DIR is correct on deploy machine

ROOT_DIR="/home/mrraidas/"
#ROOT_DIR="/home/raidas/Desktop"
SUB_DIR=$ROOT_DIR"/reddit-to-db"
LOGS_DIR=$ROOT_DIR"/logs"

cd $SUB_DIR
DATE=$(date -d "30 minutes ago" '+%Y-%m-%d %H:%M:%S')

python3 other_ops/update_topics.py \
--subreddit $subreddit \
--start $DATE \
>> $LOGS_DIR"/update_topics.log" 2>&1
