# Init mongo db and mongo-express
```
docker-compose -f docker-compose-db.yml up
```
Mongo connection and interface details are stored in ```docker-compose-db.yml```.

# Pull and push data to db

```
python reddit_to_db.py --subreddit satoshistreetbets --start 2021-03-01 --end 2021-04-01 --delta 12
```

Reddit and Mongo credentials are stored inside ```./private.py``` (make this file before running the above command).

```
# https://www.reddit.com/dev/api/
reddit_details = {
    'client_id': 'CLIENT_ID',
    'client_secret': 'CLIENT_SECRET',
    'user_agent': 'my user agent'
}

mongo_details = {
    'host': '0.0.0.0',
    'port': 27017,
    'username': 'admin',
    'password': 'pass',
}
```

# DB doc structure

 ```
{

    // reddit id
    // duplicate of data.id
    '_id': '4g8ads',

    // this part is raw reddit data, must be easily
    // over-writen while refreshing the db so do not 
    // store anything else but raw data here
    'data': {
        'body': 'asd',
        'ups': 5,
        'is_root': True,
        'created_utc': 1610922785,
        ...
    },

    // this part is made by other models
    // sentiment, topic assignment, etc.
    // must not be overwritten by db refresh
    // update this by running expensive 
    // code dependent on the above data
    'metadata': {
        'sentiment': 1,
        'topics': {
            'direct': ['AMC'],
            'indirect: ['BTC']
        },
        ...
    }
}
```

# Update reddit topics and sentiment
```
python other_ops/update_direct_topics.py
python other_ops/update_sentiment.py
```

# Build and deploy API

Build and run FastAPI server in docker container
```

docker build --tag api -f Dockerfile-api .
docker run -d -p 80:80 api

```
