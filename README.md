# Init mongo db and mongo-express
```
docker-compose up
```
db connection and interface details are stored in ```docker-compose.yml``` and ```private.py```


# Pull and push data to db
```
python push_to_db.py
```

# Update reddit topics and sentiment
```
python update_db_topics.py
python update_db_sentiment.py
```

# DB doc structure

 ```
{

    // reddit id
    // duplicate of data.id
    '_id': '4g8ads',

    // this part is raw reddit data, can be easily
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
    // update this only by running code on 
    // above data part
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