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