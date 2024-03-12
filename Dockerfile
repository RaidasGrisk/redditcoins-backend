# docker build -t eu.gcr.io/reddit-app-308612/backend .
# docker push eu.gcr.io/reddit-app-308612/backend
# docker pull eu.gcr.io/reddit-app-308612/backend
# docker run -d eu.gcr.io/reddit-app-308612/backend

FROM ubuntu:latest

WORKDIR /app

COPY private.py /app
COPY reddit_to_db.py /app
COPY /other_ops /app/other_ops

# install dependencies
RUN apt-get update && apt-get install -y python3-pip
RUN apt-get install -y python3-dev libpq-dev
RUN pip install psycopg psycopg2-binary asyncpg asyncpraw aiostream

# setup chron
RUN apt-get install -y cron
COPY crontab /etc/cron.d/crontab
RUN chmod 0644 /etc/cron.d/crontab
RUN crontab /etc/cron.d/crontab

CMD python3 -u reddit_to_db.py & cron -f
