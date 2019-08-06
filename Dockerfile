FROM ubuntu:latest

RUN adduser lqapp

WORKDIR /home/lqapp

COPY requirements.txt requirements.txt

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install python3.6
RUN apt-get install -y git
RUN apt-get install python3-venv -y
RUN python3.6 -m venv venv
RUN venv/bin/pip install -r requirements.txt
RUN venv/bin/pip install gunicorn pymysql

COPY app app
COPY migrations migrations
COPY app/config.json app/config.json
COPY main.py config.py boot.sh ./
RUN chmod a+x boot.sh

ENV FLASK_APP main.py

RUN chown -R lqapp:lqapp ./
USER lqapp

EXPOSE 5000
ENTRYPOINT ["./boot.sh"]