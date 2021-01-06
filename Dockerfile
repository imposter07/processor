FROM ubuntu:latest

RUN apt-get update
RUN apt-get install -y locales locales-all
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8

RUN adduser lqapp

WORKDIR /home/lqapp

COPY requirements.txt requirements.txt

RUN apt-get update
RUN apt-get install -y software-properties-common
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get install python3.7 -y
RUN apt-get install -y git
RUN apt-get install python3.7-venv -y
RUN python3.7 -m venv venv
RUN venv/bin/pip install -r requirements.txt
RUN venv/bin/pip install gunicorn

COPY app app
COPY migrations migrations
COPY processor processor
COPY uploader uploader
COPY app/config.json app/config.json
COPY main.py config.py boot.sh ./
RUN chmod a+x boot.sh

ENV FLASK_APP main.py

RUN chown -R lqapp:lqapp ./
USER lqapp

EXPOSE 5000
ENTRYPOINT ["./boot.sh"]