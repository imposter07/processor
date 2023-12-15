FROM ubuntu:latest

# Set environment variables
ENV LC_ALL en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LANGUAGE en_US.UTF-8
ENV FLASK_APP main.py
ENV DEBIAN_FRONTEND=noninteractive

# Install basic packages
RUN apt-get update && \
    apt-get install -y locales locales-all software-properties-common git && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add deadsnakes PPA and install Python
RUN add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y python3.8 python3.8-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create and switch to the lqapp user
RUN adduser lqapp
USER lqapp
WORKDIR /home/lqapp

# Copy and install requirements
COPY --chown=lqapp:lqapp requirements.txt .
RUN python3.8 -m venv venv && \
    venv/bin/pip install -r requirements.txt && \
    venv/bin/pip install gunicorn

# Copy the rest of the application
COPY --chown=lqapp:lqapp app app
COPY --chown=lqapp:lqapp migrations migrations
COPY --chown=lqapp:lqapp processor processor
COPY --chown=lqapp:lqapp uploader uploader
COPY --chown=lqapp:lqapp app/config.json app/config.json
COPY --chown=lqapp:lqapp main.py config.py boot.sh ./

EXPOSE 5000
ENTRYPOINT ["./boot.sh"]