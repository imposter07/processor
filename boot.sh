#!/bin/bash

source venv/bin/activate
flask db upgrade
flask translate compile
exec gunicorn -b 0.0.0.0:5000 -w 4 -k gthread --access-logfile - --error-logfile - main:app