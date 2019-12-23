#!/bin/bash

source venv/bin/activate
flask db upgrade
flask translate compile
exec gunicorn -b 0.0.0.0:5000 -w 4 -k gthread --timeout 240 --access-logfile - --error-logfile - main:app