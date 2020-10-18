#!/usr/bin/env bash

python manage.py migrate
python manage.py collectstatic
echo "server running at port 8000"
python manage.py runserver 0.0.0.0:8000