#!/usr/bin/env bash

python manage.py migrate
echo "server running at port 80"
python manage.py runserver 0.0.0.0:80
