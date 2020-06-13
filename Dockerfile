FROM python

COPY . . 

RUN pip install -r requirements.txt

RUN python manage.py runserver 0:8000
