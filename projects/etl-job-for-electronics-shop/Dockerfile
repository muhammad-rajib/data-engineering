FROM ubuntu:20.04

RUN apt-get update \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2

COPY requirements.txt /opt/etl-job/requirements.txt
WORKDIR /opt/etl-job

RUN pip install -r requirements.txt

COPY . /opt/etl-job/

CMD ["python3", "app.py"]