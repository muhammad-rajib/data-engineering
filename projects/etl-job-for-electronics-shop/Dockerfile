FROM ubuntu:20.04
ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
    && apt-get -y install python3 python3-pip \
    && apt-get -y install libpq-dev gcc \
    && pip install psycopg2 \
    && apt-get install -y software-properties-common \
    && add-apt-repository ppa:openjdk-r/ppa \
    && apt-get install -y openjdk-8-jdk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64

COPY requirements.txt /opt/etl-job/requirements.txt
WORKDIR /opt/etl-job

RUN pip install -r requirements.txt

COPY . /opt/etl-job/

CMD ["python3", "app.py"]