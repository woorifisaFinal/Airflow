FROM apache/airflow:2.5.1

USER root

RUN apt-get update && apt-get install -y build-essential
RUN apt-get install -y default-libmysqlclient-dev

# Install requirements
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

CMD ["webserver"]

