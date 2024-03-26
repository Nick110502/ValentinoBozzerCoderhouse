FROM apache/airflow:2.3.3

USER root
RUN mkdir -p /opt/airflow/data
ADD webserver_config.py /opt/airflow/webserver_config.py
ADD requirements.txt /opt/airflow/requirements.txt

RUN apt-get update \
  && apt-get install -y --no-install-recommends vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

RUN chown -R airflow: /opt/airflow/data

USER airflow
