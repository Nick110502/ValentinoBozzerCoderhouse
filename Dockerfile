FROM apache/airflow:2.3.3

USER root
RUN mkdir -p /opt/airflow/data  # Asegúrate de que esta es la ruta correcta que necesitas.
ADD webserver_config.py /opt/airflow/webserver_config.py

RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Instala las dependencias necesarias para tus scripts
RUN pip install --no-cache-dir yfinance psycopg2-binary sendgrid

# El usuario de Airflow debería tener permisos para escribir en /opt/airflow/data
RUN chown -R airflow: /opt/airflow/data

USER airflow
