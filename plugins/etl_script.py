import requests
import pandas as pd
import psycopg2
from datetime import datetime
import os

# Configuración de conexión a Redshift
redshift_host = 'data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
redshift_port = '5439'
redshift_user = 'valentinobo110502_coderhouse'
redshift_password = 'S2ds6CcBP5'
redshift_dbname = 'data-engineer-database'

# API Key para OpenWeatherMap
api_key = 'a128cf8723b4849333992280605a17b9'

def extract_data():
    base_url = 'http://api.openweathermap.org/data/2.5/weather?'
    cities = ['Buenos Aires', 'Córdoba', 'Rosario', 'Mendoza', 'Tucumán']

    data_list = []
    for city in cities:
        full_url = f"{base_url}appid={api_key}&q={city}"
        response = requests.get(full_url)
        weather_data = response.json()

        data = {
            'city': city,
            'temperature': weather_data['main']['temp'],
            'humidity': weather_data['main']['humidity'],
            'pressure': weather_data['main']['pressure'],
            'datetime': datetime.now()
        }
        data_list.append(data)

    weather_df = pd.DataFrame(data_list)
    return weather_df

def transform_data(data_frame):
    # Realizar transformaciones necesarias
    # Por ahora, esto es solo un placeholder, ya que tus datos ya están en el formato deseado
    return data_frame

def load_data(transformed_data):
    conn_string = f"dbname='{redshift_dbname}' user='{redshift_user}' host='{redshift_host}' port='{redshift_port}' password='{redshift_password}'"
    conn = psycopg2.connect(conn_string)

    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            city VARCHAR(50),
            temperature DECIMAL(5,2),
            humidity INTEGER,
            pressure INTEGER,
            datetime TIMESTAMP
        );
    """)

    for index, row in transformed_data.iterrows():
        cursor.execute("""
            INSERT INTO weather_data (city, temperature, humidity, pressure, datetime)
            VALUES (%s, %s, %s, %s, %s)
        """, (row['city'], row['temperature'], row['humidity'], row['pressure'], row['datetime']))

    conn.commit()
    cursor.close()
    conn.close()

def send_alert():
    print("Alerta: Se ha detectado un problema en el proceso ETL.")

def etl_pipeline():
    # Crear el directorio si no existe
    data_directory = '/opt/airflow/data/'
    if not os.path.exists(data_directory):
        os.makedirs(data_directory)
    
    # Extraer datos
    extracted_data = extract_data()
    
    # Transformar datos
    transformed_data = transform_data(extracted_data)
    
    # Cargar datos
    try:
        load_data(transformed_data)
        print("Datos cargados con éxito.")
    except Exception as e:
        print("Error al cargar los datos.")
        send_alert()
        raise e

    # Guardar el archivo CSV de datos extraídos por si se necesita para revisión o uso futuro
    extracted_data.to_csv(os.path.join(data_directory, 'extracted_data.csv'), index=False)
    print("Archivo CSV guardado.")

if __name__ == "__main__":
    etl_pipeline()
