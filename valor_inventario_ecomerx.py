import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
import traceback
from logBi import logBi
from logException import logException
import os
import sys
from dotenv import load_dotenv
load_dotenv()

print('Inicio del proceso')

destination_table = 'galenica_inventario_mes_actual'
# Función para parsear la tabla HTML


def parse_html_table(html):
    print('Inicia Proceso para parsear el HTML')
    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    datos = pd.read_html(str(table))[0]
    df = pd.DataFrame(datos)

    df.columns = [
        'id_bodega',
        'bodega',
        'disponible',
        'valor_inventario',
        'linea',
        'fabricante',
        'id_articulo',
        'codigo_barras',
        'forma_farmaceutico',
        'articulo',
        
    ]

    df = df.iloc[1:]
    return df
# Función para validar los datos


def validate_data(df):
    df['id_bodega']=df['id_bodega'].fillna(0)
    df['id_bodega']=df['id_bodega'].astype(int)
    df= df[df['id_bodega']>1]

    df= df[df['fabricante'] == "MEGALABS COLOMBIA SAS"]
    df['disponible']=df['disponible'].fillna(0)
    df['valor_inventario']=df['valor_inventario'].fillna(0)
    df['forma_farmaceutico']=df['forma_farmaceutico'].fillna(0)
    print('Inicia Proceso para Validar el dataframe')

    df['valor_inventario'] = df['valor_inventario'].str.strip()
    df['valor_inventario'] = df['valor_inventario'].str.replace('=', '')

    df['disponible'] = df['disponible'].str.strip()
    df['disponible'] = df['disponible'].str.replace('=', '')
    df['valor_inventario']=df['valor_inventario'].astype(float)
    df['disponible']=df['disponible'].astype(int)
    
    df['costo_unitario'] = df['valor_inventario']/df['disponible']
    

    return df

# Función para cargar los datos en la base de datos MySQL


def load_data_to_mysql(df):
    print('Inicia Proceso para cargar df a la base de datos')

    engine = create_engine(
        f'mysql+pymysql://{user}:{password}@{host}/{database}')

    with engine.connect() as connection:
        query = text(f'TRUNCATE TABLE {destination_table}')
        connection.execute(query)

    column_order = [
        'id_bodega',
        'bodega',
        'disponible',
        'valor_inventario',
        'linea',
        'fabricante',
        'id_articulo',
        'codigo_barras',
        'forma_farmaceutico',
        'articulo',
        'costo_unitario'

    ]
    df = df[column_order]
    df.to_sql(destination_table,
              engine, if_exists='append', index=False)
    print('Proceso finalizado')

# Función para validar si el registro existe en la base de datos


try:

    host = os.getenv("DB_HOST")
    user = os.getenv("DB_USERNAME")
    password = os.getenv("DB_PASSWORD")
    database = os.getenv("DB_DATABASE")

    print('Inicia descarga de reporte.')
    # URL del web query

    url="https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=71318&email=jose.bustos@distriaxa.co&role=3&cr=1957&hash=AAEJ7tMQsU72ldaLN-Lh7MJzICIu8bhxXlZaCgv95PLdpP2KCTw"
    # Obtención de los datos del web query
    response = requests.get(url)
    # Parseo de la respuesta HTML
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extracción de la tabla
    table = soup.find('table')
    
    # Creación del DataFrame
    df = parse_html_table(str(table))

   
    # Validación de los datos
    df = validate_data(df)

    primera_fila = df.iloc[2]
    print(primera_fila)
  
    # Carga de los datos en la base de datos MySQL
    load_data_to_mysql(df)

    logBi(f'Extraccion {destination_table} ok.', '')
except Exception as e:
    logBi(f'Extraccion {destination_table} fallo.', '')
    traceback.print_exc()
    exc_type, exc_obj, exc_tb = sys.exc_info()
    file_name = exc_tb.tb_frame.f_code.co_filename
    line_number = exc_tb.tb_lineno

    print(f"Archivo: {file_name}, Línea: {line_number}")
    print("Se produjo un error: %s", e)
    logException('Descarga reporte inventario a corte fallo', str(e))
