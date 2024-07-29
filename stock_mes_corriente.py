import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy  import create_engine, text
import datetime
from dateutil.relativedelta import relativedelta
from logBi import logBi
from logException import logException
import os
from dotenv import load_dotenv
load_dotenv()

nombre_tabla = 'stock_mes_corriente'

print('Inicio del proceso')

# Función para parsear la tabla HTML
def parse_html_table(html):
   print('Inicia Proceso para parsear el HTML')
   soup = BeautifulSoup(html, 'html.parser')
   table = soup.find('table')

   datos = pd.read_html(str(table))[0]
   df = pd.DataFrame(datos)
   
   df.columns = ['id_bodega', 'bodega', 'disponible', 'inv_valor', 'linea', 'fabricante', 'id_interno', 'cod_bar', 'forma_farmaceutica', 'item']
   df['created_at'] = datetime.datetime.now()
   df['updated_at'] = datetime.datetime.now()
   
   report_date= datetime.datetime.now()
   # Ajuste dia domingo error portlet
   if( report_date.weekday()==6):
       report_date= report_date-relativedelta(days=1)
   
   df['fecha_informe']= report_date.strftime("%Y-%m-%d")
   print(df['fecha_informe'])
   df['costo_und'] = 0
   df = df.iloc[1:]
   return df

# Función para validar los datos
def validate_data(df):
   print('Inicia Proceso para Validar el dataframe')
   nombre_columna = 'id_bodega'
   df = df.dropna(subset=[nombre_columna])  
   
   # Eliminación de los espacios en blanco al principio y al final de la cadena
   df['inv_valor'] = df['inv_valor'].str.strip()
   # Reemplazo de los caracteres o símbolos no válidos con espacios
   df['inv_valor'] = df['inv_valor'].str.replace('=', '')
   df['disponible'] = df['disponible'].str.replace('=', '')
   # Conversión de la columna 'valor' a float
   df['inv_valor'] = df['inv_valor'].astype(float)
   df['disponible'] = df['disponible'].astype(int)

   condition = (df['inv_valor'] != 0) & (df['disponible'] != 0)
   df.loc[condition, 'costo_und'] = df['inv_valor'] / df['disponible']
   
   df['costo_und'] = df['costo_und'].round(2)
 
   return df

# Función para cargar los datos en la base de datos MySQL
def load_data_to_mysql(df):
   print('Inicia Proceso para cargar df a la base de datos')
   engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
   print('truncando la tabla...')
   # Ejecuta la sentencia SQL para truncar la tabla
   with engine.connect() as connection:
      query = text(f'TRUNCATE TABLE {nombre_tabla}')
      connection.execute(query)
   column_order = ['fecha_informe', 'id_bodega', 'bodega', 'fabricante', 'linea', 'cod_bar', 'forma_farmaceutica', 'item', 'disponible', 'inv_valor', 'costo_und', 'id_interno', 'updated_at']
   df = df[column_order]
   df.to_sql(nombre_tabla, engine, if_exists='append', index=False) 
   print('Proceso finalizado') 

try:
   host = os.getenv("DB_HOST")
   user = os.getenv("DB_USERNAME")
   password = os.getenv("DB_PASSWORD")
   database = os.getenv("DB_DATABASE")

   # URL del web query
   url = "https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=91521&email=diego.jimenez@axa.com.co&role=3&cr=1661&hash=AAEJ7tMQlFkfycVyW3OZPREyNVxRicD3Z7cigr-B3UzkBaK77Og"

   response = requests.get(url)
   print(response.status_code)

   # Parseo de la respuesta HTML
   soup = BeautifulSoup(response.text, 'html.parser')

   # Extracción de la tabla
   table = soup.find('table')

   # Creación del DataFrame
   df = parse_html_table(str(table))
    
   # Validación de los datos
   df = validate_data(df)

   # Carga de los datos en la base de datos MySQL
   load_data_to_mysql(df)

   logBi(f'Extraccion  {nombre_tabla} ok.','')
except Exception as e:
   print("Se produjo un error: %s", e)
   msg=f'Extraccion  {nombre_tabla} fallo.'
   logBi(msg,'')
   logException(msg,str(e))