import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy  import create_engine, text
from logBi import logBi
from logException import logException
import datetime
import os
from dotenv import load_dotenv
load_dotenv()
nombre_tabla = "ventas_miles_mes_corriente"


print('Inicio del proceso')
# Función para parsear la tabla HTML
def parse_html_table(html):
   print('Inicia Proceso para parsear el HTML')
   soup = BeautifulSoup(html, 'html.parser')
   table = soup.find('table')
   datos = pd.read_html(str(table))[0]
   df = pd.DataFrame(datos)
   df.columns = ['Fecha', 'Número de documento', 'Valor', 'Agrupador', 'Ubicación', 'Clase', 'Clase: Nombre', 'A QUIEN CAE LA VENTA?', 'Documento Cliente', 'Nombre Cliente', 'Departamento', 'Nombre establecimiento', 'Lista de precios', 'Tipo de transacción', 'direccion envio', 'Método de Pago']
 
   df['Updated at'] = datetime.datetime.now()
   df = df.iloc[1:]
   return df

# Función para validar los datos
def validate_data(df):
   print('Inicia Proceso para Validar el dataframe')
   # Validación de los tipos de datos
   # Conversión de la columna 'fecha' a fecha
   df['Fecha'] = pd.to_datetime(df['Fecha'], format='%d/%m/%Y')
   # Formateo de la fecha a formato MySQL
   df['Fecha'] = df['Fecha'].apply(lambda x: x.strftime('%Y-%m-%d'))
   # Eliminación de los espacios en blanco al principio y al final de la cadena
   df['Valor'] = df['Valor'].str.strip()
   # Reemplazo de los caracteres o símbolos no válidos con espacios
   df['Valor'] = df['Valor'].str.replace('=', '')

   # Conversión de la columna 'valor' a float
   df['Valor'] = df['Valor'].astype(float)
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
   column_order = ['Agrupador', 'Ubicación', 'Clase', 'Clase: Nombre', 'A QUIEN CAE LA VENTA?', 'Documento Cliente', 'Nombre Cliente', 'Departamento', 'Nombre establecimiento', 'Lista de precios', 'Tipo de transacción', 'Fecha', 'Número de documento','direccion envio', 'Método de Pago', 'Valor', 'Updated at']
   df = df[column_order]
   df.to_sql(nombre_tabla, engine, if_exists='append', index=False) 
   print('Proceso finalizado') 
   # Cierre de la conexión a la base de datos
  
# Manejo de errores
try:
   host = os.getenv("DB_HOST")
   user = os.getenv("DB_USERNAME")
   password = os.getenv("DB_PASSWORD")
   database = os.getenv("DB_DATABASE")

   # URL del web query
   url = "https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=91521&email=diego.jimenez@axa.com.co&role=3&cr=1885&hash=AAEJ7tMQnpiYwxbMgSBV8bXRBxcQcSV2rBVwHBUt-owpCFbVcXQ"

   # Obtención de los datos del web query
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

   logBi(f'Extracion {nombre_tabla} ok.','')
except Exception as e:
   print("Se produjo un error: %s", e)
   msg=f'Extraccion  {nombre_tabla} fallo.'
   logBi(msg,'')
   logException(msg,str(e))