import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy  import create_engine, text
import logging
import numpy as np
import datetime
from logBi import logBi
from logException import logException

import os
from dotenv import load_dotenv
load_dotenv()

# Función para parsear la tabla HTML
def parse_html_table(html):
   
   print('Inicia Proceso para parsear el HTML')
   soup = BeautifulSoup(html, 'html.parser')
   table = soup.find('table')
  
   datos = pd.read_html(str(table))[0]
   df = pd.DataFrame(datos)
   
   df.columns = ['Fecha de transacción', 'Número de documento', 'Fecha de vencimiento', 'Total', 'Cuenta: Número', 'Cuenta (línea): Número', 'Tipo de transacción', 'Ubicación: ID de sucursal', 'articulo_agrupador', 'agrupador_ubicacion', 'Ubicación', 'Clase', 'Clase: Nombre', 'Entidad (línea): KS NOMBRE COMPAÑIA', 'Entidad: ID', 'Entidad: KS Nombre Comercial', 'Cuenta de cuentas por cobrar predeterminadas: Número', 'Cliente:Trabajo', 'Actual']
   #df['created_at'] = datetime.datetime.now()
   df['Updated at'] = datetime.datetime.now()
   #df['fecha_informe'] = datetime.datetime.now()
   df['Cartera'] = ''
   df = df.iloc[1:]
   return df

# Función para validar los datos
def validate_data(df):
   print('Inicia Proceso para Validar el dataframe')
   
   # Conversión de la columna 'fecha_transaccion' y 'fecha_vencimiento' a fecha
   df['Fecha de transacción'] = pd.to_datetime(df['Fecha de transacción'], format='%d/%m/%Y')
   df['Fecha de vencimiento'] = pd.to_datetime(df['Fecha de vencimiento'], format='%d/%m/%Y')
   
   # Formateo de la fecha a formato MariaDB
   df['Fecha de transacción'] = df['Fecha de transacción'].apply(lambda x: x.strftime('%Y-%m-%d'))
   df['Fecha de vencimiento'] = df['Fecha de vencimiento'].apply(lambda x: x.strftime('%Y-%m-%d'))
   
   # Eliminación de los espacios en blanco al principio y al final de la cadena
   df['Total'] = df['Total'].str.strip()
   
   # Reemplazo de los caracteres o símbolos no válidos con espacios
   df['Total'] = df['Total'].str.replace('=', '')
   df['Clase: Nombre'] = df['Clase: Nombre'].str.replace('- Nº Clase -', 'No Clase')
   df['Clase: Nombre'] = df['Clase: Nombre'].str.replace('DROXI COSTA', 'DROXI CARIBE')
   df['Ubicación'] = df['Ubicación'].str.replace('- Nº Ubicación -', 'No Ubicación') 
   df['Ubicación'] = df['Ubicación'].str.replace(':', ' : ') 
   
   # Conversión de la columna 'Total' a float
   df['Total'] = df['Total'].astype(float)
   df['Actual'] = 0
   
   condition = (df['Cuenta: Número'] == '13050501') | (df['Cuenta (línea): Número'] == '13050501')
   df['Cartera'] = np.where(condition, 'Cartera', None)
   
   df['Total'] = df['Total'].round(2)
   
   # Validación de valores faltantes
   nombre_columna = 'Cartera'
   df = df.dropna(subset=[nombre_columna]) 
   df.loc[:, 'Ubicación: ID de sucursal'] = pd.to_numeric(df['Ubicación: ID de sucursal'], errors='coerce').fillna(0).astype(int)
   return df

# Función para cargar los datos en la base de datos MySQL
def load_data_to_mysql(df):
   print('Inicia Proceso para cargar df a la base de datos')
   engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
   nombre_tabla = 'tb_cartera_comercial_detallada'
   print('truncando la tabla...')
   # Ejecuta la sentencia SQL para truncar la tabla
   with engine.connect() as connection:
      query = text(f'TRUNCATE TABLE {nombre_tabla}')
      connection.execute(query)
   column_order = ['Cuenta de cuentas por cobrar predeterminadas: Número', 'Cuenta: Número', 'Cuenta (línea): Número', 'Tipo de transacción', 'Fecha de transacción', 'Número de documento', 'Fecha de vencimiento', 'Ubicación: ID de sucursal', 'Ubicación', 'Clase', 'Clase: Nombre', 'Cliente:Trabajo', 'Actual', 'Entidad (línea): KS NOMBRE COMPAÑIA', 'Entidad: KS Nombre Comercial', 'Total', 'Entidad: ID', 'Updated at', 'ID_Clase', 'Cartera']
   df = df[column_order]
   df.to_sql(nombre_tabla, engine, if_exists='append', index=False) 
   print('Proceso finalizado') 
   # Cierre de la conexión a la base de datos
   #engine.commit()




# Manejo de errores
try:
   # URL del web query
   url ="https://4572765.app.netsuite.com/app/reporting/webquery.nl?compid=4572765&entity=91521&email=diego.jimenez@axa.com.co&role=3&cr=1696&hash=AAEJ7tMQdOimuYIqM6zuVllVMrtUPbWpD532x2btYgPs5rHBK7o"
   # Obtención de los datos del web query
   response = requests.get(url)
   print(response.status_code) 

   host = os.getenv("DB_HOST")
   user = os.getenv("DB_USERNAME")
   password = os.getenv("DB_PASSWORD")
   database = os.getenv("DB_DATABASE")

   # Parseo de la respuesta HTML
   soup = BeautifulSoup(response.text, 'html.parser')

   # Extracción de la tabla
   table = soup.find('table')

   # Creación del DataFrame
   df = parse_html_table(str(table))

      #print(df.columns)

   # Validación de los datos
   df = validate_data(df)
   
  # Cargar los datos del archivo Excel
   archivo_excel = "C:/Users/Administrator/Desktop/scripts/Tabla Maestra Homologacion Clases.xlsx"
   
   nombre_hoja = "Homologacion no costos"
   df_excel = pd.read_excel(archivo_excel,sheet_name=nombre_hoja)
   nombre_columna = 'id_ubicacion'
   nombre_columna2 = 'id_nueva_clase'
   df_excel = df_excel.dropna(subset=[nombre_columna,nombre_columna2])
   df_excel['id_ubicacion'] = df_excel['id_ubicacion'].astype(int)
   df_excel['id_nueva_clase'] = df_excel['id_nueva_clase'].astype(int)
   df['Clase: Nombre'] = df['Clase: Nombre'].str.strip()
   df_excel['Nombre NTST Alt'] = df_excel['Nombre NTST Alt'].str.strip()
   
   # Realiza el join entre df y df_excel
   df_final = df.merge(df_excel, left_on=['Ubicación: ID de sucursal','Clase: Nombre'], right_on=['id_ubicacion','Nombre NTST Alt'], how='left')
   
   df['ID_Clase'] = df_final['id_nueva_clase']

   # Carga de los datos en la base de datos MySQL
   load_data_to_mysql(df)
   logBi('Extracion cartera_comercial_detallada ok.','')
except Exception as e:
   print(str(e))
   logBi('Extracion cartera_comercial_detallada  fallo.','')
   logException('Extracion cartera_comercial_detallada  fallo',str(e))