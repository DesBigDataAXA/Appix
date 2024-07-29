from sqlalchemy  import create_engine, text
from datetime import date, timedelta, datetime
import pandas as pd
import time
from dateutil.relativedelta import relativedelta
import os
from dotenv import load_dotenv
import helpers
from logBi import logBi
from logException import logException

load_dotenv()

host = os.getenv("DB_HOST")
user = os.getenv("DB_USERNAME")
password = os.getenv("DB_PASSWORD")
database = os.getenv("DB_DATABASE")

source_table="incoming_extraccion"
destination_table="incoming_2022"
table_control="data_script_incoming_filtered"

def extract():
    try:
        #Obtener codigos ean a filtrar.
        print('start')

        engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
        sql = """SELECT * FROM `apiix`;"""
        dfean = pd.read_sql_query(sql, engine)

        last_day_extracted=helpers.lastDayExtracted(table_control)
        if last_day_extracted is None:
            #Si no existen registros de ultima extracion, se hace extraccion a partir del 2021.
            start_date = datetime.strptime(str("2020-12-31"), "%Y-%m-%d")
        else:
            start_date = datetime.strptime(str(last_day_extracted), "%Y-%m-%d")
       
        start_date = start_date + timedelta(days=1)
       
        while start_date.date() < datetime.now().date(): 
            status = 0
            total_records=0
            date=start_date.strftime("%Y-%m-%d")
            end_date=(start_date+timedelta(days=1)).strftime("%Y-%m-%d")
            print(start_date)
            try:
                
                helpers.clear(start_date,destination_table)
               
                # se Obtienes pedidos de  dia en especifico.
                query = f"""SELECT * FROM `{source_table}` WHERE fechaIngreso > "{date}"  AND fechaIngreso  < "{end_date}";"""
                print(query)
                
                total_records=0
                for chunk_dataframe in pd.read_sql(query, engine, chunksize=100000):
                    print('Ejecuntando consulta.')
                    if not chunk_dataframe.empty:
                        chunk_dataframe = chunk_dataframe.fillna(0)
                        chunk_dataframe['CodBarras'] =chunk_dataframe['CodBarras'].astype(str).str.strip()
                        print(chunk_dataframe.head())
                        print(len(chunk_dataframe))
                        df_filtered = chunk_dataframe.loc[chunk_dataframe['CodBarras'].isin(dfean['ean'])]
                
                        print(len(df_filtered))
                        helpers.loadDf(df_filtered,destination_table)
                        total_records+= len(df_filtered)
                    else:
                        logBi('Respuesta vacia chuck incoming filtered','')
                print(total_records)
                status = 1
            except Exception as e:
                logException('Etl filtrado incoming while', str(e))
            finally:
                helpers.saveDate(date, status,total_records,table_control)
                start_date = start_date + timedelta(days=1)
                print('finalizo')
        logBi('Filtrado total incoming ok','')
    except Exception as e:
        logException('Etl filtrado incoming ', str(e))


if __name__ == "__main__":
    extract()
