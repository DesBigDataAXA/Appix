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

source_table="outgoing_extraction"
destination_table="outgoing_2022"
table_control="data_script_outgoing_filtered"
days=1
def extract():
    try:
        print('start')

        #Obtener codigos ean a filtrar.
        engine = create_engine(f'mariadb+pymysql://{user}:{password}@{host}/{database}')
        sql = """SELECT * FROM `apiix`;"""
        dfean = pd.read_sql_query(sql, engine)

        last_day_extracted=helpers.lastDayExtracted(table_control)
        if last_day_extracted is None :
            #Si no existen registros de ultima extracion, se hace extraccion a partir del 2024.
            date = datetime.strptime(str("2023-12-31"), "%Y-%m-%d")
        else:
            date = datetime.strptime(str(last_day_extracted), "%Y-%m-%d")
       
        date+= timedelta(days=1)
       
        while date.date() < datetime.now().date(): 
            status = 0
            total_records=0
            start=date.strftime("%Y-%m-%d")
            end=(date+ relativedelta(days=days)).strftime("%Y-%m-%d")
            print(start,end)
        
            try:
                helpers.clear(date,destination_table)
                
                # se Obtienes pedidos de  dia en especifico.
                query = f"""SELECT * FROM `{source_table}` WHERE fechaIngreso >= '{start}'  AND fechaIngreso  < '{end}';"""
                print(query)
               
                total_records=0
                for chunk_dataframe in pd.read_sql(query, engine, chunksize=50000):
                    print('Ejecuntando consulta.')
                    if not chunk_dataframe.empty:
                        chunk_dataframe = chunk_dataframe.fillna(0)
                        chunk_dataframe = chunk_dataframe.drop('created_at', axis=1)
                        chunk_dataframe = chunk_dataframe.drop('updated_at', axis=1)
                        chunk_dataframe['CodBarras'] =chunk_dataframe['CodBarras'].astype(str).str.strip()
                        print(chunk_dataframe.head())
                        print(len(chunk_dataframe))
                        df_filtered = chunk_dataframe.loc[chunk_dataframe['CodBarras'].isin(dfean['ean'])]
                
                        print(len(df_filtered))
                        helpers.loadDf(df_filtered,destination_table)
                        total_records+= len(df_filtered)
                    else:
                        logBi('Filtrado outcoming',"Dataframe se encuntra vacio, revisar el query:\n " +query + "\n no esta trayendo datos desde la base de datos")
                print(total_records)
                status = 1
            except Exception as e:
                logException('Etl filtrado outcoming while', str(e))
            finally:
                end_date=date + relativedelta(days=days)-relativedelta(days=1)
                helpers.saveDate(end_date, status,total_records,table_control)
                date +=  relativedelta(days=days)
                print('finalizo')
        logBi('Filtrado total Outgoing ok','')
    except Exception as e:
        logException('Etl filtrado outcoming ', str(e))

if __name__ == "__main__":
    extract()
