from sqlalchemy import create_engine, text
from datetime import date, timedelta, datetime
import pandas as pd
import time
from dateutil.relativedelta import relativedelta

import helpers
from logBi import logBi
from logException import logException

#variable que contiene tiempo a esperar entre consultas.
sleep_minutes=1
destination_table="incoming_extraccion"
table_control="data_script_incoming_extraction"
def extract():
    try:

        last_day_extracted=helpers.lastDayExtracted(table_control)
        if last_day_extracted is None:
            #Si no existen registros de ultima extracion, se hace extraccion a partir del 2021.
            start_date = datetime.strptime(str("2020-12-31"), "%Y-%m-%d")
        else:
            start_date = datetime.strptime(str(last_day_extracted), "%Y-%m-%d")
       
        start_date = start_date + timedelta(days=1)

        while start_date .date() < datetime.now().date() : 
            print(datetime.now().strftime("%H"))
            if (int(datetime.now().strftime("%H")) > 20 and int(datetime.now().strftime("%H"))< 22):
                sleep_minutes=1
            if (int(datetime.now().strftime("%H")) > 22 and int(datetime.now().strftime("%H"))<= 23):
                sleep_minutes=0
            if (int(datetime.now().strftime("%H")) > 0 and int(datetime.now().strftime("%H"))<5 ):
                sleep_minutes=0

            status = 0
            total_records=0
        
            try:
                date = start_date.strftime("%Y-%m-%d")
                year_date=start_date.strftime("%Y")
               
                helpers.clear(start_date,destination_table)

                # Realiza conexion a base de datos de apix.
                engine = create_engine('mssql+pyodbc://KardexUser:Axa2021..@mdc-databases-server.database.windows.net:1433/kardexdb?driver=ODBC+Driver+17+for+SQL+Server')

                # Consulta sql que trae los datos de los pedidos
                query = f"""SELECT KeyIdDetalleEntrada, KeyIdEntrada, fechaIngreso, docSoporte, fechaSoporte, totalEntrada, KeyIdCliente, nomCliente,
                    EsAfiliado, TipoAfliado, NombreE_S, NomProveedor, NombreEmpleado, ApellidoEmpleado, NumEntrada, Pagada,
                    TotalSinIva, MontoIva, cantidad, precioCompra, fechaVencimiento, nomArticulo, CodBarras, CantPresentArt,
                    CodBarrasHijo, NomFabricante, nomCategoria, subTotal, SubTotalSinIva, CostoPromedioArt, CodDepart, NomDepart, 
                    CodCiudad, NomCiudad, NitCliente,InternalId,FechaSync
                    FROM [dbo].[Incoming{year_date}] WHERE CAST(fechaIngreso AS DATE) = '{date}'"""

            
                total_records=0
                for chunk_dataframe in pd.read_sql(query, engine, chunksize=10000):
                    
                    if not chunk_dataframe.empty:
                        chunk_dataframe = chunk_dataframe.fillna(0)
                        chunk_dataframe['NitCliente'] = chunk_dataframe['NitCliente'].astype(str)
                        chunk_dataframe['NitCliente'] = [x.split('-')[0] for x in chunk_dataframe['NitCliente']]
                        chunk_dataframe['NitCliente'] = chunk_dataframe['NitCliente'].str.replace('.', '', regex=False)
                        chunk_dataframe['nomCliente'] = chunk_dataframe['nomCliente'].str.rstrip()
                        chunk_dataframe['nomArticulo'] = chunk_dataframe['nomArticulo'].str.rstrip()

                        print(chunk_dataframe.head())
                        helpers.loadDf(chunk_dataframe,destination_table)
                        total_records+= len(chunk_dataframe)
                    else:
                        logBi('Respuesta vacia-incoming','')
                print(total_records)
                status = 1
            except Exception as e:
                logException('Extración completa incoming-while', str(e))
            finally:
                helpers.saveDate(date, status,total_records,table_control)
                start_date = start_date + timedelta(days=1)
                print('finalizo')
                print('siguiente consulta en :'+str(sleep_minutes)+" minutos")
                time.sleep(sleep_minutes*60)
        logBi('Extracion total incoming ok','')
    except Exception as e:
        logException('Extración completa incoming-general', str(e))


if __name__ == "__main__":
    extract()
