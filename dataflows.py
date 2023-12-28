#Se importa el módulo que permite cargar la información del archivo .env. Para esto, se debe usar pip install python-dotenv
import dotenv
#Se importa el módulo que permite manejar la información de los dataframe
import data_handling
#Se importa el módulo que permite acceder a la información de los archivos .env
import os
#Se importa el módulo que permite enviar la información hacia el endpoint
import adobe_requests
#Se importa el módulo que permite realizar pausas en la ejecución del sistema
import time
import datetime
import csv
import pandas as pd

#Se carga el archivo .env disponible en la carpeta config
dotenv.load_dotenv(dotenv_path="config/aep.env")

#Se define la función que manipula y envía la información a Experience Platform
def create_dataflow(dataflow_data: dict, file: dict, access_token: str) -> None:

    #Se define el dataframe como el producto de la función para la lectura del contenido del archivo
    dataframe = data_handling.read_file_content(file_content = file["file_content"], file_delimiter = dataflow_data["separator"], identity_column = dataflow_data["identity_column"])

    #Se define la cabecera del archivo como la cabecera de cada columna del dataframe
    dataframe_headers = list(dataframe.columns.values)

    #Se definen las líneas del archivo como la iteración del dataframe línea por línea
    dataframe_rows = [list(row) for row in dataframe.itertuples(index=True)]

    #
    profile_sent = pd.DataFrame(columns=["LAST_SEND_TIMESTAMP", "PROFILE"])

    list_end = False

    lista = []

    #Se realiza una iteración usando un indice desde cero hasta la cantidad de líneas del dataframe
    #for i in range (len(dataframe_rows)):

    dataframe_rows_index = 0
    
    while len(dataframe_rows) > dataframe_rows_index or not list_end:

        flag = False

        if len(dataframe_rows) > dataframe_rows_index:

            row_to_send = dataframe_rows[dataframe_rows_index][1:]
        
        else:

            if len(lista) > 0:

                seconds = datetime.datetime.now() - lista[0][1]

                if seconds.total_seconds() < 30:

                    continue
            


        if len(lista) > 0:

            seconds = datetime.datetime.now() - lista[0][1]

            if seconds.total_seconds() > 30:
                
                flag = True

                row_to_send = lista[0][0][:]

        #Se define el payload como el producto de la función para la generación del payload
        payload = data_handling.generate_payload(file_name = file["file_name"], dataflow_data = dataflow_data, keys_list = dataframe_headers, values_list = row_to_send)
        
        #Se envia el payload usando la función para el envío de información al endpoint
        adobe_requests.send_payload_to_endpoint(access_token = access_token, adobe_flow_id = dataflow_data["flow_id"], data = payload)

        if flag == False:
        
            #Se evalua si el indice actual es menor que la cantidad de indices totales de la lista
            #if dataframe_rows_index < len(dataframe_rows)-1:
            
                #Se evalua si el campo que contiene la identidad en la iteración actual es el mismo en la iteración siguiente
            
            while dataframe_rows_index < len(dataframe_rows)-1 and dataframe_rows[dataframe_rows_index + 1][1] == dataframe_rows[dataframe_rows_index][1]:
                
                    #Se ejecuta una pausa en el sistema de la cantidad de segundos definida. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "JOURNEYS_REENTRANCE_WAIT_PERIOD"
                    #time.sleep(float(os.getenv("JOURNEYS_REENTRANCE_WAIT_PERIOD")))

                    #
                lista.append([dataframe_rows[dataframe_rows_index + 1][1:], datetime.datetime.now()])

            
                dataframe_rows_index = dataframe_rows_index + 1

                list_end = False
            
            dataframe_rows_index = dataframe_rows_index + 1

        else:
            del lista[0]

            if len(lista) > 0:
                
                lista = data_handling.send_to_list_end(lista, row_to_send[0])
            else:

                list_end = True
