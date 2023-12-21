#Se importa el módulo que permite cargar la información del archivo .env. Para esto, se debe usar pip install python-dotenv
import dotenv
#Se importa el módulo que permite manejar la información de los dataframe
import data_handling
#Se importa el módulo que permite acceder a la información de los archivos .env
import os
#Se importa el módulo que permite manejar la información de los dataframe
import adobe_requests
#Se importa el módulo que permite realizar pausas en la ejecución del sistema
import time

#Se carga el archivo .env disponible en la carpeta config
dotenv.load_dotenv(dotenv_path="config/aep.env")

#Se define la función que manipula y envía la información a Experience Platform
def create_dataflow(dataflow_data: dict, file: dict) -> None:

    print(file)

    #Se define el dataframe como el producto de la función para la lectura del contenido del archivo
    dataframe = data_handling.read_file_content(file_content = file["file_content"], file_delimiter = dataflow_data["separator"], identity_column = dataflow_data["identity_column"])

    #Se define la cabecera del archivo como la cabecera de cada columna del dataframe
    dataframe_headers = list(dataframe.columns.values)

    #Se definen las líneas del archivo como la iteración del dataframe línea por línea
    dataframe_rows = [list(row) for row in dataframe.itertuples(index=True)]

    #Se define el token de acceso como el producto de la función para la generación del token de acceso
    access_token = adobe_requests.generate_access_token()

    #Se realiza una iteración usando un indice desde cero hasta la cantidad de líneas del dataframe
    for i in range (len(dataframe_rows)):
        
        #Se define el payload como el producto de la función para la generación del payload
        payload = data_handling.generate_payload(file_name = file["file_name"], dataflow_data = dataflow_data, keys_list = dataframe_headers, values_list = dataframe_rows[i][1:])
        
        #Se envia el payload usando la función para el envío de información al endpoint
        adobe_requests.send_payload_to_endpoint(access_token = access_token["access_token"], adobe_flow_id = dataflow_data["flow_id"], data = payload)

        #Se evalua si el indice actual es menor que la cantidad de indices totales de la lista
        if i < len(dataframe_rows)-1:
            
            #Se evalua si el campo que contiene la identidad en la iteración actual es el mismo en la iteración siguiente
            if dataframe_rows[i + 1][1] == dataframe_rows[i][1]:
                
                #Se ejecuta una pausa en el sistema de la cantidad de segundos definida. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "JOURNEYS_REENTRANCE_WAIT_PERIOD"
                time.sleep(float(os.getenv("JOURNEYS_REENTRANCE_WAIT_PERIOD")))
        
        
