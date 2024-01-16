#Se importa el módulo que permite cargar la información del archivo .env. Para esto, se debe usar pip install python-dotenv
import dotenv
#Se importa el módulo que permite manejar la información de los dataframe
import data_handling
#Se importa el módulo que permite enviar la información los endpoints
import adobe_requests
#Se importa el módulo que permite crear clientes HTTP. Para esto, se debe usar pip install httpx
import httpx

#Se carga el archivo .env disponible en la carpeta config
dotenv.load_dotenv(dotenv_path="config/aep.env")

#Se define la función que manipula y envía la información a Experience Platform
def create_dataflow(dataflow_data: dict, file: dict, access_token: str) -> None:

    #Se define el dataframe como el producto de la función para la lectura del contenido del archivo
    dataframe = data_handling.read_file_content(file_content = file["file_content"], file_delimiter = dataflow_data["separator"], identity_column = dataflow_data["identity_column"])

    #Se define la cabecera del archivo como la cabecera de cada columna del dataframe
    dataframe_headers = list(dataframe.columns.values)

    #Se definen las líneas del archivo como la iteración del dataframe línea por línea
    dataframe_rows = [list(row) for row in dataframe.itertuples(index=False)]

    #Se define el cliente HTTP como el objeto de cliente sin tiempo de finalización
    http_client = httpx.Client(timeout=None)

    #Se evalua si la información asociada al flujo de datos se envia mediante un flujo de datos en Experience Platform
    if "flow_id" in dataflow_data.keys():
    
        #Se define un diccionario que contiene todos los perfiles envíados y la ultima vez en la cual se envía
        sent_profiles = {}

        #Se realiza una iteración mientras la cantidad de líenas restantes del archivo sean mayores a cero
        while len(dataframe_rows) > 0:

            #Se realiza una iteración para cada elemento de la lista
            for row in dataframe_rows:
                
                #Se evalua si la información está disponible para enviar
                if data_handling.is_able_to_send(sent_profiles = sent_profiles, profile = row[0]):

                    #Se define el payload como el producto de la función para la generación del payload
                    payload = data_handling.generate_event_payload(file_name = file["file_name"], dataflow_data = dataflow_data, keys_list = dataframe_headers, values_list = row)

                    #Se envia el payload usando la función para el envío de información al endpoint
                    adobe_requests.send_event_to_endpoint(client = http_client, access_token = access_token, adobe_flow_id = dataflow_data["flow_id"], data = payload)

                    #Se actualiza la información de envio de ese perfil
                    sent_profiles = data_handling.update_sent_profiles(sent_profiles = sent_profiles, profile = row[0])

                    #Se agrega la línea enviada al archivo de backup
                    data_handling.write_row_in_backup(file = file, row = row)

                    #Se elimina ese elemento de la lista
                    dataframe_rows.remove(row)


    #Se evalua si la información asociada al flujo de datos se envia mediante una campaña                    
    elif "campaign_id" in dataflow_data.keys():

        #Se realiza una iteración para cada elemento de la lista    
        for row in dataframe_rows:

            #Se define el payload como el producto de la función para la generación del payload
            payload = data_handling.generate_api_triggered_payload(file_name = file["file_name"], dataflow_data = dataflow_data, profile = row[0], keys_list = dataframe_headers, values_list = row)
            
            #Se envia el payload usando la función para el envío de información al endpoint
            adobe_requests.send_api_triggers_to_endpoint(client = http_client, access_token = access_token, data = payload)

            #Se agrega la línea enviada al archivo de backup
            data_handling.write_row_in_backup(file = file, row = row)
    
    
    #Se cierra el cliente HTTP usando el cliente definido anteriormente
    http_client.close()
