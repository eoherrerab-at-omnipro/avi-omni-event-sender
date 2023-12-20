#Se importa el módulo que permite manejar la información de los dataframe
import data_handling
#Se importa el módulo que permite manipular la API de Adobe Experience Platform
import adobe_requests
#Se importa el módulo que permite crear procesos al mismo tiempo
import multiprocessing
#Se importa el módulo que permite manejar la información por perfiles
import event_series

#Se define la función que manipula y envía la información a Experience Platform
def create_dataflow(dataflow_data: dict, file: dict) -> None:
    
    #Se define el dataframe como el producto de la función para la lectura del contenido del archivo
    dataframe = data_handling.read_file_content(file_content = file["file_content"], file_delimiter = dataflow_data["separator"], identity_column = dataflow_data["identity_column"])

    #Se define la cabecera del archivo como la cabecera de cada columna del dataframe
    dataframe_headers = list(dataframe.columns.values)

    #Se definen las líneas del archivo como la iteración del dataframe línea por línea
    dataframe_rows = [list(row) for row in dataframe.itertuples(index=False)]

    #Se define el token de acceso como el producto de la función para la generación del token de acceso
    access_token = adobe_requests.generate_access_token()

    #Se define el diccionario que contiene la información por perfil único
    payloads = {}

    #Se realia una iteración sobre todos los elementos que existan en la lista de filas del dataframe
    for dataframe_row in dataframe_rows:

        #Se define el payload como el producto de la función para la generación del payload
        payload = data_handling.generate_payload(file_name = file["file_name"], dataflow_data = dataflow_data, keys_list = dataframe_headers, values_list = dataframe_row)

        #Se evalua si la identidad está en el diccionario de payloads
        if payload[dataflow_data["identity_column"]] not in payloads:
            
            ##Se define una lista que contendrá la información de los payloads
            payloads[payload[dataflow_data["identity_column"]]] = []

        #Se agrega un elemento a la lista
        payloads[payload[dataflow_data["identity_column"]]].append(payload)

    #Se realia una iteración sobre todos los elementos que existan en la lista de llaves del diccionario de payloads
    for key in payloads.keys():

        #Se define un nuevo proceso, usando la función para crear un nuevo flujo de datos internamente
        process =  multiprocessing.Process(target = event_series.send_event_series_to_endpoint, args = [access_token, dataflow_data["flow_id"], payloads[key]])

        #Se inicia el proceso definido anteriormente    
        process.start()