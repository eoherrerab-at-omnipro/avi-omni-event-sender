#Se importa el módulo que permite cargar la información del archivo .env. Para esto, se debe usar pip install python-dotenv
import dotenv
#Se importa el módulo que permite acceder a la información de los archivos .env
import os
#Se importa el módulo que permite manejar la información de los dataframe
import adobe_requests
#Se importa el módulo que permite realizar pausas en la ejecución del sistema
import time

#Se carga el archivo .env disponible en la carpeta config
dotenv.load_dotenv(dotenv_path="config/aep.env")

#Se define la función que permite ingestar una serie de eventos
def send_event_series_to_endpoint(access_token: str, adobe_flow_id: str, event_series: list):
    
    #Se realia una iteración sobre todos los elementos que existan en la lista de eventos
    for event in event_series:
        
        #Se envia el payload usando la función para el envío de información al endpoint
        adobe_requests.send_event_to_endpoint(access_token = access_token["access_token"], adobe_flow_id = adobe_flow_id, data = event)
        
        #Se ejecuta una pausa en el sistema de la cantidad de segundos definida. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "JOURNEYS_REENTRANCE_WAIT_PERIOD"
        time.sleep(float(os.getenv("JOURNEYS_REENTRANCE_WAIT_PERIOD")))