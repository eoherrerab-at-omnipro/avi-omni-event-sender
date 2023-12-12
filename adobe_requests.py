#Se importa el módulo que permite acceder a la información de los archivos .env
import os
#Se importa el módulo que permite cargar la información del archivo .env. Para esto, se debe usar pip install python-dotenv
import dotenv
#Se importa el módulo que permite enviar peticiones HTTP. Para esto, se debe usar pip install requests
import requests
#Se importa el módulo que permite manipular archivos JSON
import json

#Se carga el archivo .env disponible en la carpeta config
dotenv.load_dotenv(dotenv_path="config/aep.env")

#Se define la función que genera el token de acceso.
def generate_access_token() -> dict:

    #Se definen los parámetros de la petición
    params = {
        #Se define los permisos del cliente asociado. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "GRANT_TYPE"
        "grant_type": os.getenv("GRANT_TYPE"),
        #Se define la ID del cliente asociado. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "IMS"
        "client_id": os.getenv("API_KEY"),
        #Se define el secreto del cliente asociado. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "CLIENT_SECRET"
        "client_secret": os.getenv("CLIENT_SECRET"),
        #Se define el alcance. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "SCOPES"
        "scope": os.getenv("SCOPES")
    }

    #Se define la cabecera de la petición
    header = {
        #Se define el host al cual se envia la petición. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "IMS"
        "Host": os.getenv("IMS")
    }

    #Se define el cuerpo de la petición como un diccionario vacio
    body = {

    }

    #Se define la respuesta de la petición como el envío de la petición misma, mediante un método POST, utilizando
    #como URL el IMS cargado desde el archivo .env y la cabecera y cuerpo de la petición previamente definidos
    res = requests.request(method="POST", url=os.getenv("AUTH_ENDPOINT"), params=params ,headers=header, data=body)

    #Se define el token de acceso como el valor obtenido del resultado de la petición, específicamente el campo "access_token"
    access_token = json.loads(res.text)

    #Se retorna el token de acceso a la función original
    return access_token


#Se define la función que genera el trabajo de evaluación de segmentos, la cual recibe por parámetro el token de acceso en formato string (str)
def send_payload_to_endpoint(access_token: str, adobe_flow_id: str, data: dict) -> None:
    
    #Se define la cabecera de la petición
    header = {
        #Se define el tipo de contenido que tiene la petición
        "Content-Type": "application/json",
        #Se define el ID del IMS de la organización. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "IMS_ORG"
        "x-gw-ims-org-id": os.getenv("IMS_ORG"),
        #Se define la llave de API. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "API_KEY"
        "x-api-key": os.getenv("API_KEY"),
        #Se define el ambiente donde se crea el trabajo. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "ENVIROMENT"
        "x-sandbox-name": os.getenv("ENVIROMENT"),
        #Se define el ID del flujo de datos por el cual se ingesta la onformación. Para esto, se utiliza la variable que se recibe por parámentro
        "x-adobe-flow-id": adobe_flow_id,
        #Se define el token de acceso para la autenticación. Para esto, se utiliza la variable que se recibe por parámentro
        "Authorization": "Bearer "+ access_token
    }

    #Se define el cuerpo de la petición. Para eso, se utiliza la variable anteriormente declarada
    body = json.dumps(data, indent=2)

    #Se define la respuesta de la petición como el envío de la petición misma, mediante un método POST, utilizando
    #como URL un enlace estático, la cabecera y cuerpo de la petición previamente definidos
    res = requests.request(method = "POST", url = os.getenv("ENDPOINT"), headers = header, data = body)