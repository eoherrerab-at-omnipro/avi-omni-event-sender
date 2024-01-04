#Se importa el módulo que permite cargar la información del archivo .env. Para esto, se debe usar pip install python-dotenv
import dotenv
#Se importa el módulo que permite acceder e interactuar con los archivos del SFTP. Para esto, se debe utilizar pip install paramiko
import pysftp
#Se importa el módulo que permite acceder a la información de los archivos .env
import os
#Se importa el módulo que permite obtener la fecha actual
import datetime
#Se importa el módulo que permite manejar la información del historial de archivos manipulados
import data_handling

#Se carga el archivo .env disponible en la carpeta config
dotenv.load_dotenv(dotenv_path = "config/sftp.env")

#Se define la función que obtiene los archivos desde el SFTP, en base a las condiciones impuestas en el nombre
def list_files() -> list[dict]:
    
    #Se define un objeto de configuración de conexión
    connection_options = pysftp.CnOpts()

    #Se define las llaves del host como nulas
    connection_options.hostkeys = None

    #Se define una conexión al host usando las configuraciones de conexión definidas anteriormente. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "HOST_NAME", "USER_NAME", "PASSWORD"
    sftp = pysftp.Connection(os.getenv("HOST_NAME"), username = os.getenv("USER_NAME"), password = os.getenv("PASSWORD"), cnopts = connection_options)

    #Se define la lista de rutas de carpetas que se deben evaluar, usando como separador el operador "|". Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "FOLDER_PATHS"
    folder_paths = os.getenv("FOLDER_PATHS").split(sep="|")

    #Se define una lista que contendrá la información de los archivos validos para su envío
    files = []

    #Se obtiene la fecha de hoy
    today = datetime.datetime.today()

    #Se realiza una iteración sobre todos los elementos que existan en la lista
    for folder_path in folder_paths:

        #Se evalua si existe, al menos, un elemento en la lista obtenida a partir de listar los archivos existentes en la carpeta
        if sftp.listdir(remotepath = folder_path):

            #Se realia una iteración sobre todos los elementos que existan en la lista obtenida a partir de listar los archivos existentes en la carpeta
            for file_path in sftp.listdir(remotepath = folder_path):
                
                #Se define una lista para, posteriormente, validar si esta lista no existe actualmente en el historial de archivos manipulados
                row_to_verify = [sftp.stat(remotepath = folder_path + file_path).st_mtime, os.path.basename(file_path), folder_path + file_path]

                #Se evalua si el nombre del archivo, sin extensión de archivo, termina en la fecha de hoy y si no existe registro del archivo en el historial de archivos manipulados
                if os.path.splitext(file_path)[0].endswith(today.strftime("%Y%m%d")) and not data_handling.is_row_in_log(row = row_to_verify):

                    #Se agrega un elemento a la lista. Este elemento se define a partir de la información y metadata del archivo
                    files.append({
                        #Se define el nombre del archivo
                        "file_name": file_path,
                        #Se define la ultima vez que se modificó el archivo
                        "last_modify_file": str(sftp.stat(remotepath = folder_path + file_path).st_mtime),
                        #Se define el contenido del archivo
                        "file_content": sftp.open(remote_file = folder_path + file_path, mode = "r").read().decode(encoding="iso-8859-1")
                    })

                    #Se agrega este registro al historial de archivos manipulados
                    data_handling.write_row_in_log(row = row_to_verify)

    #Se cierra la conexión al host usando el cliente definido anteriormente
    sftp.close()

    #Se retorna la lista de archivos a la función original
    return files


#Se define la función que obtiene el archivo maestro en formato JSON desde el SFTP, en base a las condiciones impuestas en el nombre
def get_master_json_file() -> str:

    #Se define un objeto de configuración de conexión
    connection_options = pysftp.CnOpts()

    #Se define las llaves del host como nulas
    connection_options.hostkeys = None

    #Se define una conexión al host usando las configuraciones de conexión definidas anteriormente. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "HOST_NAME", "USER_NAME", "PASSWORD"
    sftp = pysftp.Connection(os.getenv("HOST_NAME"), username = os.getenv("USER_NAME"), password = os.getenv("PASSWORD"), cnopts = connection_options)

    #Se define el contenido del archivo maestro en formato JSON. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "MASTER_PATH"
    content = sftp.open(remote_file = os.getenv("MASTER_PATH"), mode = "r").read().decode(encoding="iso-8859-1")

    #Se cierra la conexión al host usando el cliente definido anteriormente
    sftp.close()

    #Se retorna el contenido del archivo maestro en formato JSON a la función original
    return content