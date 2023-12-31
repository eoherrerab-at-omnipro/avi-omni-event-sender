#Se importa el módulo que permite cargar la información del archivo .env. Para esto, se debe usar pip install python-dotenv
import dotenv
#Se importa el módulo que permite manipular información en formatos estructurados. Para esto, se debe usar pip install pandas
import pandas as pd
#Se importa el módulo que permite manipular las entradas y salidas de datos
import io
#Se importa el módulo que permite acceder a la información de los archivos .env
import os

import datetime

#Se carga el archivo .env disponible en la carpeta config
dotenv.load_dotenv(dotenv_path="config/log.env")

#Se define la función que obtiene la información del dataflow asociada al tipo de archivo
def get_dataflow_data(dataflows_data: dict, file_name: str) -> dict | None:
  
  #Se realiza una iteración sobre todos los elementos que existen en la lista
  for key in dataflows_data.keys():
    
    #Se evalua si la llave es igual a la palabra clave ubicada en la posición cero del nombre del archivo
    if key.lower() == file_name.split(sep = "_")[0].lower():

      #Se retorna la información del dataflow asociada al tipo de archivo a la función original
      return dataflows_data[key]


#Se define la función que obtiene el dataframe asociado al contenido del archivo
def read_file_content(file_content: str, file_delimiter: str, identity_column: str) -> pd.DataFrame:
  
  #Se define el dataframe como la lectura del contenido del archivo, controlando su entrada como un archivo, y usando el delimitador asociado
  dataframe = pd.read_csv(io.StringIO(file_content), delimiter = file_delimiter, dtype=str)

  #Se define el dataframe como el dataframe definido anteriormente, pero reemplazando los valores vacíos por null
  dataframe = dataframe.replace(pd.NA, None)

  #Se define el dataframe como el dataframe definido anteriormente, pero organizandolo por criterio de la columna que contiene la identidad
  dataframe = dataframe.sort_values(by = identity_column)

  #Se define el dataframe como el dataframe definido anteriormente, pero reasignando los indices de cada línea
  dataframe = dataframe.reset_index(drop = True)

  #Se define el nuevo orden para el dataframe con la columna principal como la primera
  new_order = [identity_column] + [column for column in dataframe.columns if column != identity_column]

  #Se define el dataframe como el dataframe definido anteriormente, pero aplicando el nuevo orden definido anteriormente
  dataframe = dataframe[new_order]

  #Se retorna el dataframe a la función original
  return dataframe


#Se define la función para generar el cuerpo de la petición a enviar via HTTP API
def generate_payload(file_name: str, dataflow_data: dict, keys_list:list[str], values_list: list[str]) -> dict:

  #Se evalua si la longitud de la lista que contiene el nombre del archivo, realizando la separación, es mayor a dos
  if len(os.path.basename(file_name).split(sep="_")) > 2:
    
    #Se evalua si la llave está entre las llaves de dataflow asociado y si la cantidad de información adicional es igual a la cantidad de valores en la llave
    if "aditional_values" in dataflow_data.keys() and str(type(dataflow_data["aditional_values"])) == "<class 'list'>":

      #Se define el payload como un diccionario usando una serie de listas. Para esto, se utilizan las variables que se reciben por parámentro
      payload = dict(zip(keys_list + dataflow_data["aditional_values"], values_list + os.path.basename(file_name).split(sep="_")[1:-1]))

    else:
      #Se define el payload como un diccionario usando una serie de listas. Para esto, se utilizan las variables que se reciben por parámentro
      payload = dict(zip(keys_list, values_list))
  
  else:
    #Se define el payload como un diccionario usando una serie de listas. Para esto, se utilizan las variables que se reciben por parámentro
    payload = dict(zip(keys_list, values_list))

  #Se retorna el diccionario a la función original
  return payload 


#Se define la función que permite conocer si un registro está en el historial de archivos manipulados
def is_row_in_log(row: list[str]) -> bool:

  #Se define el dataframe como la lectura del archivo. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "LOG_FILEPATH"
  dataframe = pd.read_csv(os.getenv("LOG_FILEPATH"))

  #Se define las líneas del dataframe como una lista de cada línea del dataframe, convertida en una lista
  dataframe_rows = [list(row) for row in dataframe.itertuples(index=False)]

  #Se retorna la evaluación de si la línea está en las líneas del dataframe a la función original
  return row in dataframe_rows


#Se define la función que permite agregar un nuevo registro al historial de archivos manipulados
def write_row_in_log(row: list[str]) -> None:

  #Se define el dataframe como la lectura del archivo. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "LOG_FILEPATH"
  dataframe = pd.read_csv(os.getenv("LOG_FILEPATH"))

  #Se define la línea a agregar al historial como un dataframe de una sola línea, conformado por un diccionario
  row_to_add = pd.DataFrame(dict(zip(dataframe.columns.values, row)), index=[0])

  #Se define el dataframe como la concatenación del dataframe, es decir, el historial ya existente con la línea a agregar 
  dataframe = pd.concat([dataframe, row_to_add], ignore_index=True)

  #Se guarda el dataframe en un archivo CSV. Para esto, se accede al archivo .env cargado anteriormente y se obtiene la variable "LOG_FILEPATH"
  dataframe.to_csv(os.getenv("LOG_FILEPATH"), index=False)


def is_profile_in_sent_profiles(sent_profiles: pd.DataFrame, profile: str):

  profiles = [list(row)[1] for row in sent_profiles.itertuples(index=False)]

  return profile in profiles


def update_sent_profile(sent_profiles: list[list], profile: str):

  return None

    


def send_to_list_end(lista: list, profile: str):
  
  lista_aux =  lista

  if profile == lista_aux[len(lista_aux)-1][0][0]:
    return lista_aux

  while profile == lista_aux[0][0][0]:
    
    lista_aux.append([lista_aux[0][0], datetime.datetime.now()])
    
    del lista_aux[0]
  
  return lista_aux

