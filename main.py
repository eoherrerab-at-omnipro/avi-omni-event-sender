#Se importa el módulo que permite la conversión de cadenas de carácteres a formato JSON
import json
#Se importa el módulo que permite realizar la conexión y la lectura de archivos en servidores SFTP
import sftp_connection
#Se importa el módulo que permite manejar la información de los dataflows
import data_handling
#Se importa el módulo que permite crear procesos al mismo tiempo
import multiprocessing
#Se importa el módulo que permite crear flujos de datos para el envío de información 
import dataflows

#Se evalua si se está iniciando la ejecutando desde la función principal
if __name__ == "__main__":

    #Se define la data de los dataflows como un JSON que contiene el producto de la función para obtener el archivo maestro
    dataflows_data = json.loads(s = sftp_connection.get_master_json_file())

    #Se define la información de los archivos como el producto de la función que permite obtener la información de los archivos en el SFTP
    files = sftp_connection.list_files()

    #Se define una lista de datos por flujo de datos
    data_for_pool = []

    #Se realiza una iteración sobre todos los elementos de la lista
    for file in files:
        
        #Se define la información del flujo de datos como el producto de la función para obtener dicha información a partir del archivo maestro
        dataflow_data = data_handling.get_dataflow_data(dataflows_data = dataflows_data, file_name = file["file_name"])

        #Se agrega un elemento a la lista
        data_for_pool.append([dataflow_data, file])

    #Se evalua si la lista
    if len(files) > 0:
        
        #Se define una piscina de procesos con la catidad de procesos como la cantidad de archivos leídos
        with multiprocessing.Pool(processes=len(files)) as pool:
            
            #Se define una respuesta de los procesos
            res = pool.starmap(dataflows.create_dataflow, data_for_pool)