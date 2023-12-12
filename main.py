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

    #Se realiza una iteración sobre todos los elementos que existan en la lista
    for file in files:

        #Se define la data del dataflow en especifico asociado al archivo como el producto de la función que permite obtener la información del dataflow a partir del archivo maestro
        dataflow_data = data_handling.get_dataflow_data(dataflows_data = dataflows_data, file_name = file["file_name"])

        #Se evalua si se encuentra el dataflow correspondiente
        if dataflow_data:

            #Se define un nuevo proceso, usando la función para crear un nuevo flujo de datos internamente
            process =  multiprocessing.Process(target = dataflows.create_dataflow, args = [dataflow_data, file])

            #Se inicia el proceso definido anteriormente    
            process.start()