#Se importa el módulo que permite la conversión de cadenas de carácteres a formato JSON
import json
#Se importa el módulo que permite realizar la conexión y la lectura de archivos en servidores SFTP
import sftp_connection
#Se importa el módulo que permite manejar la información de los dataflows
import data_handling
#Se importa el módulo que permite crear procesos al mismo tiempo
import multiprocessing
#Se importa el módulo que permite enviar generar el token de acceso
import adobe_requests
#Se importa el módulo que permite crear flujos de datos para el envío de información 
import dataflows
#Se importa el módulo que permite crear clientes HTTP. Para esto, se debe usar pip install httpx
import httpx

#Se evalua si se está iniciando la ejecutando desde la función principal
if __name__ == "__main__":

    #Se define la data de los dataflows como un JSON que contiene el producto de la función para obtener el archivo maestro
    dataflows_data = json.loads(s = sftp_connection.get_master_json_file())

    #Se define la información de los archivos como el producto de la función que permite obtener la información de los archivos en el SFTP
    files = sftp_connection.list_files()

    #Se define una lista de datos por flujo de datos
    data_for_pool = []

    #Se define el cliente HTTP como el objeto de cliente sin tiempo de finalización
    http_client = httpx.Client(timeout=None)

    #Se define el token de acceso como el producto de la función para la generación del token de acceso
    access_token = adobe_requests.generate_access_token(client = http_client)

    #Se cierra el cliente HTTP usando el cliente definido anteriormente
    http_client.close()

    #Se realiza una iteración sobre todos los elementos de la lista
    for file in files:
        
        #Se define la información del flujo de datos como el producto de la función para obtener dicha información a partir del archivo maestro
        dataflow_data = data_handling.get_dataflow_data(dataflows_data = dataflows_data, file_name = file["file_name"])

        #Se evalua si existe información asociada a este flujo de datos
        if dataflow_data:

            #Se agrega un elemento a la lista
            data_for_pool.append((dataflow_data, file, access_token["access_token"]))

    #Se evalua si la lista de archivos con información tiene menos de 10 elementos
    if len(data_for_pool) < 10:

        #Se define una piscina de procesos con la cantidad de procesos como la cantidad de archivos leídos
        with multiprocessing.Pool(processes = len(data_for_pool)) as pool:
            
            #Se define una respuesta de los procesos como la ejecución de los flujos mismos
            res = pool.starmap_async(dataflows.create_dataflow, data_for_pool)

            #Se espera la ejecución de los flujos
            res.wait()
    
    else:

        #Se define una piscina de procesos con la catidad de procesos como la cantidad de archivos leídos
        with multiprocessing.Pool(processes = 10) as pool:
            
            #Se define una respuesta de los procesos como la ejecución de los flujos mismos
            res = pool.starmap_async(dataflows.create_dataflow, data_for_pool)

            #Se espera la ejecución de los flujos
            res.wait()
