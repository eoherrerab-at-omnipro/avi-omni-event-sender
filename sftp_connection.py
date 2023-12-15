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

dotenv.load_dotenv(dotenv_path = "config/sftp.env")

def list_files() -> list[dict]:
    
    connection_options = pysftp.CnOpts()

    connection_options.hostkeys = None

    sftp = pysftp.Connection(os.getenv("HOSTNAME"), username=os.getenv("USER"), password=os.getenv("PASSWORD"), cnopts=connection_options)

    folder_paths = os.getenv("FOLDER_PATHS").split(sep="|")

    files = []

    today = datetime.datetime.today()

    for folder_path in folder_paths:

        if sftp.listdir(remotepath = folder_path):

            for file_path in sftp.listdir(remotepath = folder_path):

                row_to_verify = [sftp.stat(remotepath = folder_path + file_path).st_mtime, os.path.basename(file_path), folder_path + file_path]

                if os.path.splitext(file_path)[0].endswith(today.strftime("%Y%m%d")) and not data_handling.is_row_in_log(row = row_to_verify):

                    files.append({
                        "file_name": file_path,
                        "last_modify_file": str(sftp.stat(remotepath = folder_path + file_path).st_mtime),
                        "file_content": sftp.open(remote_file = folder_path + file_path, mode = "r").read().decode(encoding="utf-8")
                    })

                    data_handling.write_row_in_log(row = row_to_verify)

    sftp.close()

    return files

def get_master_json_file() -> str:

    connection_options = pysftp.CnOpts()

    connection_options.hostkeys = None

    sftp = pysftp.Connection(os.getenv("HOSTNAME"), username=os.getenv("USER"), password=os.getenv("PASSWORD"), cnopts=connection_options)

    content = sftp.open(remote_file = os.getenv("MASTER_PATH"), mode = "r").read().decode(encoding="utf-8")

    sftp.close()

    return content