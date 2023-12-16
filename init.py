#Se importa el módulo que permite la creación de subprocesos por multiplexación
import subprocess

#Se define el comando a ejecutar
command = "python3.11 -B main.py"

#Se define un subproceso usando el comando definido anteriormente y definiendo la ejecución del proceso por tuberías
process = subprocess.Popen(command, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)