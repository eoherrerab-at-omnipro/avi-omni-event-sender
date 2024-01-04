# Usa una imagen base de Python 3.11
FROM python:3.11

# Copia el contenido de tu proyecto al contenedor
COPY . /app

# Establece el directorio de trabajo en /app
WORKDIR /app

# Instala las dependencias requeridas utilizando pip
RUN pip install -r requirements/requirements.txt

# Comando para ejecutar tu aplicación principal
CMD ["python", "-B", "main.py"]
