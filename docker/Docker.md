# Comandos basicos de Docker

## Construir imagen de Docker
```
docker build -t nombre-de-imagen:etiqueta-de-imagen -f ruta/al/archivo/Dockerfile ruta/al/desarrollo/a/dockerizar
```
## Exportar imagen de Docker
```
docker save -o nombre-de-archivo-de-imagen.tar nombre-de-imagen:etiqueta-de-imagen
```

## Crear contenedor interactivo sin autoeliminación
```
docker run -it --name nombre-de-contenedor-interactivo nombre-de-imagen:etiqueta-de-imagen /bin/bash
```

## Crear contenedor interactivo con autoeliminación
```
docker run -it --name nombre-de-contenedor-interactivo --rm nombre-de-imagen:etiqueta-de-imagen /bin/bash
```

## Crear imagen de Docker a partir de contenedor
```
docker commit nombre-de-contenedor-interactivo nuevo-nombre-de-imagen:nueva-etiqueta-de-imagen
```

## Listar imagenes de Docker
```
docker images
```

## Listar contenedores de Docker
```
docker ps
```

## Listar contenedores de Docker incluso los no activos
```
docker ps -a
```

## Cargar imagen de Docker exportada a Docker
```
docker load -i nombre-de-archivo-de-imagen.tar
```
