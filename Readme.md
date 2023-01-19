# Primer Proyecto Individual

Para este proyecto se busco emplear el rol de un Data Engineer, para la extraccion, tranformacion, carga y visualización de datos, todo esto
mediante el desarrollo de una API, la cual es una interfaz que comunica dos aplicaciones y brinda acceso simple a los datos requeridos.

![](/_src/diagrama_Api.png)

## Tranformacion de data
En esta consigna se nos entrego data en formato.CSV para que sea ordenada, limpiada y modificada, para lo cual usamos la libreria pandas para 
trabajarlos como dataframes, algunas tranformaciones que se nos pide son la adicion de una nueva columna 'id', cambio de formato de las fechas, cambio
a minusculas de todos los datos y entre otras tranformaciones mas.** La ejecucion de estas tranformaciones y explicacion de cada linea de codigo esta en el archivo transformaciones.ipynb **
En este caso la data esta relacionada a plataformas de streaming.

![](/_src/datos.png)

## API y Deployment
Para la parte del desarrollo de la API se usara el framework FastAPI y para el deployment usaremos Deta.

![](/_src/fastapi_deta.png)

Para la api se requerira diseñar consultas, en este caso piden 5 ** las cuales estan detalladas y comentadas en el archivo main.py **. Luego para la parte de deployment de la aplicacion se usara Deta.

## Resultados
## Url primera consulta
https://rh3nt9.deta.dev/primera_consulta/netflix/love
## Url segunda consulta
https://rh3nt9.deta.dev/segunda_consulta/netflix/85/2010
## Url tercera consulta
https://rh3nt9.deta.dev/tercera_consulta/amazon
## Url cuarta consulta
https://rh3nt9.deta.dev/cuarta_consulta/netflix/min/2016
## Url quinta consulta
https://rh3nt9.deta.dev/quinta_consulta/18+