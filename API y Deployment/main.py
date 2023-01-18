from fastapi import FastAPI                                 #importamos las librerias 
import pandas as pd                                         #a utilizar

app = FastAPI()

amazon_url='https://raw.githubusercontent.com/elgualas/datos/main/amazon.csv'       #Extraemos la data ya transformada de mi repositorio
disney_url='https://raw.githubusercontent.com/elgualas/datos/main/disney.csv'       #para evitar exceso de espacio
hulu_url='https://raw.githubusercontent.com/elgualas/datos/main/hulu.csv'
netflix_url='https://raw.githubusercontent.com/elgualas/datos/main/netflix.csv'

netflix_df = pd.read_csv(netflix_url).fillna('')                #convertimos los CSV a dataframe y reemplazamos los valores nulos
amazon_df = pd.read_csv(amazon_url).fillna('')
hulu_df = pd.read_csv(hulu_url).fillna('')
disney_df = pd.read_csv(disney_url).fillna('')


@app.get("/primera_consulta/{plat}/{word}")                             #Primera funcion para la primera consulta:
async def read_item(plat: str, word: str):                              #Esta consiste en leer 2 caracteres 'plataforma' en la cual se buscara
    if plat=='netflix':                                                 #en su respectivo dataset la palabra 'word' y devolver el numero de veces
        n=len(netflix_df[netflix_df['title'].str.contains(word)])       #que aparece.
        return {plat:n}                                                 #La funcion busca en la columna 'title' dicha palabra con el metodo 
    elif plat=='amazon':                                                #contains() y devuelve la cantidad de filas con el metodo len().
        n=len(amazon_df[amazon_df['title'].str.contains(word)])         #Y asi con cada plataforma.
        return {plat:n}
    elif plat=='disney':
        n=len(disney_df[disney_df['title'].str.contains(word)])
        return {plat:n}
    elif plat=='hulu':
        n=len(hulu_df[hulu_df['title'].str.contains(word)])
        return {plat:n}

@app.get("/segunda_consulta/{plat}/{puntaje}/{ano}")
async def read_item(plat: str, puntaje: int, ano: int):
    if plat=='netflix':
        n=len(netflix_df.loc[(netflix_df['score'].astype(int)>puntaje)&(netflix_df['release_year'].astype(int)==ano)&(netflix_df['type']=='movie')])    #Para la segunda consulta, esta funcion lee 3 parametros
        return {plat:n}                                                                                                                                 #'plataforma','puntaje' y 'año'
    elif plat=='amazon':                                                                                                                                #para devolver el numero de peliculas mayores a ese puntaje
        n=len(amazon_df.loc[(amazon_df['score'].astype(int)>puntaje)&(amazon_df['release_year'].astype(int)==ano)&(amazon_df['type']=='movie')])        #del año indicado.
        return {plat:n}                                                                                                                                 #Para esta funcion se ejecuto una triple condicional
    elif plat=='disney':                                                                                                                                #buscando ser mayor al 'puntaje' en la columna score,
        n=len(disney_df.loc[(disney_df['score'].astype(int)>puntaje)&(disney_df['release_year'].astype(int)==ano)&(disney_df['type']=='movie')])        #tener el mismo 'año' en la columna 'release_year' 
        return {plat:n}                                                                                                                                 #y ser del tipo 'movie' en la columna 'type'.
    elif plat=='hulu':                                                                                                                                  #devolviendo la cantidad de filas con el metodo len().
        n=len(hulu_df.loc[(hulu_df['score'].astype(int)>puntaje)&(hulu_df['release_year'].astype(int)==ano)&(hulu_df['type']=='movie')])                #Y asi con cada plataforma
        return {plat:n}

@app.get("/tercera_consulta/{plat}")
async def read_item(plat: str):
    if plat=='netflix':
        n=netflix_df.loc[(netflix_df['score'].astype(int)==netflix_df['score'].astype(int).max())&(netflix_df['type']=='movie')].sort_values('title')   #Para la tercera consulta, la funcion solo leera el parametro
        n=n['title'].to_list()[1]                                                                                                                       #'plataforma' para devolver la segunda mejor pelicula segun orden
        return {plat:n}                                                                                                                                 #alfabetico, en este caso buscamos el mayor puntaje de la columna
    elif plat=='amazon':                                                                                                                                #'score' con el metodo max() y ordenando de manera alfabetica con
        n=amazon_df.loc[(amazon_df['score'].astype(int)==amazon_df['score'].astype(int).max())&(amazon_df['type']=='movie')].sort_values('title')       #el metodo sort_values() la columna 'title', el dataframe de respuesta
        n=n['title'].to_list()[1]                                                                                                                       #lo convertimos en una lista, especificamente la columna title, con 
        return {plat:n}                                                                                                                                 #el metodo to_list() y retornamos su elemento [1].
    elif plat=='disney':                                                                                                                                #Y asi con cada plataforma.
        n=disney_df.loc[(disney_df['score'].astype(int)==disney_df['score'].astype(int).max())&(disney_df['type']=='movie')].sort_values('title')
        n=n['title'].to_list()[1]
        return {plat:n}
    elif plat=='hulu':
        n=hulu_df.loc[(hulu_df['score'].astype(int)==hulu_df['score'].astype(int).max())&(hulu_df['type']=='movie')].sort_values('title')
        n=n['title'].to_list()[1]
        return {plat:n}

@app.get("/cuarta_consulta/{plat}/{tipo}/{ano}")
async def read_item(plat: str, tipo: str, ano: int):
    if plat=='netflix':
        netflix_df['duration_int']=netflix_df['duration_int'].astype('int64')
        n=netflix_df.loc[(netflix_df['release_year'].astype(int)==ano)&(netflix_df['duration_type']==tipo)].sort_values('duration_int',ascending=False) #Para la cuarta consulta, la funcion leera 3 parametros
        titulo=n['title'].to_list()[0]                                                                                                                  #'plataforma', 'tipo' y 'año' los cuales sera usados para devolver
        duracion=n['duration_int'].to_list()[0]                                                                                                         #la pelicula que mas duro, en este caso se hace una doble condicion
        return [plat,titulo,duracion]                                                                                                                   #a las columnas 'release_year' y 'duration_type' para ser filtradas por 
    elif plat=='amazon':                                                                                                                                #los parametros 'tipo' y 'año', despues se ordena de manera descendente
        amazon_df['duration_int']=amazon_df['duration_int'].astype('int64')                                                                             #con el metodo sort_values() el dataframe resultante los volvemos lista
        n=amazon_df.loc[(amazon_df['release_year'].astype(int)==ano)&(amazon_df['duration_type']==tipo)].sort_values('duration_int',ascending=False)    #con el metodo to_list() especificamente las columnas 'title' y 'duration_int'
        titulo=n['title'].to_list()[0]                                                                                                                  #de estas listas retornamos su elemento [0].
        duracion=n['duration_int'].to_list()[0]                                                                                                         #Y asi para cada plataforma.    
        return [plat,titulo,duracion]
    elif plat=='disney':
        disney_df['duration_int']=disney_df['duration_int'].astype('int64')
        n=disney_df.loc[(disney_df['release_year'].astype(int)==ano)&(disney_df['duration_type']==tipo)].sort_values('duration_int',ascending=False)
        titulo=n['title'].to_list()[0]
        duracion=n['duration_int'].to_list()[0]
        return [plat,titulo,duracion]
    elif plat=='hulu':
        hulu_df['duration_int']=hulu_df['duration_int'].astype('int64')
        n=hulu_df.loc[(hulu_df['release_year'].astype(int)==ano)&(hulu_df['duration_type']==tipo)].sort_values('duration_int',ascending=False)
        titulo=n['title'].to_list()[0]
        duracion=n['duration_int'].to_list()[0]
        return [plat,titulo,duracion]

@app.get("/quinta_consulta/{rating}")                               #Para la quinta y ultima consulta, diseñamos la funcion para que lea
async def read_item(rating: str):                                   #el parametro 'rating' y asi devolver la cantidad de series y peliculas
    a=pd.concat([netflix_df,amazon_df,hulu_df,disney_df])           #con esa caracteristica sin importar la plataforma.
    return (rating,a.loc[(a['rating']==rating)].shape[0])           #Para esto unimos todos los dataframes con el metodo concat() y filtramos
                                                                    #la columna 'rating' con nuestro unico parametro y devolvemos el numero de filas
                                                                    #usando el metodo shape[0] el cual es similar a len().