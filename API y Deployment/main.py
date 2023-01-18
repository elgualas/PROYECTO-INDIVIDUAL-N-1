from fastapi import FastAPI
import pandas as pd

app = FastAPI()

amazon_url='https://raw.githubusercontent.com/elgualas/datos/main/amazon.csv'
disney_url='https://raw.githubusercontent.com/elgualas/datos/main/disney.csv'
hulu_url='https://raw.githubusercontent.com/elgualas/datos/main/hulu.csv'
netflix_url='https://raw.githubusercontent.com/elgualas/datos/main/netflix.csv'

netflix_df = pd.read_csv(netflix_url).fillna('')
amazon_df = pd.read_csv(amazon_url).fillna('')
hulu_df = pd.read_csv(hulu_url).fillna('')
disney_df = pd.read_csv(disney_url).fillna('')


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/items/{item_id}")
def read_item(item_id: int):
    return {"item_id": item_id}

@app.get("/primera_consulta/{plat}/{word}")
async def read_item(plat: str, word: str):
    if plat=='netflix':
        n=len(netflix_df[netflix_df['title'].str.contains(word)])
        return {plat:n}
    elif plat=='amazon':
        n=len(amazon_df[amazon_df['title'].str.contains(word)])
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
        n=len(netflix_df.loc[(netflix_df['score'].astype(int)>puntaje)&(netflix_df['release_year'].astype(int)==ano)&(netflix_df['type']=='movie')])
        return {plat:n}
    elif plat=='amazon':
        n=len(amazon_df.loc[(amazon_df['score'].astype(int)>puntaje)&(amazon_df['release_year'].astype(int)==ano)&(amazon_df['type']=='movie')])
        return {plat:n}
    elif plat=='disney':
        n=len(disney_df.loc[(disney_df['score'].astype(int)>puntaje)&(disney_df['release_year'].astype(int)==ano)&(disney_df['type']=='movie')])
        return {plat:n}
    elif plat=='hulu':
        n=len(hulu_df.loc[(hulu_df['score'].astype(int)>puntaje)&(hulu_df['release_year'].astype(int)==ano)&(hulu_df['type']=='movie')])
        return {plat:n}

@app.get("/tercera_consulta/{plat}")
async def read_item(plat: str):
    if plat=='netflix':
        n=netflix_df.loc[(netflix_df['score'].astype(int)==netflix_df['score'].astype(int).max())&(netflix_df['type']=='movie')].sort_values('title')
        n=n['title'].to_list()[1]
        return {plat:n}
    elif plat=='amazon':
        n=amazon_df.loc[(amazon_df['score'].astype(int)==amazon_df['score'].astype(int).max())&(amazon_df['type']=='movie')].sort_values('title')
        n=n['title'].to_list()[1]
        return {plat:n}
    elif plat=='disney':
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
        n=netflix_df.loc[(netflix_df['release_year'].astype(int)==ano)&(netflix_df['duration_type']==tipo)].sort_values('duration_int',ascending=False)
        titulo=n['title'].to_list()[0]
        duracion=n['duration_int'].to_list()[0]
        return [plat,titulo,duracion]
    elif plat=='amazon':
        amazon_df['duration_int']=amazon_df['duration_int'].astype('int64')
        n=amazon_df.loc[(amazon_df['release_year'].astype(int)==ano)&(amazon_df['duration_type']==tipo)].sort_values('duration_int',ascending=False)
        titulo=n['title'].to_list()[0]
        duracion=n['duration_int'].to_list()[0]
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

@app.get("/quinta_consulta/{rating}")
async def read_item(rating: str):
    a=pd.concat([netflix_df,amazon_df,hulu_df,disney_df])
    return (rating,a.loc[(a['rating']==rating)].shape[0])