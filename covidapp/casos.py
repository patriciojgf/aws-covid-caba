#!/usr/bin/env python3
import pandas as pd
import os
import sys
import boto3
import psycopg2
from io import StringIO

param_dic = {
    #"host"      : "localhost",
    "host"      : "-",
    "port"      : "5432",
    "database"  : "cabacovid",
    "user"      : "-",
    "password"  : "-"
}

def execute_query(conn, query):
    """ Execute a single query """

    ret = 0 # Return value
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    # If this was a select query, return the result
    if 'select' in query.lower():
        ret = cursor.fetchall()
    cursor.close()
    return ret

def connect(params_dic):
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit(1) 
    print("Connection successful")
    return conn

def copy_from_stringio(conn, df, table):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, index_label='id', header=False)
    buffer.seek(0)
    
    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, table, sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    cursor.close()



#download csv file from s3 bucket with boto3
s3 = boto3.resource('s3')
s3_object = s3.Object('covidbucket-s3', 'casos_covid19.csv')
#get response metadata from s3_object
status = s3_object.meta.client.head_bucket(Bucket='covidbucket-s3').get("ResponseMetadata", {}).get("HTTPStatusCode")
#validate status
if status == 200:
    print(f"Successful S3 get_object response. Status - {status}")
    #gen pandas from s3 object
    archivo_casos_covid = pd.read_csv(s3_object.get()['Body'],encoding='latin1', usecols=[1,4,6,7,8,9,10,13] ,dtype=str)
else:
    print(f"Unsuccessful S3 get_object response. Status - {status}")



archivo_casos_covid["edad"] = archivo_casos_covid[(archivo_casos_covid['edad'].notna())]["edad"].astype(int)
#Limpio el dataframe de casos que no son de CABA y registros mal cargados
archivo_casos_covid = archivo_casos_covid[\
                                            (archivo_casos_covid["provincia"]=="CABA")& \
                                            (archivo_casos_covid["edad"]>=0)& \
                                            (archivo_casos_covid["edad"]<150)& \
                                            (archivo_casos_covid['comuna'].notna())& \
                                            (archivo_casos_covid['genero'].notna()) \
                                          ]
 
#Convierto comuna a int
archivo_casos_covid['comuna'] = archivo_casos_covid['comuna'].astype(int)
 
#Cambio las edades por grupos etarios para trabajar los datos segmentados como los poblacionales
cortes_grupo_etario = [-1,30,40,50,60,70,80,90,150]
nombres_grupo_etario = [1,2,3,4,5,6,7,8]
archivo_casos_covid['grupo_etario_id'] = pd.cut(archivo_casos_covid['edad'], bins=cortes_grupo_etario,labels=nombres_grupo_etario,right=False).astype(int)
archivo_casos_covid.drop("edad",axis=1, inplace=True)
 
#Reemplazo genero por si id
dic_genero = {"masculino" : 1, "femenino" : 2 }
archivo_casos_covid['genero_id']=archivo_casos_covid['genero'].map(dic_genero).fillna(3).astype(int)
archivo_casos_covid.drop("genero",axis=1, inplace=True)
 
#Creo el dataframe que voy a usar para identificar la cantidad de contagios confirmados, filtro por CABA, Contagiado y fecha no nula
df_contagios = archivo_casos_covid[(archivo_casos_covid["clasificacion"]=="confirmado") &(archivo_casos_covid["fecha_apertura_snvs"].notna())\
                                  ][["fecha_apertura_snvs","comuna","genero_id","grupo_etario_id","tipo_contagio"]]
 
df_fallecidos = archivo_casos_covid[(archivo_casos_covid["fecha_fallecimiento"].notna()) \
                                  ][["fecha_fallecimiento","comuna","genero_id","grupo_etario_id"]]
 
#Creo una columna "fecha" en formato date y hago drop de la columna original
df_fallecidos.insert(1,'fecha', df_fallecidos["fecha_fallecimiento"].str[:9],True)
df_fallecidos["fecha"]=pd.to_datetime(df_fallecidos["fecha"])
df_fallecidos.drop("fecha_fallecimiento",axis=1, inplace=True)
 
df_contagios.insert(1,'fecha', df_contagios["fecha_apertura_snvs"].str[:9],True)
df_contagios["fecha"]=pd.to_datetime(df_contagios["fecha"])
df_contagios.drop("fecha_apertura_snvs",axis=1, inplace=True)
 
#Reemplazo el tipo de contagio por su id
dic_tipo_contagio = {"Comunitario" : 1, "En Investigación" : 2 ,"Contacto": 3,"Importado" : 4, "Trabajador de la Salud" : 5}
df_contagios['tipo_contagio_id'] = df_contagios['tipo_contagio'].map(dic_tipo_contagio).fillna(6).astype(int)
df_contagios.drop("tipo_contagio",axis=1, inplace=True)
 
#Agrupar y contar
df_contagios = df_contagios.value_counts(['fecha','comuna','genero_id','grupo_etario_id','tipo_contagio_id']).reset_index(name='cantidad').sort_values(by=['fecha','comuna','genero_id','grupo_etario_id','tipo_contagio_id'])
df_fallecidos = df_fallecidos.value_counts(['fecha','comuna','genero_id','grupo_etario_id']).reset_index(name='cantidad').sort_values(by=['fecha','comuna','genero_id','grupo_etario_id'])
#
 
df_contagios=df_contagios.set_index('fecha')
df_fallecidos=df_fallecidos.set_index('fecha')
 
conn = connect(param_dic)
copy_from_stringio(conn,df_contagios,'contagiados')
copy_from_stringio(conn,df_fallecidos,'fallecidos')
conn.close()
