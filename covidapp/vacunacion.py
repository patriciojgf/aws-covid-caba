import psycopg2
from io import StringIO
import sys
import os
import boto3
import pandas as pd
import numpy as np


param_dic = {
    #"host"      : "localhost",
    "host"      : "cabacovid.cs4nf40vsq8e.us-east-1.rds.amazonaws.com",
    "port"      : "5432",
    "database"  : "cabacovid",
    "user"      : "covid_app",
    "password"  : "covid_app"
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
    
#unzip file from s3 bucket and create a dataframe


s3 = boto3.resource('s3')

#s3_object = s3.Object('covidbucket-s3', 'datos_nomivac_covid19.zip')
#get response metadata from s3_object
#status = s3_object.meta.client.head_bucket(Bucket='covidbucket-s3').get("ResponseMetadata", {}).get("HTTPStatusCode")

bucket = s3.Bucket('covidbucket-s3')
bucket.download_file('datos_nomivac_covid19.zip', 'datos_nomivac_covid19.zip')
txt = pd.read_csv("datos_nomivac_covid19.zip",encoding = "utf-8",compression='zip',sep=',',chunksize=100000,usecols=['fecha_aplicacion','sexo','grupo_etario','orden_dosis','vacuna','jurisdiccion_residencia_id','jurisdiccion_aplicacion_id','depto_residencia_id'],dtype=str)

#delete datos_nomivac_covid19.zip
os.remove("datos_nomivac_covid19.zip")

#txt = pd.read_csv(s3_object.get()['Body'],encoding = "utf-8",compression='zip',sep=',',chunksize=100000,usecols=['fecha_aplicacion','sexo','grupo_etario','orden_dosis','vacuna','jurisdiccion_residencia_id'],dtype=str)

df_s = []
#txt = pd.read_csv('https://sisa.msal.gov.ar/datos/descargas/covid-19/files/datos_nomivac_covid19.zip',encoding = "utf-8",compression='zip',sep=',',chunksize=100000,usecols=['fecha_aplicacion','sexo','grupo_etario','orden_dosis','vacuna','jurisdiccion_residencia_id','jurisdiccion_aplicacion_id','depto_residencia_id'],dtype=str)
for df in txt:
    df_s.append(df[( df['jurisdiccion_residencia_id']=='02' ) & ( df['jurisdiccion_aplicacion_id']=='02' )])
df_f = pd.concat(df_s,ignore_index = True)
df_s = []
df_f['depto_residencia_id'] = df_f['depto_residencia_id'].astype(int)
df_f['orden_dosis'] = df_f['orden_dosis'].astype(int)
df_f['fecha_aplicacion'] = pd.to_datetime(df_f['fecha_aplicacion'])
df2=df_f
df2.rename(columns={'fecha_aplicacion':'FECHA','sexo':'GENERO_ID','grupo_etario':'GRUPO_ETARIO_ID','orden_dosis':'DOSIS','vacuna':'VACUNA_ID','depto_residencia_id':'COMUNA_ID'}, inplace=True)

tipos_vacuna = {'AstraZeneca':'1', 'Moderna':'2', 'Sinopharm':'3','Sputnik':'4','Cansino':'5','Pfizer':'6','Covishield':'7'}
df2.VACUNA_ID = df2.VACUNA_ID.apply(lambda x: 'Pfizer' if 'pfizer' in x.lower() else x)
df2.VACUNA_ID = df2.VACUNA_ID.apply(lambda x: 'Cansino' if 'cansino' in x.lower() else x)
df2.VACUNA_ID = df2.VACUNA_ID.apply(lambda x: 'AstraZeneca' if 'astrazeneca' in x.lower() else x)
df2.VACUNA_ID = df2.VACUNA_ID.apply(lambda x: 'Moderna' if 'moderna' in x.lower() else x)
df2.VACUNA_ID = df2.VACUNA_ID.apply(lambda x: 'Sinopharm' if 'sinopharm' in x.lower() else x)
df2.VACUNA_ID = df2.VACUNA_ID.apply(lambda x: 'Sputnik' if 'sputnik' in x.lower() else x)
df2.VACUNA_ID = df2.VACUNA_ID.apply(lambda x: 'Covishield' if 'covishield' in x.lower() else x)
df2['VACUNA_ID']  = df2['VACUNA_ID'].map(tipos_vacuna,'7')

tipos_genero = {'F':'1','M':'2','S.I.':3,'X':3}
df2['GENERO_ID'] = df2['GENERO_ID'].map(tipos_genero,'3')

df2.GRUPO_ETARIO_ID.replace(['<12','12-17','18-29',\
                            '30-39',\
                            '40-49',\
                            '50-59',\
                            '60-69',\
                            '70-79',\
                            '90-99','80-89','>=100',\
                            'S.I.'],['1','1','1','2','3','4','5','6','7','7','7','8'],inplace=True)

df2 = df2.groupby(['FECHA','GENERO_ID','GRUPO_ETARIO_ID','DOSIS','VACUNA_ID','COMUNA_ID']).size().reset_index(name='CANTIDAD')

df2=df2.set_index('FECHA')
conn = connect(param_dic)

#execute_query(conn, "DELETE FROM PUBLIC.vacunacion")
copy_from_stringio(conn,df2,'vacunacion')
conn.close()



