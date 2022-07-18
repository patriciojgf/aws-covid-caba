#!/usr/bin/env python3
import pandas as pd
import os
import sys
import boto3
import psycopg2
from io import StringIO

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


s3 = boto3.resource('s3')

#rango_edades
s3_object = s3.Object('covidbucket-s3', 'rangos_edades.csv')
status = s3_object.meta.client.head_bucket(Bucket='covidbucket-s3').get("ResponseMetadata", {}).get("HTTPStatusCode")
if status == 200:
    print(f"Successful S3 get_object response rango_edades. Status - {status}")
    df_rangos_edades = pd.read_csv(s3_object.get()['Body'])
    df_rangos_edades = df_rangos_edades.set_index('grupo_etario_id')
else:
    print(f"Unsuccessful S3 get_object response rango_edades. Status - {status}")
    
#tipo_contagio
s3_object = s3.Object('covidbucket-s3', 'tipo_contagio.csv')
status = s3_object.meta.client.head_bucket(Bucket='covidbucket-s3').get("ResponseMetadata", {}).get("HTTPStatusCode")
if status == 200:
    print(f"Successful S3 get_object response tipo_contagio. Status - {status}")
    df_tipo_contagio = pd.read_csv(s3_object.get()['Body'])
    df_tipo_contagio = df_tipo_contagio.set_index('tipo_contagio_id')
else:
    print(f"Unsuccessful S3 get_object response tipo_contagio. Status - {status}")
    
#tipos_genero
s3_object = s3.Object('covidbucket-s3', 'tipos_genero.csv')
status = s3_object.meta.client.head_bucket(Bucket='covidbucket-s3').get("ResponseMetadata", {}).get("HTTPStatusCode")
if status == 200:
    print(f"Successful S3 get_object response tipos_genero. Status - {status}")
    df_tipos_genero = pd.read_csv(s3_object.get()['Body'])
    df_tipos_genero = df_tipos_genero.set_index('genero_id')
else:
    print(f"Unsuccessful S3 get_object response tipos_genero. Status - {status}")
    
#tipos_vacuna
s3_object = s3.Object('covidbucket-s3', 'tipos_vacuna.csv')
status = s3_object.meta.client.head_bucket(Bucket='covidbucket-s3').get("ResponseMetadata", {}).get("HTTPStatusCode")
if status == 200:
    print(f"Successful S3 get_object response tipos_vacuna. Status - {status}")
    df_tipos_vacunas = pd.read_csv(s3_object.get()['Body'])
    df_tipos_vacunas = df_tipos_vacunas.set_index('vacuna_id')
else:
    print(f"Unsuccessful S3 get_object response tipos_vacuna. Status - {status}")

conn = connect(param_dic)

copy_from_stringio(conn,df_rangos_edades,'grupo_etario')
copy_from_stringio(conn,df_tipos_vacunas,'vacuna')
copy_from_stringio(conn,df_tipos_genero,'genero')
copy_from_stringio(conn,df_tipo_contagio,'tipo_contagio')

conn.close()