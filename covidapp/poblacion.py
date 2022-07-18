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



#download csv file from s3 bucket with boto3
s3 = boto3.resource('s3')
s3_object = s3.Object('covidbucket-s3', 'PBP_CO1025.xls')
#get response metadata from s3_object
status = s3_object.meta.client.head_bucket(Bucket='covidbucket-s3').get("ResponseMetadata", {}).get("HTTPStatusCode")
#validate status

#read excel to df
varones_df = pd.read_excel(s3_object.get()['Body'].read(),'2022', skiprows = 24, nrows=18,  usecols = 'A:Q')
mujeres_df = pd.read_excel(s3_object.get()['Body'].read(),'2022', skiprows = 46, nrows=18,  usecols = 'A:Q')
#delete total fields
varones_df.drop('Total', axis=1, inplace=True)
varones_df.rename(columns={'Grupo de edad (años)':'grupo_etario_id'}, inplace=True)
varones_df = pd.melt(varones_df,id_vars='grupo_etario_id',value_vars=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15],var_name='Comuna',value_name='Cantidad')
varones_df = varones_df.set_index('grupo_etario_id')
varones_df = varones_df.drop("Total")

varones_df.reset_index(inplace=True)
varones_df.replace(['0-4','5-9','10-14','15-19','20-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74','75-79','80 y más'],['1','1','1','1','1','1','2','2','3','3','4','4','5','5','6','6','7'],inplace=True)


mujeres_df.drop('Total', axis=1, inplace=True)
mujeres_df.rename(columns={'Grupo de edad (años)':'grupo_etario_id'}, inplace=True)
mujeres_df = pd.melt(mujeres_df,id_vars='grupo_etario_id',value_vars=[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15],var_name='Comuna',value_name='Cantidad')
mujeres_df = mujeres_df.set_index('grupo_etario_id')
mujeres_df = mujeres_df.drop("Total")

mujeres_df.reset_index(inplace=True)
mujeres_df.replace(['0-4','5-9','10-14','15-19','20-24','25-29','30-34','35-39','40-44','45-49','50-54','55-59','60-64','65-69','70-74','75-79','80 y más'],['1','1','1','1','1','1','2','2','3','3','4','4','5','5','6','6','7'],inplace=True)


varones_df['genero_id'] = 1
mujeres_df['genero_id'] = 2

poblacion_df = pd.concat([varones_df,mujeres_df],axis=0)

poblacion_df = poblacion_df[['Comuna','genero_id','grupo_etario_id','Cantidad']]
poblacion_df.set_index(['Comuna','genero_id','grupo_etario_id'],inplace=True)

poblacion_df = poblacion_df.groupby(['Comuna','genero_id','grupo_etario_id'])[['Cantidad']].sum()

#Cargo la tabla
conn = connect(param_dic)
copy_from_stringio(conn,poblacion_df,'poblacion')
conn.close()