#!/usr/bin/env python3
import psycopg2
from io import StringIO
import sys

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





conn = connect(param_dic)
execute_query(conn, "DELETE FROM PUBLIC.vacunacion")
execute_query(conn, "DELETE FROM PUBLIC.contagiados")
execute_query(conn, "DELETE FROM PUBLIC.fallecidos")
execute_query(conn, "DELETE FROM PUBLIC.poblacion")
execute_query(conn, "DELETE FROM PUBLIC.grupo_etario;DELETE FROM PUBLIC.vacuna;DELETE FROM PUBLIC.genero;DELETE FROM PUBLIC.tipo_contagio;")
conn.close()