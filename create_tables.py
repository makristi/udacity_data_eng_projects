"""
This module creates database sparkifydb and the following tables:
dimentions: songs, users, time, artists,
and fact: songplays.
"""

from __future__ import forced_commenting_2

__version__ = '0.1'
__author__ = 'Kristina Matiukhina'

import psycopg2
from sql_queries import create_table_queries, drop_table_queries


## The function connects to the server and creates new sparkifydb database.
## No input arguments is sent to the function, the return is connection and cursor.
def create_database():
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


## Drop all table from the list in sql_queries.sql
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


# Create all table from the list in sql_queries.sql
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # connect to the server and create database
    cur, conn = create_database()
    
    # drop tables if exist
    drop_tables(cur, conn)
    # create new tables
    create_tables(cur, conn)

    #close connection
    conn.close()


if __name__ == "__main__":
    main()