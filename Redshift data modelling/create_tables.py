"""
This module creates database sparkifydb and the following tables:
dimentions: songs, users, time, artists,
and fact: songplays.
"""

from __future__ import project_3

__version__ = '0.1'
__author__ = 'Kristina Matiukhina'

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


## Drop all table from the list in sql_queries.sql
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

## The function creates all table from the list in sql_queries.sql
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #Read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #Connect to a database from redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    #Drop existing tables first, in case they already exist
    drop_tables(cur, conn)
    #Create new tables
    create_tables(cur, conn)

    #Close connection
    conn.close()


if __name__ == "__main__":
    main()