"""
This module reads all json files in ./data folder and
populates tables created from create_table script. 
"""

from __future__ import project3

__version__ = '0.1'
__author__ = 'Kristina Matiukhina'

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


## Load data from files in S3 into staging tables
## This is a middle step between files and final tables 
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


## Transform data and load them from stging tables into final ones
def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # Read config file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Connect to a database on redshift cluster
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Load data to staging tables
    load_staging_tables(cur, conn)
    #Transform and load data to final tables
    insert_tables(cur, conn)
    
    #Close connection
    conn.close()


if __name__ == "__main__":
    main()