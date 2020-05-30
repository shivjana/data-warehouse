import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import IaC
import sys

def load_staging_tables(cur, conn):
    """This module loads the data from the S3 bucket into the staging tables by executing the COPY command"""
    print("Copying Tables")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """This module executes insert queries and inserts rows into multiple tables"""
    print("Inserting Tables")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    DWH_DB = config.get('CLUSTER','DWH_DB')
    DWH_DB_USER = config.get('CLUSTER','DWH_DB_USER')
    DWH_DB_PASSWORD = config.get('CLUSTER','DWH_DB_PASSWORD')
    DWH_PORT = config.get('CLUSTER','DWH_PORT')
    HOST = config.get('CLUSTER','HOST')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST,DWH_DB,DWH_DB_USER,DWH_DB_PASSWORD,DWH_PORT))
    cur = conn.cursor()
    if sys.argv[1] == "delete_cluster":
        KEY=config.get('AWS','KEY')
        SECRET= config.get('AWS','SECRET')
        IaC.delete_redshift_cluster(KEY,SECRET,**config)
    else:
        load_staging_tables(cur, conn)
        insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()