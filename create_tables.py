import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import IaC


def drop_tables(cur, conn):
    """This module drops the tables if they already exist"""
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(e)
        conn.commit()


def create_tables(cur, conn):
    """This module creates tables"""
    for query in create_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(e)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read('dwh.cfg')
    
    KEY=config.get('AWS','KEY')
    SECRET= config.get('AWS','SECRET')
    DWH_IAM_ROLE_NAME = config.get('CLUSTER','DWH_IAM_ROLE_NAME')
    ClusterIdentifier = config.get('CLUSTER','DWH_CLUSTER_IDENTIFIER')
    
    roleArn,iam = IaC.create_iam_role(KEY,SECRET,DWH_IAM_ROLE_NAME)
    config['IAM_ROLE']['ARN'] = roleArn
    
    myClusterProps,redshift = IaC.create_redshift_cluster(KEY,SECRET,roleArn,**config['CLUSTER'])
    
    config['CLUSTER']['HOST'] = myClusterProps['Endpoint']['Address']
    config['IAM_ROLE']['DWH_ROLE_ARN'] = myClusterProps['IamRoles'][0]['IamRoleArn']
    with open('dwh.cfg', 'w') as configfile:
        config.write(configfile)
        
    print("Connecting to DB")
    DWH_DB = config.get('CLUSTER','DWH_DB')
    DWH_DB_USER = config.get('CLUSTER','DWH_DB_USER')
    DWH_DB_PASSWORD = config.get('CLUSTER','DWH_DB_PASSWORD')
    DWH_PORT = config.get('CLUSTER','DWH_PORT')
    HOST = config.get('CLUSTER','HOST')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST,DWH_DB,DWH_DB_USER,DWH_DB_PASSWORD,DWH_PORT))
    
    cur = conn.cursor()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()
    #IaC.delete_redshift_cluster(KEY,SECRET,**config)

if __name__ == "__main__":
    main()