import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

""" load data from S3 buckets to staging tables using copy command
INPUTS:
* cur: cursor variable of the database
* conn: connection variable of the database
"""

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('Executing loading staging tables' +query)
        cur.execute(query)
        conn.commit()
        print('Executed loading staging tables' +query)
 
""" insert data from staging tables to fact and dimension tables
INPUTS:
* cur: cursor variable of the database
* conn: connection variable of the database
"""    

def insert_tables(cur, conn):
    for query in insert_table_queries:
        print('Executing create'+query)
        cur.execute(query)
        conn.commit()

        
""" main function to connect database, using credentials from configuration file (dwh.cfg), load S3 bucket data to staging tables and loads final tables, closes the database connection

""" 
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
