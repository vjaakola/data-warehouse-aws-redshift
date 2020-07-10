import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



""" drop tables from drop table queries and printing the status
INPUTS:
* cur: cursor variable of the database
* conn: connection variable of the database
"""

def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Dropping table: " +query)
            print(e)

    print("Tables dropped")

""" create tables from drop table queries and printing the status
INPUTS:
* cur: cursor variable of the database
* conn: connection variable of the database
"""  
    
def create_tables(cur, conn):
    for query in create_table_queries:
        try:
            cur.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Error: Creating table: " + query)
            print(e)
    print("Tables created")


""" main function to connect database, using credentials from configuration file (dwh.cfg), dropping and creating tables

""" 
    
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
