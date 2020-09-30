import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    '''
    Drop pre-existing tables if they are
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Create all tables from scratch
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    '''
    Read config file and setup connection with cluster, create tables
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    DWH_ENDPOINT           = config.get("HOST","DWH_ENDPOINT")       
    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    conn = psycopg2.connect(f"host={DWH_ENDPOINT} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()