import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, select_number_rows_queries

def load_staging_tables(cur, conn):
    '''
    Exract data from files stored in S3 to the staging tables
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Load and Transform data from staging tables into the dimensional tables
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
        
def get_results(cur):
    '''
    Check ETL results
    '''
    for query in select_number_rows_queries:
        print('Running ' + query)
        cur.execute(query)
        results = cur.fetchone()

        for row in results:
            print("   ", row)


def main():
    '''
    Setup connection and run ETL pipelines
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
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    get_results(cur)

    conn.close()


if __name__ == "__main__":
    main()