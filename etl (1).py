import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# Creating functions to stage data and insert rows in the fact and dimension tables.

def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

# Now we use the above two functions to stage data and insert rows in the fact and dimension tables in the Redshift cluster.
# The credentials are obtained from the .cfg file.

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get("DWH","DWH_ENDPOINT"), config.get("DWH","DWH_DB"), config.get("DWH","DWH_DB_USER"), config.get("DWH","DWH_DB_PASSWORD"), config.get("DWH","DWH_PORT")))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()