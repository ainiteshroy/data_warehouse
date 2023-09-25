import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# Creating functions to drop and create required tables.

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

# Now we use the above two functions to drop and create tables in the Redshift cluster.
# The credentials are obtained from the .cfg file.

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(config.get("DWH","DWH_ENDPOINT"), config.get("DWH","DWH_DB"), config.get("DWH","DWH_DB_USER"), config.get("DWH","DWH_DB_PASSWORD"), config.get("DWH","DWH_PORT")))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()