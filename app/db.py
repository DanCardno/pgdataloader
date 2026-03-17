import psycopg2
import os

def get_connection(database="postgres", host=None, port=None, user=None, password=None):
    return psycopg2.connect(
        database=database,
        user=user or os.getenv("PG_USER", "postgres"),
        password=password or os.getenv("PG_PASSWORD", "postgres"),
        host=host or os.getenv("PG_HOST", "db"),
        port=port or os.getenv("PG_PORT", "5432")
    )

def list_databases(host=None, port=None, user=None, password=None):
    conn = get_connection(host=host, port=port, user=user, password=password)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT datname FROM pg_database 
        WHERE datistemplate = false 
        AND datname != 'postgres'
        ORDER BY datname;
    """)
    databases = [row[0] for row in cursor.fetchall()]
    conn.close()
    return databases
