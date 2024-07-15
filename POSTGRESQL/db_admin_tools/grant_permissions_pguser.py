import psycopg2
from psycopg2 import sql
from datetime import datetime
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv('.env.production')

BACKUP_USER = os.getenv('DB_BPUSER')
DB_NAME = os.getenv('DB_DEFAULT')
LOG_FILE = "log/grant_permissions_pguser.log"

# Configuración de conexión
db_config = {
    "host": os.getenv('DB_HOST'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "port": os.getenv('DB_PORT'),
}

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as f:
        f.write(f"{timestamp} - {message}\n")

# Establecer conexión con PostgreSQL
def connect_to_postgres(dbname):
    try:
        conn = psycopg2.connect(dbname=dbname, **db_config)
        return conn
    except psycopg2.OperationalError as e:
        log_message(f"Connection - Error al conectar a la DB {dbname}: {e}")
    except psycopg2.Error as e:
        log_message(f"Connection - Error inesperado de psycopg2: {e}")
    return None

# Obtener lista de bases de datos
def get_databases(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("SELECT datname FROM pg_database WHERE datistemplate = false;")
            )
            databases = [row[0] for row in cursor.fetchall()]
            log_message(f"Bases de datos obtenidas: {len(databases)}")
        return databases
    except psycopg2.Error as e:
        log_message(f"Error al obtener la lista de bases de datos: {e}")
        return []

# Obtener lista de esquemas en una base de datos
def get_schemas(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                sql.SQL("""
                    SELECT schema_name FROM information_schema.schemata
                    WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                    AND schema_name NOT LIKE 'pg_temp_%'
                    AND schema_name NOT LIKE 'pg_toast_temp_%';
                """)
            )
            schemas = [row[0] for row in cursor.fetchall()]
        return schemas
    except psycopg2.Error as e:
        log_message(f"Error al obtener la lista de esquemas: {e}")
        return []

# Otorgar permisos al usuario backup_user en cada base de datos
def grant_permissions():
    conn_ = connect_to_postgres(DB_NAME)
    if not conn_:
        return

    try:
        databases = get_databases(conn_)

        for db in databases:
            conn_db = connect_to_postgres(db)
            if not conn_db:
                continue

            try:
                schemas = get_schemas(conn_db)

                sql_statements = [
                    sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                        sql.Identifier(db), sql.Identifier(BACKUP_USER)),
                ]

                for esquema in schemas:
                    sql_statements.extend([
                        sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(
                            sql.Identifier(esquema), sql.Identifier(BACKUP_USER)),

                        sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA {} TO {}").format(
                            sql.Identifier(esquema), sql.Identifier(BACKUP_USER)),
                        sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT SELECT ON TABLES TO {}").format(
                            sql.Identifier(esquema), sql.Identifier(BACKUP_USER)),

                        sql.SQL("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA {} TO {}").format(
                            sql.Identifier(esquema), sql.Identifier(BACKUP_USER)),
                        sql.SQL("ALTER DEFAULT PRIVILEGES IN SCHEMA {} GRANT USAGE, SELECT ON SEQUENCES TO {}").format(
                            sql.Identifier(esquema), sql.Identifier(BACKUP_USER)),
                    ])

                with conn_db.cursor() as cursor:
                    cursor.execute("BEGIN;")
                    for statement in sql_statements:
                        cursor.execute(statement)
                    cursor.execute("COMMIT;")

                log_message(f"INFO:Permisos otorgados para usuario:[{BACKUP_USER}] - DB: [{db}]")
            except psycopg2.Error as e:
                log_message(f"Error al otorgar permisos usuario:[{BACKUP_USER}] - DB: [{db}]: {e}")
                conn_db.rollback()

            finally:
                conn_db.close()
    finally:
        conn_.close()

def main():
    grant_permissions()

    log_message("______________________________________________________________________________________________________________________________")
    print(f"Proceso de otorgamiento de permisos completado. Detalles en el archivo {LOG_FILE}")

if __name__ == "__main__":
    main()