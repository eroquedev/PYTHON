import os
import psycopg2
from psycopg2 import sql
from psycopg2.errors import DependentObjectsStillExist, UndefinedObject
from datetime import datetime
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env
load_dotenv('.env.production')

# Archivo de log
LOG_FILE = 'log/revoke_drop_pguser.log'

# Configuración de conexión
db_config = {
    "host": os.getenv('DB_HOST'),
    "user": os.getenv('DB_USER'),
    "password": os.getenv('DB_PASSWORD'),
    "port": os.getenv('DB_PORT'),
}
DB_NAME = os.getenv('DB_DEFAULT')

# Nombre del usuario a eliminar
user_to_drop = os.getenv('DB_BPUSER')

#Escribe un mensaje en el archivo de log con una marca de tiempo.
def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, 'a') as log:
        log.write(f"{timestamp} - {message}\n")

#Conecta a la base de datos especificada y devuelve la conexión.
def connect_to_database(dbname):
    try:
        conn = psycopg2.connect(dbname=dbname, **db_config)
        conn.autocommit = True
        return conn
    except psycopg2.OperationalError as e:
        log_message(f"ERROR:Connection - Error al conectar a la DB {dbname}: {e}")
    except psycopg2.Error as e:
        log_message(f"ERROR:Connection - Error inesperado de psycopg2: {e}")
    return None

#Verifica si el usuario especificado existe en la base de datos.
def user_exists(conn, user):
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT 1 FROM pg_roles WHERE rolname = %s"), [user])
            exists = cursor.fetchone() is not None
            print(exists)
        return exists
    except psycopg2.Error as e:
        log_message(f"ERROR: al verificar la existencia del usuario {user}: {e}")
        return False

#Obtiene la lista de bases de datos no plantillas.
def get_databases(conn):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT datname FROM pg_database WHERE datistemplate = false;")
            databases = [row[0] for row in cursor.fetchall()]
            log_message(f"Bases de datos obtenidas: {len(databases)}")
        return databases
    except psycopg2.Error as e:
        log_message(f"Error al obtener la lista de bases de datos: {e}")
        return []

#Revoca los privilegios y elimina objetos del usuario especificado en la base de datos.
def revoke_privileges_and_drop_user(conn, dbname):
    try:
        with conn.cursor() as cur:
            cur.execute(sql.SQL("REVOKE ALL PRIVILEGES ON DATABASE {} FROM {}").format(
                sql.Identifier(dbname),
                sql.Identifier(user_to_drop)
            ))

            cur.execute(sql.SQL("DROP OWNED BY {}").format(
                sql.Identifier(user_to_drop)
            ))

            log_message(f"INFO: Privilegios revocados y objetos eliminados en la base de datos [{dbname}]")

    except (DependentObjectsStillExist, UndefinedObject) as e:
        log_message(f"ERROR: en la base de datos {dbname}: {e}")
    except psycopg2.Error as e:
        log_message(f"ERROR: general en la base de datos {dbname}: {e}")

#Revoca privilegios y elimina el usuario en todas las bases de datos.
def drop_user_everywhere():
    conn = connect_to_database(DB_NAME)
    if not conn:
        return

    if not user_exists(conn, user_to_drop):
        log_message(f"ERROR: Usuario {user_to_drop} no existe en PostgreSQL.")
        return

    try:
        databases = get_databases(conn)

        for db in databases:
            conn_db = connect_to_database(db)
            if conn_db:
                revoke_privileges_and_drop_user(conn_db, db)
                conn_db.close()

        with connect_to_database(DB_NAME) as conn_postgres:
            if conn_postgres:
                with conn_postgres.cursor() as cur:
                    cur.execute(sql.SQL("DROP USER IF EXISTS {}").format(
                        sql.Identifier(user_to_drop)
                    ))
                    log_message(f"INFO: Usuario {user_to_drop} eliminado en PostgreSQL")
    except psycopg2.Error as e:
        log_message(f"ERROR: al realizar operaciones: {e}")

def main():
    drop_user_everywhere()
    log_message("---")
    print(f"Proceso de revocados y eliminar usuario completado. Detalles en el archivo {LOG_FILE}")

if __name__ == "__main__":
    main()