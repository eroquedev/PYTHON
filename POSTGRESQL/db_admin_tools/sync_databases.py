import psycopg2
import logging
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv('.env.production')

# Configuración
PGHOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_DEFAULT')

# Configuración de logging
LOG_FILE = "log/sync_databases.log"
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(message)s')

def log_message(message):
    """Log a message to the log file."""
    logging.info(message)

def connect_to_database():
    """Establish a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(host=PGHOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)
        return conn
    except psycopg2.DatabaseError as e:
        log_message(f"Error al conectar a la base de datos: {e}")
        raise

def sync_databases(conn):
    """Synchronize databases by calling the PostgreSQL function."""
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT sync_databases()")
            conn.commit()
            log_message("Synchronize databases completada exitosamente.")
    except psycopg2.Error as e:
        conn.rollback()
        log_message(f"Error al sincronizar las bases de datos: {e}")
        raise

def main():
    """Main function to synchronize databases."""
    try:
        with connect_to_database() as conn:
            sync_databases(conn)
    except Exception as e:
        log_message(f"Proceso fallido: {e}")
    finally:
        log_message("---")
        print(f"Proceso de sincronización completado. Detalles en el archivo {LOG_FILE}")

if __name__ == "__main__":
    main()