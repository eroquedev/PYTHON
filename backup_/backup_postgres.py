import os
import subprocess
from datetime import datetime, timedelta
import shutil
from dotenv import load_dotenv
import logging
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cargar las variables de entorno desde el archivo .env
load_dotenv('.env.production')

# Configuración
BACKUP_DIR = "backup/"
RETENTION_DAYS = 7
LOG_FILE = "log/backup_postgres.log"

PSQL_PATH = os.getenv('PSQL_PATH', 'psql')
PG_DUMP_PATH = os.getenv('PG_DUMP_PATH', 'pg_dump')

PGHOST = os.getenv('DB_HOST')
DB_BUSER = os.getenv('DB_BPUSER')
DB_BPASSWORD = os.getenv('DB_BPUSERPASS')

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_NAME = os.getenv('DB_DEFAULT')

# Configurar la variable de entorno para la contraseña
os.environ['PGPASSWORD'] = DB_BPASSWORD

# Configuración de logging
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format='%(asctime)s - %(message)s')

#Crea el directorio de backup con la fecha actual.
def create_backup_dir():
    date_str = datetime.now().strftime("%Y-%m-%d")
    backup_path = os.path.join(BACKUP_DIR, date_str)
    os.makedirs(backup_path, exist_ok=True)
    return backup_path

#Registra un mensaje en el archivo de log.
def log_message(message):
    logging.info(message)

#Actualiza el estado del backup en la base de datos.
def update_backup_status(db, success=True):
    try:
        with psycopg2.connect(host=PGHOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE backup_dbs 
                    SET last_backup_date = CURRENT_TIMESTAMP, backup_status = %s
                    WHERE datname = %s
                """, (success, db))
                conn.commit()
    except Exception as e:
        log_message(f"ERROR - Al actualizar el estado del backup para {db}: {e}")

#Obtiene la lista de bases de datos a respaldar.
def get_databases_to_backup(limit):
    try:
        with psycopg2.connect(host=PGHOST, user=DB_BUSER, password=DB_BPASSWORD, dbname=DB_NAME) as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT datname FROM backup_dbs 
                    WHERE backup_status = false
                    AND (DATE(last_backup_date) <> DATE(CURRENT_DATE + INTERVAL '1 day') OR last_backup_date IS NULL)
                    ORDER BY datname
                    LIMIT %s
                """
                cur.execute(query, (limit,))
                databases = [row[0] for row in cur.fetchall()]

        log_message(f"INFO - Bases de datos obtenidas: {len(databases)}")
        return databases
    except Exception as e:
        log_message(f"ERROR - al obtener la lista de bases de datos: {e}")
        return []

#Realiza el backup de una base de datos.
def backup_database(db, backup_path):
    try:
        backup_file = os.path.join(backup_path, f"{db}.backup")
        start_time = datetime.now()

        with open(backup_file, 'wb') as f:
            subprocess.run([PG_DUMP_PATH, '-h', PGHOST, '-U', DB_BUSER, '-d', db, '-F', 'c'], stdout=f, check=True)

        end_time = datetime.now()
        file_size = os.path.getsize(backup_file)
        log_message(f"INFO - Backup completado para DB: [{db}] en {end_time - start_time}, tamaño del archivo: {file_size} bytes")

        update_backup_status(db, success=True)
    except subprocess.CalledProcessError as e:
        log_message(f"ERROR - Al respaldar la base de datos {db}: {e}")
        update_backup_status(db, success=False)

#Elimina backups antiguos según el período de retención.
def delete_old_backups():
    retention_date = datetime.now() - timedelta(days=RETENTION_DAYS)
    log_message(f"Eliminando backups anteriores a {retention_date.strftime('%Y-%m-%d')}")

    for folder in os.listdir(BACKUP_DIR):
        folder_path = os.path.join(BACKUP_DIR, folder)
        if os.path.isdir(folder_path):
            try:
                folder_date = datetime.strptime(folder, "%Y-%m-%d")
                if folder_date < retention_date:
                    shutil.rmtree(folder_path)
                    log_message(f"INFO - Backup antiguo eliminado: {folder_path}")
            except ValueError:
                # No es una carpeta con formato de fecha válido, ignorar
                continue
            except Exception as e:
                log_message(f"ERROR - Al eliminar el backup antiguo {folder}: {e}")

#Función principal que orquesta el proceso de backups.
def main():
    backup_path = create_backup_dir()
    log_message(f"Directorio de backup: {backup_path}")

    batch_size = 10  # Cantidad de bases de datos por lote
    num_workers = os.cpu_count() or 4  # Número de trabajadores según los recursos disponibles

    databases = get_databases_to_backup(limit=batch_size)

    if not databases:
        log_message("INFO - No se encontraron bases de datos para respaldar.")
        log_message("---")
        return

    log_message(f"INFO - Usando {num_workers} hilos para el proceso de backup")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(backup_database, db, backup_path): db for db in databases}
        for future in as_completed(futures):
            db = futures[future]
            try:
                future.result()
            except Exception as e:
                log_message(f"ERROR - Al procesar la base de datos {db}: {e}")

    delete_old_backups()
    log_message("---")
    print(f"Proceso de backups completados. Detalles en el archivo {LOG_FILE}")

if __name__ == "__main__":
    main()
