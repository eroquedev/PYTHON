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

def create_backup_dir():
    """Crear directorio de backup con la fecha actual"""
    date_str = datetime.now().strftime("%Y-%m-%d")
    backup_path = os.path.join(BACKUP_DIR, date_str)
    os.makedirs(backup_path, exist_ok=True)
    return backup_path

def log_message(message):
    """Registrar un mensaje en el log"""
    logging.info(message)

def update_backup_status(successful_dbs):
    """Actualizar el estado del backup en la base de datos"""
    try:
        conn = psycopg2.connect(host=PGHOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)
        cur = conn.cursor()
        cur.execute("""
            UPDATE backup_dbs 
            SET last_backup_date = CURRENT_TIMESTAMP, backup_status = TRUE
            WHERE datname = ANY(%s);
        """, (successful_dbs,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log_message(f"ERROR - Al actualizar el estado del backup para {successful_dbs}: {e}")

def get_databases_to_backup(limit):
    """Obtener la lista de bases de datos que necesitan backup"""
    try:
        conn = psycopg2.connect(host=PGHOST, user=DB_BUSER, password=DB_BPASSWORD, dbname=DB_NAME)
        cur = conn.cursor()
        query = """SELECT datname FROM backup_dbs
                WHERE backup_status = false
                AND (DATE(last_backup_date) <> DATE(CURRENT_DATE + INTERVAL '1 day') OR last_backup_date IS NULL)
                ORDER BY rank
                LIMIT %s;"""
        cur.execute(query, (limit,))
        databases = [row[0] for row in cur.fetchall()]
        cur.close()
        conn.close()
        log_message(f"Bases de datos obtenidas: {len(databases)}")
        return databases
    except Exception as e:
        log_message(f"ERROR - Al obtener la lista de bases de datos: {e}")
        return []

def backup_database(db, backup_path):
    """Realizar el backup de una base de datos"""
    try:
        backup_file = os.path.join(backup_path, f"{db}.backup")
        start_time = datetime.now()
        with open(backup_file, 'wb') as f:
            subprocess.run([PG_DUMP_PATH, '-h', PGHOST, '-U', DB_BUSER, '-d', db, '-F', 'c'], stdout=f, check=True)
        end_time = datetime.now()
        file_size = os.path.getsize(backup_file)
        log_message(f"INFO - Backup completado para DB: [{db}] en {end_time - start_time}, tamaño del archivo: {file_size} bytes")
        return db, True
    except subprocess.CalledProcessError as e:
        log_message(f"ERROR - Al respaldar la base de datos {db}: {e}")
        return db, False

def delete_old_backups():
    """Eliminar backups antiguos que exceden el período de retención"""
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

def main():
    """Función principal para la ejecución del script de backup"""
    backup_path = create_backup_dir()
    log_message(f"Directorio de backup: {backup_path}")

    batch_size = 3  # Cantidad de bases de datos por lote
    num_workers = 3  # Número de trabajadores

    while True:
        databases = get_databases_to_backup(limit=batch_size)

        if not databases:
            log_message("INFO - No se encontraron bases de datos para respaldar.")
            break

        log_message(f"Usando {num_workers} hilos para el proceso de backup")

        successful_dbs = []

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(backup_database, db, backup_path): db for db in databases}
            for future in as_completed(futures):
                db = futures[future]
                try:
                    db, success = future.result()
                    if success:
                        successful_dbs.append(db)
                except Exception as e:
                    log_message(f"ERROR - Al procesar la base de datos {db}: {e}")

        if successful_dbs:
            update_backup_status(successful_dbs)

    delete_old_backups()
    log_message("---")
    print(f"Proceso de backups completados. Detalles en el archivo {LOG_FILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_message(f"ERROR - Error inesperado: {e}")
        print(f"Error inesperado: {e}. Revisa el log para más detalles.")