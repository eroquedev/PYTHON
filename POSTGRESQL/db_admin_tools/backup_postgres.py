import os
import subprocess
from datetime import datetime, timedelta
import shutil
from dotenv import load_dotenv
import logging
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed

# Cargar las variables de entorno desde el archivo .env
load_dotenv('.env.local')

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

GPGNAME = os.getenv('GPG_NAME')
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

def update_backup_status(databases, status):
    """Actualizar el estado del backup en la base de datos"""
    try:
        conn = psycopg2.connect(host=PGHOST, user=DB_USER, password=DB_PASSWORD, dbname=DB_NAME)
        cur = conn.cursor()
        cur.execute("""
            UPDATE backup_dbs 
            SET status = %s,
            last_backup_date = CURRENT_TIMESTAMP
            WHERE datname = ANY(%s);
        """, (status, databases))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log_message(f"ERROR - Al actualizar el estado del backup para {databases}: {e}")

def get_databases_to_backup(limit):
    """Obtener la lista de bases de datos que necesitan backup"""
    try:
        conn = psycopg2.connect(host=PGHOST, user=DB_BUSER, password=DB_BPASSWORD, dbname=DB_NAME)
        cur = conn.cursor()
        query = """SELECT datname FROM backup_dbs
                WHERE status = 'PENDING'
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

def encrypt_file_with_gpg(file_path):
    """Cifrar un archivo usando GPG"""
    try:
        encrypted_file_path = file_path + '.gpg'
        subprocess.run(['gpg', '--yes', '--output', encrypted_file_path, '--encrypt', '--recipient', GPGNAME, file_path], check=True)
        os.remove(file_path)
        return encrypted_file_path
    except subprocess.CalledProcessError as e:
        log_message(f"ERROR - Al cifrar el archivo {file_path} con GnuPG: {e}")
        if os.path.exists(file_path):
            os.remove(file_path)  # Eliminar el archivo de backup en caso de error
        return None

def backup_database(db, backup_path):
    """Realizar el backup de una base de datos y cifrar el archivo"""
    backup_file = os.path.join(backup_path, f"{db}.backup")
    
    try:
        update_backup_status([db], 'IN_PROGRESS')
        start_time = datetime.now()
        
        with open(backup_file, 'wb') as f:
            subprocess.run([PG_DUMP_PATH, '-h', PGHOST, '-U', DB_BUSER, '-d', db, '-F', 'c'], stdout=f, stderr=subprocess.PIPE, check=True)
        
        encrypted_backup_file = encrypt_file_with_gpg(backup_file)
        if not encrypted_backup_file:
            update_backup_status([db], 'FAILED')
            return db, False
        
        end_time = datetime.now()
        file_size = os.path.getsize(encrypted_backup_file)
        log_message(f"INFO - Backup completado y cifrado para DB: [{db}] en {end_time - start_time}, size del archivo: {file_size} bytes")
        update_backup_status([db], 'SUCCESS')
        return db, True

    except subprocess.CalledProcessError as e:
        error_message = ""
        if e.stderr:
            try:
                error_message = e.stderr.decode().strip()
            except Exception as decode_error:
                # log_message(f"ERROR - Decodificación de stderr fallida: {decode_error}")
                error_message = str(e)

        if not error_message:
            error_message = str(e)
        
        log_message(f"ERROR - Al respaldar la base de datos {db}: {error_message}")

        if "permiso denegado" in error_message.lower():
            update_backup_status([db], 'NO_PERMISSIONS')
        else:
            update_backup_status([db], 'FAILED')

        if os.path.exists(backup_file):
            os.remove(backup_file)
        return db, False
    except Exception as e:
        log_message(f"ERROR - backup_database - Al respaldar la base de datos {db}: {e}")
        if os.path.exists(backup_file):
            os.remove(backup_file)
        update_backup_status([db], 'FAILED')
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

    max_failed_attempts = 3
    consecutive_failed_batches = 0

    while True:
        databases = get_databases_to_backup(limit=batch_size)

        if not databases:
            log_message("INFO - No se encontraron bases de datos para respaldar.")
            break

        log_message(f"Usando {num_workers} hilos para el proceso de backup")

        successful_dbs = []
        failed_dbs = []

        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {executor.submit(backup_database, db, backup_path): db for db in databases}
            for future in as_completed(futures):
                db = futures[future]
                try:
                    db, success = future.result()
                    if success:
                        successful_dbs.append(db)
                    else:
                        failed_dbs.append(db)
                except Exception as e:
                    log_message(f"ERROR - Al procesar la base de datos {db}: {e}")
                    failed_dbs.append(db)

        if successful_dbs:
            log_message(f"INFO - Bases de datos respaldadas con éxito: {successful_dbs}")
            consecutive_failed_batches = 0

        if failed_dbs:
            log_message(f"ERROR - Bases de datos que fallaron al respaldar: {failed_dbs}")
            # Incrementar el contador de lotes fallidos consecutivos
            consecutive_failed_batches += 1
            if consecutive_failed_batches >= max_failed_attempts:
                log_message(f"ERROR - Se alcanzó el número máximo de lotes fallidos consecutivos [{max_failed_attempts}]. Deteniendo el proceso de backup.")
                break

    delete_old_backups()
    log_message("---")
    print(f"Proceso de backups completados. Detalles en el archivo {LOG_FILE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_message(f"ERROR - Error inesperado: {e}")
        print(f"Error inesperado: {e}. Revisa el log para más detalles.")