import os
import subprocess
from datetime import datetime, timedelta
import shutil
from dotenv import load_dotenv
import os

# Cargar las variables de entorno desde el archivo .env
load_dotenv('.env.production')

# Configuración
BACKUP_DIR = "backup/"
RETENTION_DAYS = 7
LOG_FILE = "log/backup_postgres.log"

PSQL_PATH = os.getenv('PSQL_PATH')
PG_DUMP_PATH = os.getenv('PG_DUMP_PATH')

PGHOST = os.getenv('DB_HOST')
DB_USER = os.getenv('DB_BPUSER')
DB_PASSWORD = os.getenv('DB_BPUSERPASS')
DB_NAME = os.getenv('DB_DEFAULT')

# Configurar la variable de entorno para la contraseña
os.environ['PGPASSWORD'] = DB_PASSWORD

def create_backup_dir():
    date_str = datetime.now().strftime("%Y-%m-%d")
    backup_path = os.path.join(BACKUP_DIR, date_str)
    os.makedirs(backup_path, exist_ok=True)
    return backup_path

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, 'a') as log:
        log.write(f"{timestamp} - {message}\n")

def get_databases():
    try:
        query = "SELECT datname FROM pg_database WHERE datistemplate = false"
        result = subprocess.run(
            [PSQL_PATH, '-h', PGHOST, '-U', DB_USER, '-d', DB_NAME, '-t', '-c', query],
            capture_output=True, text=True, check=True
        )
        databases = result.stdout.strip().split()
        log_message(f"Bases de datos obtenidas: {len(databases)}")
        return databases
    except subprocess.CalledProcessError as e:
        log_message(f"ERROR: al obtener la lista de bases de datos: {e}")
        return []

def backup_database(db, backup_path):
    try:
        backup_file = os.path.join(backup_path, f"{db}.backup")
        start_time = datetime.now()

        with open(backup_file, 'wb') as f:
            subprocess.run([PG_DUMP_PATH, '-h', PGHOST, '-U', DB_USER, '-d', db, '-F', 'c'], stdout=f, check=True)

        end_time = datetime.now()
        file_size = os.path.getsize(backup_file)
        log_message(f"Backup completado para DB: [{db}] en {end_time - start_time}, size del archivo: {file_size} bytes")
    except subprocess.CalledProcessError as e:
        log_message(f"Error al respaldar la base de datos {db}: {e}")

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
                    log_message(f"Backup antiguo eliminado: {folder_path}")
            except ValueError:
                # No es una carpeta con formato de fecha válido, ignorar
                continue
            except Exception as e:
                log_message(f"Error al eliminar el backup antiguo {folder}: {e}")

def main():
    backup_path = create_backup_dir()
    log_message(f"Directorio de backup: {backup_path}")

    databases = get_databases()
    if not databases:
        log_message("No se encontraron bases de datos para respaldar.")
        log_message("______________________________________________________________________________________________________________________________")
        return

    for db in databases:
        backup_database(db, backup_path)

    delete_old_backups()
    log_message("______________________________________________________________________________________________________________________________")
    print(f"Proceso de backups completados. Detalles en el archivo {LOG_FILE}")

if __name__ == "__main__":
    main()