CREATE USER USERNAME WITH PASSWORD 'PASSWORD';

CREATE TABLE IF NOT EXISTS backup_dbs (
    datname TEXT PRIMARY KEY,
    last_backup_date TIMESTAMP,
    backup_status BOOLEAN DEFAULT false,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION sync_databases()
RETURNS VOID AS $$
BEGIN
    -- Insertar las bases de datos nuevas
    INSERT INTO backup_dbs (datname)
    SELECT datname
    FROM pg_database 
    WHERE datistemplate = false
    AND datname NOT IN (SELECT datname FROM backup_dbs)
    ON CONFLICT (datname) DO NOTHING; -- Evitar errores en caso de conflictos

    -- Eliminar las bases de datos que ya no existen
    DELETE FROM backup_dbs
    WHERE datname NOT IN (
        SELECT datname
        FROM pg_database 
        WHERE datistemplate = false
    );

    -- Actualizar la columna updated_at para las bases de datos existentes
    UPDATE backup_dbs
    SET updated_at = CURRENT_TIMESTAMP
    WHERE datname IN (
        SELECT datname
        FROM pg_database 
        WHERE datistemplate = false
    );

EXCEPTION
    WHEN OTHERS THEN
        -- Lanzar una excepción con el mensaje de error
        RAISE EXCEPTION 'Error sincronizando bases de datos: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

SELECT sync_databases();

SELECT *
FROM backup_dbs 
WHERE DATE(last_backup_date) <> DATE(CURRENT_DATE + INTERVAL '1 day')  OR last_backup_date IS null
AND backup_status = false;

/*
SELECT *
FROM backup_dbs 
WHERE last_backup_date IS NOT NULL
AND DATE(last_backup_date) <> DATE(CURRENT_DATE + INTERVAL '1 day');
*/