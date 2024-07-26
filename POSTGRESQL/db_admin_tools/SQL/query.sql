CREATE USER USERNAME WITH PASSWORD 'PASSWORD';

CREATE TABLE IF NOT EXISTS backup_dbs (
    datname TEXT PRIMARY KEY,
    size text,
    rank INTEGER,
    status VARCHAR(20) DEFAULT 'PENDING',
    last_backup_date TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON COLUMN backup_dbs.status IS 'PENDING: Indica que la base de datos está pendiente de ser respaldada.
IN_PROGRESS: Indica que el proceso de respaldo de la base de datos está en curso.
SUCCESS: Indica que el respaldo de la base de datos se completó exitosamente.
FAILED: Indica que el respaldo de la base de datos falló.
NO_PERMISSIONS: Indica que el usuario no tiene permisos para respaldar la base de datos.';

CREATE OR REPLACE FUNCTION sync_databases()
RETURNS VOID AS $$
begin

    -- Insertar nuevas bases de datos
    INSERT INTO backup_dbs (datname, size, status, updated_at)
    SELECT datname,
    pg_size_pretty(pg_database_size(datname)) AS size,
    'PENDING', CURRENT_TIMESTAMP
    FROM pg_database
    WHERE datistemplate = false
    AND datname NOT IN (SELECT datname FROM backup_dbs);

    -- Eliminar bases de datos que ya no existen
    DELETE FROM backup_dbs
    WHERE datname NOT IN (
        SELECT datname
        FROM pg_database
        WHERE datistemplate = false
    );

    -- Actualizar el estado al PENDING y ordenar las bases de datos por el tamaño
    WITH RankedDatabases AS (
        SELECT datname,
            ROW_NUMBER() OVER (ORDER BY pg_database_size(datname) ASC) AS rank
        FROM pg_database
        WHERE datistemplate = false
    )
    UPDATE backup_dbs
    SET status = 'PENDING',
        rank = rd.rank
    FROM RankedDatabases rd
    WHERE backup_dbs.datname = rd.datname;

EXCEPTION
    WHEN OTHERS THEN
        -- Lanzar una excepción con el mensaje de error
        RAISE EXCEPTION 'Error sincronizando bases de datos: %', SQLERRM;
END;
$$ LANGUAGE plpgsql;

SELECT sync_databases();

SELECT *
FROM backup_dbs 
WHERE status = 'PENDING'
ORDER BY rank
LIMIT 3 OFFSET 0;