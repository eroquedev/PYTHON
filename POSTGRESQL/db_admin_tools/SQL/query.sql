CREATE USER USERNAME WITH PASSWORD 'PASSWORD';

CREATE TABLE IF NOT EXISTS backup_dbs (
    datname TEXT PRIMARY KEY,
    size TEXT NOT NULL,
    rank INTEGER NOT null,
    last_backup_date TIMESTAMP,
    backup_status BOOLEAN DEFAULT false,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION sync_databases()
RETURNS VOID AS $$
BEGIN
    -- Insertar las bases de datos nuevas
    INSERT INTO backup_dbs (datname, size, rank)
    SELECT datname, 
    pg_size_pretty(pg_database_size(datname)) AS size,
    ROW_NUMBER() OVER (ORDER BY pg_database_size(datname) ASC) AS rank
    FROM pg_database 
    WHERE datistemplate = false
    AND datname NOT IN (SELECT datname FROM backup_dbs)
    ORDER BY pg_database_size(datname) ASC
    ON CONFLICT (datname) DO NOTHING; -- Evitar errores en caso de conflictos

    -- Eliminar las bases de datos que ya no existen
    DELETE FROM backup_dbs
    WHERE datname NOT IN (
        SELECT datname
        FROM pg_database 
        WHERE datistemplate = false
    );

	-- Actualizar la columna updated_at y backup_status para las bases de datos existentes
	UPDATE backup_dbs AS b
	SET updated_at = CURRENT_TIMESTAMP,
	    backup_status = CASE 
	                      WHEN b.last_backup_date IS NULL THEN false
	                      WHEN b.last_backup_date::date <> DATE(CURRENT_DATE + INTERVAL '1 day') THEN false 
	                      ELSE b.backup_status 
	                    END
	WHERE b.datname IN (
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
WHERE
    backup_status = false
    AND (DATE(last_backup_date) <> DATE(CURRENT_DATE + INTERVAL '1 day') OR last_backup_date IS NULL)
ORDER BY rank
LIMIT 3 OFFSET 0;

/*
SELECT *
FROM backup_dbs 
WHERE last_backup_date IS NOT NULL
AND DATE(last_backup_date) <> DATE(CURRENT_DATE + INTERVAL '1 day');
*/