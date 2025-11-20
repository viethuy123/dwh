\c dwh;
CREATE SERVER staging_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', dbname 'stg', port '5432');

CREATE USER MAPPING FOR dev_user
  SERVER staging_server
  OPTIONS (user 'dev_user', password 'dev_password');
 
IMPORT FOREIGN SCHEMA information_schema
  LIMIT TO (tables)
  FROM SERVER staging_server
  INTO fdw_metadata;


\c dtm;
CREATE SERVER warehouse_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', dbname 'dwh', port '5432');

CREATE USER MAPPING FOR dev_user
  SERVER warehouse_server
  OPTIONS (user 'dev_user', password 'dev_password');

IMPORT FOREIGN SCHEMA information_schema
  LIMIT TO (tables)
  FROM SERVER warehouse_server
  INTO fdw_metadata;

CREATE OR REPLACE PROCEDURE sync_fdw_tables(
    local_schema TEXT,
    remote_schema TEXT,
    server_name TEXT,
    metadata_schema TEXT DEFAULT 'fdw_metadata'
)
LANGUAGE plpgsql
AS $$
DECLARE
    foreign_table TEXT;
    remote_tables TEXT[];
BEGIN
    
    EXECUTE format(
        'SELECT array_agg(table_name) FROM %I.tables WHERE table_schema = %L AND table_type = %L',
        metadata_schema, remote_schema, 'BASE TABLE'
    ) INTO remote_tables;

    IF remote_tables IS NOT NULL THEN
        FOREACH foreign_table IN ARRAY remote_tables
        LOOP
            RAISE NOTICE 'Syncing table: %', foreign_table;

            EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.%I CASCADE', local_schema, foreign_table);

            EXECUTE format($f$
                IMPORT FOREIGN SCHEMA %I
                LIMIT TO (%I)
                FROM SERVER %I
                INTO %I;
            $f$, remote_schema, foreign_table, server_name, local_schema);
            
        END LOOP;
    END IF;

    FOR foreign_table IN
        SELECT ft.relname
        FROM pg_foreign_table f
        JOIN pg_class ft ON f.ftrelid = ft.oid
        JOIN pg_namespace ns ON ft.relnamespace = ns.oid
        WHERE ns.nspname = local_schema
    LOOP
        IF NOT (foreign_table = ANY(remote_tables)) THEN
            RAISE NOTICE 'Removing orphaned table: %', foreign_table;
            EXECUTE format('DROP FOREIGN TABLE IF EXISTS %I.%I CASCADE', local_schema, foreign_table);
        END IF;
    END LOOP;

END;
$$;
