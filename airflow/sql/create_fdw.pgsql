\c dwh;
CREATE SERVER staging_server
  FOREIGN DATA WRAPPER postgres_fdw
  OPTIONS (host 'localhost', dbname 'stg', port '5432');
CREATE USER MAPPING FOR dev_user
  SERVER staging_server
  OPTIONS (user 'dev_user', password 'dev_password');
IMPORT FOREIGN SCHEMA src_jisseki
  FROM SERVER staging_server
  INTO jisseki_fdw;
IMPORT FOREIGN SCHEMA src_jira
  FROM SERVER staging_server
  INTO jira_fdw;
IMPORT FOREIGN SCHEMA src_create
  FROM SERVER staging_server
  INTO create_fdw;

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

IMPORT FOREIGN SCHEMA public
  FROM SERVER warehouse_server
  INTO dwh_fdw;

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
    existing_tables TEXT[];
BEGIN

    SELECT array_agg(ft.relname)
    INTO existing_tables
    FROM pg_foreign_table f
    JOIN pg_class ft ON f.ftrelid = ft.oid
    JOIN pg_namespace ns ON ft.relnamespace = ns.oid
    WHERE ns.nspname = local_schema;

    FOR foreign_table IN
        EXECUTE format(
            'SELECT table_name FROM %I.tables WHERE table_schema = %L AND table_type = %L',
            metadata_schema, remote_schema, 'BASE TABLE'
        )
    LOOP
        IF existing_tables IS NULL OR NOT foreign_table = ANY(existing_tables) THEN
            RAISE NOTICE 'Importing new table: %', foreign_table;

            EXECUTE format($f$
                IMPORT FOREIGN SCHEMA %I
                LIMIT TO (%I)
                FROM SERVER %I
                INTO %I;
            $f$, remote_schema, foreign_table, server_name, local_schema);
        END IF;
    END LOOP;
END;
$$;

DROP FOREIGN TABLE IF EXISTS dwh_fdw.pods CASCADE;
