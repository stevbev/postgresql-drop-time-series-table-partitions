-- FUNCTION: public.drop_partitions(timestamp without time zone, text, text, integer, text)

-- USAGE: SELECT public.drop_partitions(timestamp without time zone, schema_name, base_table_name, partition_count, partition_scheme);

-- EXAMPLE: SELECT public.drop_partitions(current_date-180, 'public', 'my_table_name', 5, 'week');
--          Drops table partitions older than the week 180 days ago with name format public.my_table_name_IYYYIW

-- DROP FUNCTION public.drop_partitions(timestamp without time zone, text, text, integer, text);

CREATE OR REPLACE FUNCTION public.drop_partitions(
    retention_period timestamp without time zone,
    schema_name text,
    base_table_name text,
    partition_count integer,
    partition_plan text)

    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE 
AS $BODY$
    DECLARE dateFormat text;
    DECLARE planInterval interval;
    DECLARE searchInterval interval;
    DECLARE endTime timestamp;
    DECLARE startTime timestamp;
    DECLARE deletedTables integer;
    DECLARE fullTablename text;
    DECLARE deleteStatement text;

BEGIN
    --set partition table name format based on how automated partitioning was configured
    dateFormat := CASE WHEN partition_plan='month' THEN 'YYYYMM'
                       WHEN partition_plan='week'  THEN 'IYYYIW'
                       WHEN partition_plan='day'   THEN 'YYYYDDD'
                       WHEN partition_plan='year'  THEN 'YYYY'
                       ELSE 'error'
                  END;

    --throw error if an invalid partition_plan is specified
    IF dateFormat = 'error' THEN
        RAISE EXCEPTION 'Non valid plan --> %', partition_plan;
    END IF;

    --set variable values
    planInterval := ('1 ' || partition_plan)::interval; --increment by one day/week/month/year
    searchInterval := (partition_count || ' ' || partition_plan)::interval; --the number of tables/partitions to look for
    endTime := (date_trunc(partition_plan, retention_period)); --the oldest time period to retain data for
    startTime := (date_trunc(partition_plan, (endTime - searchInterval))); --the oldest time period to delete data for
    deletedTables := 0;

    --look for tables/partitions to drop
    while (startTime < endTime) LOOP
        fullTablename := base_table_name || '_' || to_char(startTime, dateFormat);
        startTime := startTime + planInterval;

        --test if the partition to delete does exist
        IF EXISTS(SELECT * FROM information_schema.tables WHERE table_schema = schema_name AND table_name = fullTablename) THEN
            deleteStatement := 'DROP TABLE '||schema_name||'.'||fullTablename||';';    

            --run the delete/drop statement
            EXECUTE deleteStatement;

            --increment the counter
            deletedTables := deletedTables+1;
        END IF;
    END LOOP;

    --return the number of deleted tables/partitions to the caller
    RETURN deletedTables;

    END;
$BODY$;

ALTER FUNCTION public.drop_partitions(timestamp without time zone, text, text, integer, text) 
    OWNER TO postgres;
