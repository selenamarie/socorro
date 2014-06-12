CREATE OR REPLACE FUNCTION update_correlations_module(
    updateday date,
    checkdata boolean DEFAULT true,
    check_period interval DEFAULT '01:00:00'::interval
)
    RETURNS boolean
    LANGUAGE plpgsql
    SET work_mem TO '512MB'
    SET client_min_messages TO 'ERROR'
    AS $$
BEGIN
-- this function populates daily matviews
-- for some of the correlation reports
-- depends on reports_clean, product_versions and processed_crashes

-- check if correlations has already been run for the date
PERFORM 1 FROM correlations_module
WHERE report_date = updateday LIMIT 1;
IF FOUND THEN
  IF checkdata THEN
      RAISE NOTICE 'update_correlations has already been run for %', updateday;
  END IF;
  RETURN FALSE;
END IF;

-- check if reports_clean is complete
IF NOT reports_clean_done(updateday, check_period) THEN
    IF checkdata THEN
        RAISE NOTICE 'Reports_clean has not been updated to the end of %',updateday;
        RETURN FALSE;
    ELSE
        RETURN FALSE;
    END IF;
END IF;

--create correlations_module matview
WITH crash AS (
    SELECT json_array_elements(processed_crash->'json_dump'->'modules') AS modules
           , product_version_id
           , signature_id
           , reports_clean.date_processed::date
           , reports_clean.os_name
    FROM processed_crashes
    JOIN reports_clean ON (processed_crashes.uuid::text = reports_clean.uuid)
    JOIN product_versions USING (product_version_id)
    WHERE reports_clean.date_processed
        BETWEEN updateday::timestamptz AND updateday::timestamptz + '1 day'::interval
    AND processed_crashes.date_processed -- also need to filter on date_processed
        BETWEEN updateday::timestamptz AND updateday::timestamptz + '1 day'::interval

    AND sunset_date > now()
), modules as (
    INSERT INTO modules (
        name
        , version
    )
    SELECT
        (modules->'filename')::text as name
        , (modules->'debug_file')::text as version
    FROM
        crash
    WHERE
        (modules->'filename')::text IS NOT NULL
    AND (modules->'debug_file')::text IS NOT NULL
    RETURNING module_id, name, version
)
INSERT INTO correlations_module (
    product_version_id
    , module_id
    , report_date
    , os_name
    , signature_id
    , total
)
SELECT product_version_id
       , module_id
       , date_processed as report_date
       , os_name
       , signature_id
       , count(*) as total
FROM crash
    JOIN modules
        ON (modules->'filename')::text = modules.name AND
        ON (modules->'debug_file')::text = modules.version
WHERE
    (modules->'filename')::text IS NOT NULL
AND (modules->'debug_file')::text IS NOT NULL
GROUP BY module_id
         , product_version_id
         , report_date
         , os_name
         , signature_id;

RETURN TRUE;
END;
$$;
