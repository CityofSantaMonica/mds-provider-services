-- timestamp -> timestamptz
CREATE OR REPLACE FUNCTION csm_local_timestamp(timestamp = clock_timestamp())
    RETURNS timestamptz
    LANGUAGE plpgsql
    STABLE
AS $FUNCTION$
BEGIN
    SELECT timezone('America/Los_Angeles', $1);
END;
$FUNCTION$;

-- timestamptz -> timestamp
CREATE OR REPLACE FUNCTION csm_local_timestamp(timestamptz = clock_timestamp())
    RETURNS timestamp
    LANGUAGE plpgsql
    STABLE
AS $FUNCTION$
BEGIN
    SELECT timezone('America/Los_Angeles', $1);
END;
$FUNCTION$;