-- timestamp -> timestamptz
CREATE OR REPLACE FUNCTION csm_local_timestamp(timestamp = clock_timestamp())
    RETURNS timestamptz
    LANGUAGE 'sql'
    STABLE
AS $BODY$

SELECT timezone('America/Los_Angeles', $1)

$BODY$;

-- timestamptz -> timestamp
CREATE OR REPLACE FUNCTION csm_local_timestamp(timestamptz = clock_timestamp())
    RETURNS timestamp
    LANGUAGE 'sql'
    STABLE
AS $BODY$

SELECT timezone('America/Los_Angeles', $1)

$BODY$;