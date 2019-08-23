-- json input
CREATE OR REPLACE FUNCTION csm_parse_feature_geom(json)
    RETURNS geometry
    LANGUAGE plpgsql
    IMMUTABLE PARALLEL SAFE
AS $FUNCTION$
BEGIN
    SELECT st_setsrid(st_geomfromgeojson(($1 -> 'geometry'::text)::text), 4326);
END;
$FUNCTION$;

-- jsonb input
CREATE OR REPLACE FUNCTION csm_parse_feature_geom(jsonb)
    RETURNS geometry
    LANGUAGE plpgsql
    IMMUTABLE PARALLEL SAFE
AS $FUNCTION$
BEGIN
    SELECT st_setsrid(st_geomfromgeojson(($1 -> 'geometry'::text)::text), 4326);
END;
$FUNCTION$;