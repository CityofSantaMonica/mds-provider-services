DROP TABLE IF EXISTS routes CASCADE;

CREATE TABLE IF NOT EXISTS routes (
    provider_id uuid,
    trip_id uuid,
    total_points int,
    in_csm_points int,
    in_dtsm_points int,
    route_line geometry,
    first_csm_geopoint geometry,
    last_csm_geopoint geometry,
    first_csm_timepoint timestamptz,
    last_csm_timepoint timestamptz,
    geopoints geometry[],
    timepoints timestamptz[],
    CONSTRAINT pk_routes PRIMARY KEY (provider_id, trip_id)
);

/* trigger function */
CREATE OR REPLACE FUNCTION process_trip_route()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS $FUNCTION$
BEGIN
    WITH route_points AS (
        SELECT
            coords.f as feature,
            coords.ts as feature_timestamp,
            csm_parse_feature_geom(coords.f) as geopoint,
            csm_to_timestamp(coords.ts) as timepoint,
            st_contains(csm_city_boundary(), csm_parse_feature_geom(coords.f)) as in_csm,
            st_contains(csm_downtown_district(), csm_parse_feature_geom(coords.f)) as in_dtsm
        FROM (
            SELECT DISTINCT f, (f -> 'properties' ->> 'timestamp')::numeric as ts
            FROM jsonb_array_elements(new.route -> 'features') f
        ) coords
    )

    INSERT INTO routes (
        provider_id,
        trip_id,
        total_points,
        in_csm_points,
        in_dtsm_points,
        route_line,
        first_csm_geopoint,
        last_csm_geopoint,
        geopoints,
        first_csm_timepoint,
        last_csm_timepoint,
        timepoints
    )
    SELECT
        new.provider_id,
        new.trip_id,
        count(*) as total_points,
        count(*) filter (where in_csm) as in_csm_points,
        count(*) filter (where in_dtsm) as in_dtsm_points,
        st_makeline(array_agg(geopoint order by timepoint)) as route_line,
        ((array_agg(geopoint order by timepoint) filter (where in_csm)))[1] as first_csm_geopoint,
        ((array_agg(geopoint order by timepoint desc) filter (where in_csm)))[1] as last_csm_geopoint,
        array_agg(geopoint order by timepoint) as geopoints,
        min(timepoint) filter (where in_csm) as first_csm_timepoint,
        max(timepoint) filter (where in_csm) as last_csm_timepoint,
        array_agg(timepoint order by timepoint) as timepoints
    FROM
        route_points
    ON CONFLICT ON CONSTRAINT pk_routes DO UPDATE SET
        total_points = EXCLUDED.total_points,
        in_csm_points = EXCLUDED.in_csm_points,
        in_dtsm_points = EXCLUDED.in_dtsm_points,
        route_line = EXCLUDED.route_line,
        first_csm_geopoint = EXCLUDED.first_csm_geopoint,
        last_csm_geopoint = EXCLUDED.last_csm_geopoint,
        geopoints = EXCLUDED.geopoints,
        first_csm_timepoint = EXCLUDED.first_csm_timepoint,
        last_csm_timepoint = EXCLUDED.last_csm_timepoint,
        timepoints = EXCLUDED.timepoints;

    RETURN new;
END;
$FUNCTION$;

DROP TRIGGER IF EXISTS process_inserted_trip ON trips CASCADE;

/* trigger */
CREATE TRIGGER process_inserted_trip
     AFTER INSERT ON trips
     FOR EACH ROW
     EXECUTE PROCEDURE process_trip_route();
