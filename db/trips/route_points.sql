/*
 * Defines the 'route_points' table and incremental processing function.
 * 'route_points' represents each point in each route from the 'trips' table.
 */

DROP TABLE IF EXISTS route_points CASCADE
;

DELETE FROM jobs
WHERE name = 'route_points'
;

CREATE TABLE route_points (
    provider_id uuid,
    trip_id uuid,
    feature jsonb,
    feature_timestamp bigint,
    geopoint geometry,
    timepoint timestamptz,
    in_csm boolean,
    in_dtsm boolean,
    sequence_id bigserial NOT NULL
);

CREATE INDEX route_points_sequence_id_idx
    ON route_points USING brin (sequence_id)
;

INSERT INTO jobs (name, src_table, src_sequence)
VALUES ('route_points', 'trips', 'trips_sequence_id_seq');

CREATE OR REPLACE FUNCTION csm_classify_route_points(OUT start_id bigint, OUT end_id bigint)
RETURNS record
LANGUAGE plpgsql
AS $function$
BEGIN
    /* determine which trips we can safely process */
    SELECT window_start, window_end INTO start_id, end_id
    FROM csm_incremental_job_window('route_points');

    /* exit early if there are no new trips to process */
    IF start_id > end_id THEN RETURN; END IF;

    /* process the trips -> route points */
    INSERT INTO route_points
    (provider_id, trip_id, feature, feature_timestamp, geopoint, timepoint, in_csm, in_dtsm)
      SELECT
            trips.provider_id,
            trips.trip_id,
            coords.f as feature,
            coords.ts as feature_timestamp,
            csm_parse_feature_geom(coords.f) as geopoint,
            to_timestamp(coords.ts) as timepoint,
            st_contains(csm_city_boundary(), csm_parse_feature_geom(coords.f)) as in_csm,
            st_contains(csm_downtown_district(), csm_parse_feature_geom(coords.f)) as in_dtsm
        FROM trips CROSS JOIN LATERAL (
            SELECT f, (f -> 'properties' ->> 'timestamp')::numeric as ts
            FROM jsonb_array_elements(trips.route -> 'features') f
        ) coords
      WHERE trips.sequence_id BETWEEN start_id AND end_id;
END;
$function$;
