/*
 * Defines the 'routes' table and incremental processing function.
 * 'routs' represents each route from the 'trips' table.
 */

DROP TABLE IF EXISTS routes CASCADE
;

DELETE FROM jobs
WHERE name = 'routes'
;

CREATE table routes (
    provider_id uuid,
    trip_id uuid,
    total_points int,
    in_csm_points int,
    in_dtsm_points int,
    route_line geometry,
    first_csm_geopoint geometry,
    last_csm_geopoint geometry,
    geopoints geometry[],
    first_csm_timepoint timestamptz,
    last_csm_timepoint timestamptz,
    timepoints timestamptz[],
    sequence_id bigserial NOT NULL,
    primary key (provider_id, trip_id)
);

CREATE INDEX routes_sequence_id_idx
    ON routes USING brin (sequence_id)
;

INSERT INTO jobs (name, src_table, src_sequence)
VALUES ('routes', 'route_points', 'route_points_sequence_id_seq');

CREATE OR REPLACE FUNCTION csm_aggregate_routes(OUT start_id bigint, OUT end_id bigint)
RETURNS record
LANGUAGE plpgsql
AS $function$
BEGIN
    /* determine which route points we can safely process */
    SELECT window_start, window_end INTO start_id, end_id
    FROM csm_incremental_job_window('routes');

    /* exit early if there are no new route points to process */
    IF start_id > end_id THEN RETURN; END IF;

    /* process the route points -> routes */
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
    (SELECT
        provider_id,
        trip_id,
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
    WHERE
        route_points.sequence_id BETWEEN start_id AND end_id
    GROUP BY
        provider_id, trip_id
    )
    ON CONFLICT (provider_id, trip_id) DO UPDATE SET
        total_points = EXCLUDED.total_points,
        in_csm_points = EXCLUDED.in_csm_points,
        in_dtsm_points = EXCLUDED.in_dtsm_points,
        route_line = EXCLUDED.route_line,
        first_csm_geopoint = EXCLUDED.first_csm_geopoint,
        last_csm_geopoint = EXCLUDED.last_csm_geopoint,
        geopoints = EXCLUDED.geopoints,
        first_csm_timepoint = EXCLUDED.first_csm_timepoint,
        last_csm_timepoint = EXCLUDED.last_csm_timepoint,
        timepoints = EXCLUDED.timepoints,
        sequence_id = EXCLUDED.sequence_id
    ;
END;
$function$;
