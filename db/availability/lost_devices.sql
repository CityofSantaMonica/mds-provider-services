DROP VIEW IF EXISTS lost_devices CASCADE;

CREATE VIEW lost_devices AS

SELECT
    *
FROM
    csm_availability
WHERE
    end_time IS NULL
;