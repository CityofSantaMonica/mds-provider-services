DROP VIEW IF EXISTS deployments CASCADE;

CREATE VIEW deployments AS

SELECT
    *
FROM
    csm_status_changes
WHERE
    deployment;