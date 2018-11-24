CREATE VIEW public.deployments AS

  SELECT
    *
  FROM
    status_changes
  WHERE
    event_type = 'available'::event_types
    AND event_type_reason <> 'user_drop_off'::event_type_reasons
  ORDER BY
    event_time DESC, provider_name

;