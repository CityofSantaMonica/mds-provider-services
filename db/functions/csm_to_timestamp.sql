-- double -> timestamptz
CREATE OR REPLACE FUNCTION csm_to_timestamp(double precision)
    RETURNS timestamptz
    LANGUAGE 'sql'
    STABLE
AS $BODY$

select
    case
        when to_timestamp($1) > now() then
            to_timestamp($1 / 1000.0)
        else
            to_timestamp($1)
    end

$BODY$;