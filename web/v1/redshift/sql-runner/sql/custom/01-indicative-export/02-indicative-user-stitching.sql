-- Create user mapping tables for this run
-- Also update the output table in preparation for joining to `indicative_export_staged` table

CREATE TABLE {{.scratch_schema}}.user_stitching_this_run{{.entropy}} AS (

  WITH users_this_run AS (
    SELECT
      domain_userid,
      MIN(user_id) as user_id,
      MIN(derived_tstamp) AS first_seen_tstamp
    FROM {{.scratch_schema}}.events_staged{{.entropy}}
    GROUP BY 1
  )
  SELECT
    utr.domain_userid,
    COALESCE(us.user_id, utr.user_id) AS user_id
    COALESCE(us.first_seen_tstamp, utr.first_seen_tstamp) AS first_seen_tstamp
  FROM users_this_run AS utr
  LEFT JOIN {{.output_schema}}.user_stitching{{.entropy}} us
  ON utr.domain_userid = us.domain_userid
  AND us.first_seen_tstamp < utr.first_seen_tstamp
);

DELETE FROM {{.output_schema}}.user_stitching {{.entropy}}
WHERE domain_userid IN (SELECT domain_userid FROM {{.scratch_schema}}.user_stitching_this_run{{.entropy}}
AND first_seen_tstamp > (SELECT MIN(first_seen_tstamp) FROM {{.scratch_schema}}.user_stitching_this_run{{.entropy}});

UPDATE {{.output_schema}}.indicative_export set alias_user_id = COALESCE(userfields)
FROM {{.scratch_schema}}.user_stitching_this_run{{.entropy}}
WHERE {{.output_schema}}.user_stitching.domain_userid = {{.scratch_schema}}.user_stitching_this_run.domain_userid
AND {{.output_schema}}.user_stitching.derived_tstamp >= (SELECT MIN(first_seen_tstamp) FROM {{.scratch_schema}}.user_stitching_this_run{{.entropy}});


