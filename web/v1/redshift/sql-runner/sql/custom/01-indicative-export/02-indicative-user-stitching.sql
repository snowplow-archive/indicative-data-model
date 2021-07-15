-- Create user mapping tables for this run
-- Also update the output table in preparation for joining to `indicative_export_staged` table

DROP TABLE IF EXISTS {{.scratch_schema}}.user_stitching_this_run_staged{{.entropy}};

CREATE TABLE {{.scratch_schema}}.user_stitching_this_run_staged{{.entropy}} AS (

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
    COALESCE(us.user_id, utr.user_id) AS user_id,
    COALESCE(us.first_seen_tstamp, utr.first_seen_tstamp) AS first_seen_tstamp
  FROM users_this_run AS utr
  LEFT JOIN {{.output_schema}}.user_stitching{{.entropy}} us
  ON utr.domain_userid = us.domain_userid
  AND us.first_seen_tstamp < utr.first_seen_tstamp
);

DELETE FROM {{.output_schema}}.user_stitching{{.entropy}}
WHERE domain_userid IN (SELECT domain_userid FROM {{.scratch_schema}}.user_stitching_this_run_staged{{.entropy}})
AND first_seen_tstamp >= (SELECT MIN(first_seen_tstamp) FROM {{.scratch_schema}}.user_stitching_this_run_staged{{.entropy}});

INSERT INTO {{.scratch_schema}}.user_stitching{{.entropy}} (
  SELECT * FROM {{.scratch_schema}}.user_stitching_this_run_staged{{.entropy}}
  WHERE domain_userid NOT IN (SELECT domain_userid FROM {{.output_schema}}.user_stitching{{.entropy}} GROUP BY 1)
);