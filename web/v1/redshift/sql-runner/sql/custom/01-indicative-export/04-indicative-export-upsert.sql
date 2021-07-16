-- 3. DELETE - INSERT to production

BEGIN; --it is safest to use a transaction, in case of failure.

  DELETE FROM {{.output_schema}}.indicative_export{{.entropy}} -- upsert events based on event_id and scan limit
    WHERE event_id IN (SELECT event_id FROM {{.scratch_schema}}.indicative_export_staged{{.entropy}})
    AND derived_tstamp >= (SELECT lower_limit FROM {{.scratch_schema}}.indicative_run_limits{{.entropy}});

  INSERT INTO {{.output_schema}}.indicative_export{{.entropy}}
    (SELECT * FROM {{.scratch_schema}}.indicative_export_staged{{.entropy}});

END;

--

BEGIN;

  UPDATE {{.output_schema}}.user_stitching{{.entropy}} AS us SET alias_user_id = COALESCE(ustrs.user_id, us.alias_user_id) -- update the output user stitching table, using the more up to date alias ID if available
    FROM {{.scratch_schema}}.user_stitching_this_run_staged{{.entropy}} AS ustrs
    WHERE us.domain_userid = ustrs.domain_userid
    AND us.first_seen_tstamp >= (SELECT MIN(first_seen_tstamp) FROM {{.scratch_schema}}.user_stitching_this_run_staged{{.entropy}});

END;

--

BEGIN;

  UPDATE {{.output_schema}}.indicative_export{{.entropy}} AS i SET alias_user_id = COALESCE(u.alias_user_id, i.alias_user_id) -- use the output user stitching table to update the output event table
    FROM {{.output_schema}}.user_stitching{{.entropy}} AS u
    WHERE u.domain_userid = i.domain_userid;

END;
