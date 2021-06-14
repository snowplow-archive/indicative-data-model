-- 3. DELETE - INSERT to production (and optionally drop the temp table)

BEGIN; --it is safest to use a transaction, in case of failure.

  DELETE FROM {{.output_schema}}.indicative_export{{.entropy}}
    WHERE event_id IN (SELECT event_id FROM {{.scratch_schema}}.indicative_export_staged{{.entropy}});

  INSERT INTO {{.output_schema}}.indicative_export{{.entropy}}
    (SELECT * FROM {{.scratch_schema}}.indicative_export_staged{{.entropy}});

END;
