-- 4. Cleanup staged table

DROP TABLE IF EXISTS {{.scratch_schema}}.indicative_export_staged{{.entropy}};
