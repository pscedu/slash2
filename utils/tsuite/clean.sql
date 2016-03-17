DELETE
	s2ts_run.*,
	s2ts_result.*
FROM
	s2ts_run,
	s2ts_result
WHERE
	s2ts_result.run_id = s2ts_run.id
    AND launch_date < datetime('now', 'localtime', '-60 day')
;
