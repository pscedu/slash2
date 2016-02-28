CREATE TABLE s2ts_run (
	id		INTEGER		PRIMARY KEY,
	suite_name	VARCHAR(16),
	descr		TINYTEXT,
	launch_date	TIMESTAMP,
	user		VARCHAR(32),
	diff		MEDIUMTEXT,
	success		BOOLEAN,
	output		MEDIUMTEXT,
	sl2_commid	CHAR(40),
	pfl_commid	CHAR(40)
);

CREATE INDEX user ON s2ts_run (user);

CREATE TABLE s2ts_result (
	id		INTEGER		PRIMARY KEY,
	run_id		INTEGER		NOT NULL,
	task_id		INTEGER		NOT NULL,
	test_name	VARCHAR(32),
	duration_ms	LONG
);

CREATE INDEX run_id ON s2ts_result (run_id);
CREATE INDEX test_name ON s2ts_result (test_name);
