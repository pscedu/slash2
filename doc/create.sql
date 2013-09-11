-- $Id$

DROP TABLE IF EXISTS upsch;

CREATE TABLE upsch (
	id		INT			AUTO_INCREMENT,
	resid		UNSIGNED INT,
	fid		UNSIGNED BIGINT,
	uid		UNSIGNED INT,
	gid		UNSIGNED INT,
	bno		UNSIGNED INT,
	status		CHAR(1), -- 'Q' or 'S'
	sys_pri		INT,
	usr_pri		INT,
	nonce		UNSIGNED INT,

	PRIMARY KEY(id),
	UNIQUE(resid, fid, bno)
);

CREATE INDEX 'upsch_resid_idx' ON 'upsch' ('resid');
CREATE INDEX 'upsch_fid_idx' ON 'upsch' ('fid');
CREATE INDEX 'upsch_bno_idx' ON 'upsch' ('bno');
