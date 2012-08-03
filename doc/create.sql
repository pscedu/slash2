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
	pri		INT,

	recpt_elem	BIGINT,
	recpt_key	BIGINT UNSIGNED,

	PRIMARY KEY(id),
	UNIQUE(resid, fid, bno)
);
