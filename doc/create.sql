-- $Id$

CREATE TABLE upsch (
	id		INT		AUTO_INCREMENT,
	resid		UNSIGNED INT,
	fid		UNSIGNED BIGINT,
	bno		UNSIGNED INT,

	PRIMARY KEY(id),
	INDEX(resid),
	INDEX(fid),
	INDEX(bno)
);
