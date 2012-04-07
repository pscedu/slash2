-- $Id$

CREATE TABLE upsch (
	id		INT			AUTO_INCREMENT,
	resid		UNSIGNED INT,
	fid		UNSIGNED BIGINT,
	uid		UNSIGNED INT,
	gid		UNSIGNED INT,
	bno		UNSIGNED INT,
	status		ENUM('Q', 'S'),

	PRIMARY KEY(id)
--	KEY(resid),
--	KEY(fid),
--	KEY(uid),
--	KEY(gid),
--	KEY(bno)
);
