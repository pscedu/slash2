/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running mount_slash instance.
 */

struct msctlmsg_repl {
	char	mrp_fn[PATH_MAX];
};

#define SCMT_ADDREPL		(NPCMT + 0)
#define SCMT_DELREPL		(NPCMT + 1)
