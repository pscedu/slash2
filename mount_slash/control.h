/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running mount_slash instance.
 */

/* for retrieving info about replication status */
struct msctlmsg_replst {
	char	mrs_fn[PATH_MAX];
	char	mrs_data[];
};

/* for issuing/controlling replication requests */
struct msctlmsg_replrq {
	char	mrq_fn[PATH_MAX];
	int	mrq_bmapno;
};

#define REPLRQ_BMAPNO_ALL (-1)

/* custom mount_slash message types */
#define SCMT_ADDREPLRQ		(NPCMT + 0)
#define SCMT_DELREPLRQ		(NPCMT + 1)
#define SCMT_GETREPLST		(NPCMT + 2)
