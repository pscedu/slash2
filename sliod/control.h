/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a currently running sliod.
 */

#include <sys/param.h>

#include "psc_ds/hash.h"
#include "psc_ds/listcache.h"
#include "psc_util/thread.h"

#include "sliod.h"
#include "inode.h"

/* Path to control socket. */
#define _PATH_SLIOCTLSOCK	"../sliod.sock"

#define STHRNAME_EVERYONE	"everyone"

#define SLCTLMSG_ERRMSG_MAX	50

struct slctlmsg_errmsg {
	char			sem_errmsg[SLCTLMSG_ERRMSG_MAX];
};

#define SSS_NAME_MAX 16	/* multiple of wordsize */

struct slctlmsg_subsys {
	char			sss_names[0];
};

struct slctlmsg_loglevel {
	char			sll_thrname[PSC_THRNAME_MAX];
	int			sll_levels[0];
};

struct slctlmsg_lc {
	char			slc_name[LC_NAME_MAX];
	size_t			slc_max;	/* max #items list can attain */
	size_t			slc_size;	/* #items on list */
	size_t			slc_nseen;	/* max #items list can attain */
};

#define SLC_NAME_ALL		"all"

struct slctlmsg_stats {
	char			sst_thrname[PSC_THRNAME_MAX];
	int			sst_thrtype;
	int			sst_nclients;
	int			sst_nsent;
	int			sst_nrecv;
};

#define pcst_nwrite pcst_u32_1

struct slctlmsg_hashtable {
	char			sht_name[HTNAME_MAX];
	int			sht_totalbucks;
	int			sht_usedbucks;
	int			sht_nents;
	int			sht_maxbucklen;
};

#define SHT_NAME_ALL		"all"

#define SP_FIELD_MAX		30
#define SP_VALUE_MAX		50

struct slctlmsg_param {
	char			sp_thrname[PSC_THRNAME_MAX];
	char			sp_field[SP_FIELD_MAX];
	char			sp_value[SP_VALUE_MAX];
};

struct slctlmsg_iostats {
//	struct iostats		zist_ist;
};

#define SIST_NAME_ALL		"all"

/* Slash control message types. */
#define SCMT_ERRMSG		0
#define SCMT_GETLOGLEVEL	1
#define SCMT_GETLC		2
#define SCMT_GETSTATS		3
#define SCMT_GETSUBSYS		4
#define SCMT_GETHASHTABLE	5
#define SCMT_GETPARAM		6
#define SCMT_SETPARAM		7

/*
 * Slash control message header.
 * This structure precedes each actual message.
 */
struct slctlmsghdr {
	int			scmh_type;
	int			scmh_id;
	size_t			scmh_size;
	unsigned char		scmh_data[0];
};

void slioctlthr_main(const char *);
