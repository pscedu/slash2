/* $Id$ */

/*
 * Control interface for querying and modifying
 * parameters of a running instance of mount_slash.
 */

#include <sys/param.h>

#include "psc_ds/hash.h"
#include "psc_ds/listcache.h"
#include "psc_util/thread.h"
#include "psc_util/iostats.h"

#include "inode.h"

/* Path to control socket. */
#define _PATH_MSCTLSOCK		"../mount_zest.sock"

#define MSTHRNAME_EVERYONE	"everyone"

#define MSCTLMSG_ERRMSG_MAX	50

struct msctlmsg_error {
	char			me_errmsg[MSCTLMSG_ERRMSG_MAX];
};

#define MSS_NAME_MAX		30

struct msctlmsg_subsys {
	char			ms_names[0];
};

struct msctlmsg_loglevel {
	char			ml_thrname[PSC_THRNAME_MAX];
	int			ml_levels[0];
};

struct msctlmsg_lc {
	char			mlc_name[LC_NAME_MAX];
	size_t			mlc_max;	/* max #items list can attain */
	size_t			mlc_size;	/* #items on list */
	size_t			mlc_nseen;	/* max #items list can attain */
};

#define MSLC_NAME_ALL		"all"

struct msctlmsg_stats {
	char			mst_thrname[PSC_THRNAME_MAX];
	int			mst_thrtype;
	u32			mst_u32_1;
	u32			mst_u32_2;
	u32			mst_u32_3;
};

#define mst_nclients	mst_u32_1
#define mst_nsent	mst_u32_2
#define mst_nrecv	mst_u32_3

struct msctlmsg_hashtable {
	char			mht_name[HTNAME_MAX];
	int			mht_totalbucks;
	int			mht_usedbucks;
	int			mht_nents;
	int			mht_maxbucklen;
};

#define MSHT_NAME_ALL		"all"

#define MSP_FIELD_MAX		30
#define MSP_VALUE_MAX		50

struct msctlmsg_param {
	char			mp_thrname[PSC_THRNAME_MAX];
	char			mp_field[MSP_FIELD_MAX];
	char			mp_value[MSP_VALUE_MAX];
};

struct msctlmsg_iostats {
	struct iostats		mi_ist;
};

#define MSI_NAME_ALL		"all"

/* Control message types. */
#define MSCMT_ERROR		0
#define MSCMT_GETLOGLEVEL	1
#define MSCMT_GETLC		2
#define MSCMT_GETSTATS		3
#define MSCMT_GETSUBSYS		4
#define MSCMT_GETHASHTABLE	5
#define MSCMT_GETPARAM		6
#define MSCMT_SETPARAM		7

/*
 * Control message header.
 * This structure precedes each actual message.
 */
struct msctlmsghdr {
	int			mh_type;
	int			mh_id;
	size_t			mh_size;
	unsigned char		mh_data[0];
};

void msctlthr_main(const char *);
