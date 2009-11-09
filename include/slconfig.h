/* $Id$ */

#ifndef _SLCONFIG_H_
#define _SLCONFIG_H_

#include <sys/param.h>

#include "pfl/types.h"
#include "psc_ds/hash.h"
#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"
#include "psc_util/log.h"

#include "inode.h"

struct sl_site;

#define SITE_NAME_MAX	64
#define RES_NAME_MAX	64
#define FULL_NAME_MAX	(SITE_NAME_MAX + RES_NAME_MAX + 2)	/* '@' + NUL */
#define SL_PEER_MAX	16
#define DEVNAMEMAX	128
#define MAXNET		32

enum sl_res_type {
	SLREST_ARCHIVAL_FS,
	SLREST_CLUSTER_NOSHARE_FS,
	SLREST_COMPUTE,
	SLREST_PARALLEL_FS
};

struct sl_resource {
	char			 res_name[RES_NAME_MAX];
	char			*res_desc;
	char			*res_peertmp[SL_PEER_MAX];
	sl_ios_id_t		 res_id;
	int			 res_mds;
	enum sl_res_type	 res_type;
	sl_ios_id_t		*res_peers;
	uint32_t		 res_npeers;
	uint32_t		 res_nnids;
	lnet_nid_t		*res_nids;
	char			 res_fsroot[PATH_MAX];
	void			*res_pri;
	struct sl_site		*res_site;
};

struct sl_resm {
	lnet_nid_t		 resm_nid;
	struct sl_resource	*resm_res;
	struct hash_entry	 resm_hentry;
	void			*resm_pri;
};

#define slresm_2_resid(r)	(r)->resm_res->res_id

struct sl_site {
	char			 site_name[SITE_NAME_MAX];
	char			*site_desc;
	void			*site_pri;
	struct psclist_head	 site_lentry;
	struct sl_resource	**site_resv;
	int			 site_nres;
	sl_siteid_t		 site_id;
};

#define INIT_SITE(s)		INIT_PSCLIST_ENTRY(&(s)->site_lentry)

/* structure for this node */
struct sl_nodeh {
	struct sl_resource	*node_res;
	struct sl_site		*node_site;
};

struct sl_gconf {
	char			 gconf_net[MAXNET];
	char			 gconf_fdbkeyfn[PATH_MAX];
	uint32_t		 gconf_netid;
	int			 gconf_port;
	struct psc_lockedlist	 gconf_sites;
	struct hash_table	 gconf_nids_hash;
	psc_spinlock_t		 gconf_lock;
};

#define GCONF_HASHTBL_SZ 63
#define INIT_GCONF(g)						\
	do {							\
		memset((g), 0, sizeof(*(g)));			\
		LOCK_INIT(&(g)->gconf_lock);			\
		pll_init(&(g)->gconf_sites, struct sl_site,	\
		    site_lentry, &(g)->gconf_lock);		\
		init_hash_table(&(g)->gconf_nids_hash,		\
				GCONF_HASHTBL_SZ, "resnid");	\
	} while (0)

/*
 * sl_global_id_build - produce a global, unique identifier for a resource
 *	from its internal identifier.
 * @site_id: site identifier.
 * @intres_id: resource identifier, internal to site.
 */
static __inline sl_ios_id_t
sl_global_id_build(sl_siteid_t site_id, uint32_t intres_id)
{
	psc_assert(site_id != SITE_ID_ANY);
	psc_assert(intres_id < (1 << SL_RES_BITS) - 1);
	return (((sl_ios_id_t)site_id << SL_SITE_BITS) | intres_id);
}

static __inline sl_siteid_t
sl_iosid_to_siteid(sl_ios_id_t id)
{
	return ((id & SL_SITE_MASK) >> SL_SITE_BITS);
}

struct sl_site		*slcfg_new_site(void);
struct sl_resource	*slcfg_new_res(void);
struct sl_resm		*slcfg_new_resm(void);

int			 slcfg_site_cmp(const void *, const void *);
int			 slcfg_res_cmp(const void *, const void *);
int			 slcfg_resnid_cmp(const void *, const void *);

void			 slcfg_parse(const char *);

void			 libsl_nid_associate(lnet_nid_t, struct sl_resource *);
struct sl_resm		*libsl_resm_lookup(void);
struct sl_site		*libsl_id2site(sl_ios_id_t);
struct sl_resource	*libsl_id2res(sl_ios_id_t);
struct sl_resm		*libsl_nid2resm(lnet_nid_t);
struct sl_resource	*libsl_str2res(const char *);
sl_ios_id_t		 libsl_str2id(const char *);
void			 libsl_profile_dump(void);
uint32_t		 libsl_str2restype(const char *);
void			 libsl_init(int);

extern struct sl_nodeh	 nodeInfo;
extern struct sl_gconf	 globalConfig;

#endif /* _SLCONFIG_H_ */
