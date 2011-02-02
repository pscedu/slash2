/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2011, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * This interface provides access to the information parsed from the
 * SLASH configuration file about sites and servers in a SLASH network.
 */

#ifndef _SLCONFIG_H_
#define _SLCONFIG_H_

#include <sys/param.h>

#include "pfl/hashtbl.h"
#include "pfl/types.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_rpc/rpc.h"
#include "psc_util/alloc.h"
#include "psc_util/log.h"

#include "inode.h"

struct sl_site;
struct slashrpc_cservice;

#define SITE_NAME_MAX	32
#define INTRES_NAME_MAX	32
#define RES_NAME_MAX	(SITE_NAME_MAX + INTRES_NAME_MAX)
#define RESM_ADDRBUF_SZ	(RES_NAME_MAX + 1 + PSCRPC_NIDSTR_SIZE)	/* foo@BAR:1.1.1.1@tcp0 */
#define SL_PEER_MAX	16
#define LNET_NAME_MAX	32

enum sl_res_type {
	SLREST_NONE = 0,		/* must be zero */
	SLREST_STANDALONE_FS,
	SLREST_ARCHIVAL_FS,
	SLREST_CLUSTER_NOSHARE_FS,
	SLREST_COMPUTE,
	SLREST_MDS,
	SLREST_PARALLEL_FS
};

/* Resource (I/O system, MDS) */
struct sl_resource {
	char			 res_name[RES_NAME_MAX];	/* resource name */
	char			*res_desc;
	char			*res_peertmp[SL_PEER_MAX];
	int			 res_npeers;
	sl_ios_id_t		 res_id;
	enum sl_res_type	 res_type;
	struct psc_dynarray	 res_peers;
	struct psc_dynarray	 res_members;
	char			 res_fsroot[PATH_MAX];
	char			 res_jrnldev[PATH_MAX];
	void			*res_pri;
	struct sl_site		*res_site;
};

/* Resource member (a machine in the I/O system) */
struct sl_resm {
	char			 resm_addrbuf[RESM_ADDRBUF_SZ];
	lnet_nid_t		 resm_nid;			/* Node ID for the resource member */
	struct sl_resource	*resm_res;
	struct psc_hashent	 resm_hentry;
	struct slashrpc_cservice*resm_csvc;
	void			*resm_pri;
#define resm_site		 resm_res->res_site
#define resm_type		 resm_res->res_type
#define resm_iosid		 resm_res->res_id
};

#define resm_2_resid(r)		(r)->resm_res->res_id

#define RES_MAXID		((UINT64_C(1) << (sizeof(sl_ios_id_t) * \
				    NBBY - SLASH_ID_SITE_BITS)) - 1)

/* Site (a collection of I/O systems) */
struct sl_site {
	char			 site_name[SITE_NAME_MAX];
	char			*site_desc;
	void			*site_pri;			/* struct site_mds_info */
	struct psclist_head	 site_lentry;
	struct psc_dynarray	 site_resources;
	sl_siteid_t		 site_id;
};

#define SITE_MAXID		((1 << SLASH_ID_SITE_BITS) - 1)

struct sl_gconf {
	char			 gconf_net[LNET_NAME_MAX];
	char			 gconf_fsroot[PATH_MAX];
	uint32_t		 gconf_netid;
	int			 gconf_port;
	struct psc_lockedlist	 gconf_sites;
	struct psc_hashtbl	 gconf_nid_hashtbl;
	psc_spinlock_t		 gconf_lock;
};

#define GCONF_HASHTBL_SZ	63
#define INIT_GCONF(g)							\
	do {								\
		memset((g), 0, sizeof(*(g)));				\
		INIT_SPINLOCK(&(g)->gconf_lock);			\
		pll_init(&(g)->gconf_sites, struct sl_site,		\
		    site_lentry, &(g)->gconf_lock);			\
		psc_hashtbl_init(&(g)->gconf_nid_hashtbl, 0,		\
		    struct sl_resm, resm_nid, resm_hentry,		\
		    GCONF_HASHTBL_SZ, NULL, "resnid");			\
	} while (0)

#define CONF_LOCK()			PLL_LOCK(&globalConfig.gconf_sites)
#define CONF_ULOCK()			PLL_ULOCK(&globalConfig.gconf_sites)
#define CONF_RLOCK()			PLL_RLOCK(&globalConfig.gconf_sites)
#define CONF_URLOCK(lk)			PLL_URLOCK(&globalConfig.gconf_sites, (lk))

#define CONF_FOREACH_SITE(s)		PLL_FOREACH((s), &globalConfig.gconf_sites)
#define SITE_FOREACH_RES(s, r, i)	DYNARRAY_FOREACH((r), (i), &(s)->site_resources)
#define RES_FOREACH_MEMB(r, m, j)	DYNARRAY_FOREACH((m), (j), &(r)->res_members)

#define CONF_FOREACH_SITE_CONT(s)	PLL_FOREACH_CONT((s), &globalConfig.gconf_sites)
#define SITE_FOREACH_RES_CONT(s, r, i)	DYNARRAY_FOREACH_CONT((r), (i), &(s)->site_resources)
#define RES_FOREACH_MEMB_CONT(r, m, j)	DYNARRAY_FOREACH_CONT((m), (j), &(r)->res_members)

#define CONF_FOREACH_RESM(s, r, i, m, j)				\
	CONF_FOREACH_SITE(s)						\
		SITE_FOREACH_RES((s), (r), (i))				\
			RES_FOREACH_MEMB((r), (m), (j))

#define SL_MDS_WALK(resm, code)						\
	do {								\
		struct sl_resource *_res;				\
		struct sl_site *_site;					\
		int _siter;						\
									\
		CONF_LOCK();						\
		CONF_FOREACH_SITE(_site)				\
			SITE_FOREACH_RES(_site, _res, _siter)		\
				if (_res->res_type == SLREST_MDS) {	\
					(resm) = psc_dynarray_getpos(	\
					    &_res->res_members, 0);	\
					do {				\
						code;			\
					} while (0);			\
					break;				\
				}					\
		CONF_ULOCK();						\
	} while (0)

#define SL_MDS_WALK_SETLAST()						\
	(_site = pll_last_item(&globalConfig.gconf_sites, struct sl_site))

void			 slcfg_init_res(struct sl_resource *);
void			 slcfg_init_resm(struct sl_resm *);
void			 slcfg_init_site(struct sl_site *);

int			 slcfg_res_cmp(const void *, const void *);
int			 slcfg_resm_cmp(const void *, const void *);
int			 slcfg_site_cmp(const void *, const void *);

void			 slcfg_parse(const char *);
void			 slcfg_store_tok_val(const char *, char *);

struct sl_resource	*libsl_id2res(sl_ios_id_t);
void			 libsl_init(int, int);
struct sl_resm		*libsl_nid2resm(lnet_nid_t);
void			 libsl_profile_dump(void);
struct sl_site		*libsl_resid2site(sl_ios_id_t);
struct sl_site		*libsl_siteid2site(sl_siteid_t);
sl_ios_id_t		 libsl_str2id(const char *);
struct sl_resource	*libsl_str2res(const char *);
struct sl_resm		*libsl_try_nid2resm(lnet_nid_t);

extern struct sl_resm	*nodeResm;
extern struct sl_gconf	 globalConfig;

#define nodeSite	nodeResm->resm_site
#define nodeResProf	nodeResm->resm_res

/**
 * sl_global_id_build - Produce a global, unique identifier for a resource
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
sl_resid_to_siteid(sl_ios_id_t id)
{
	return ((id & SL_SITE_MASK) >> SL_SITE_BITS);
}

#endif /* _SLCONFIG_H_ */
