/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 * ---------------------------------------------------------------------
 * %END_LICENSE%
 */

/*
 * This interface provides access to the information parsed from the
 * SLASH2 configuration file about sites and servers in a SLASH2 deployment.
 */

#ifndef _SLCONFIG_H_
#define _SLCONFIG_H_

#include <sys/param.h>

#include "pfl/alloc.h"
#include "pfl/dynarray.h"
#include "pfl/hashtbl.h"
#include "pfl/list.h"
#include "pfl/log.h"
#include "pfl/net.h"
#include "pfl/rpc.h"
#include "pfl/types.h"

#include "sltypes.h"

struct sl_site;
struct slrpc_cservice;

#define SITE_NAME_MAX		32
#define INTRES_NAME_MAX		32
#define RES_NAME_MAX		(SITE_NAME_MAX + INTRES_NAME_MAX)
#define RESM_ADDRBUF_SZ		(RES_NAME_MAX + PSCRPC_NIDSTR_SIZE)	/* foo@BAR:1.1.1.1@tcp0 */
#define LNET_NAME_MAX		32

enum sl_res_type {
	SLREST_NONE,
	SLREST_ARCHIVAL_FS,
	SLREST_CLUSTER_NOSHARE_LFS,		/* Logical set of stand-alone servers */
	SLREST_MDS,
	SLREST_PARALLEL_COMPNT,			/* A member of a parallel fs */
	SLREST_PARALLEL_LFS,			/* Logical parallel fs */
	SLREST_STANDALONE_FS			/* 6: local file system */
};

/* XXX rename to RES_ISNODE() */
#define RES_ISFS(res)							\
	((res)->res_type == SLREST_ARCHIVAL_FS		||		\
	 (res)->res_type == SLREST_PARALLEL_COMPNT	||		\
	 (res)->res_type == SLREST_STANDALONE_FS)

#define RES_ISCLUSTER(res)						\
	((res)->res_type == SLREST_CLUSTER_NOSHARE_LFS	||		\
	 (res)->res_type == SLREST_PARALLEL_LFS)

/* Resource (I/O system, MDS) */
struct sl_resource {
	uint64_t		 res_hashkey;
	struct pfl_hashentry	 res_hentry;

	sl_ios_id_t		 res_id;
	psc_atomic32_t		 res_batchcnt;
	int			 res_offset;
	int			 res_flags;	/* see RESF_* below */
	enum sl_res_type	 res_type;
	uint32_t		 res_stkvers;	/* peer SLASH2 stack version */
	uint64_t		 res_uptime;	/* peer uptime in secs */
	struct sl_site		*res_site;	/* backpointer to site */
	struct psc_dynarray	 res_peers;
	struct psc_dynarray	 res_members;	/* for cluster types */
	char			 res_name[RES_NAME_MAX];
	char			*res_desc;	/* human description */
	struct slcfg_local	*res_localcfg;
};

/* res_flags */
#define RESF_DISABLE_BIA	(1 << 0)	/* disable write assignments */
#define RESF_PREFIOS		(1 << 1)	/* is in pref_ios (CLI) */

#define RES_MAXID		((UINT64_C(1) << (sizeof(sl_ios_id_t) * \
				    NBBY - SLASH_FID_MDSID_BITS)) - 1)

#define res_getmemb(r)		psc_dynarray_getpos(&(r)->res_members, 0)

static __inline void *
resprof_get_pri(struct sl_resource *res)
{
	return (res + 1);
}

/* Resource members can have multiple network interfaces. */
struct sl_resm_nid {
	char			 resmnid_addrbuf[RESM_ADDRBUF_SZ];
	lnet_nid_t		 resmnid_nid;
};

/*
 * Resource member (a machine in an I/O system)
 *
 * This structure is used in two ways:
 *	- a CLUSTER-type resource has multiple members each consitituent
 *	  of which is itself a slcfg resource, so only resm_res is used
 *	  in these scenarios and no other fields.
 *	- an FS-type resource, for which this structure contains the
 *	  connection details, as well as a backpointer to the actual
 *	  resource profile.
 */
struct sl_resm {
	struct slrpc_cservice	*resm_csvc;	/* client RPC service - must be first */
	struct psc_dynarray	 resm_nids;	/* network interfaces */
	struct sl_resource	*resm_res;	/* backpointer to resource */
	struct pfl_mutex	 resm_mutex;
#define resm_site		 resm_res->res_site
#define resm_siteid		 resm_site->site_id
#define resm_type		 resm_res->res_type
#define resm_res_id		 resm_res->res_id
#define resm_name		 resm_res->res_name
};

static __inline void *
resm_get_pri(struct sl_resm *resm)
{
	return (resm + 1);
}

#define resm_getcsvcerr(m)						\
	((m)->resm_csvc ?						\
	   ((m)->resm_csvc->csvc_lasterrno ?				\
	    (m)->resm_csvc->csvc_lasterrno : -ETIMEDOUT) : -ETIMEDOUT)

/* Site (a collection of I/O systems) */
struct sl_site {
	char			 site_name[SITE_NAME_MAX];
	char			*site_desc;
	struct psc_listentry	 site_lentry;
	struct psc_dynarray	 site_resources;
	sl_siteid_t		 site_id;
};

/* highest allowed site ID */
#define SITE_MAXID		((1 << SLASH_FID_MDSID_BITS) - 1)

static __inline void *
site_get_pri(struct sl_site *site)
{
	return (site + 1);
}

/* link between LNET interface and LNET route */
struct lnetif_pair {
	uint32_t		 net;
	int			 flags;		/* see LPF_* flags */
	char			 ifn[IFNAMSIZ];
	struct psc_listentry	 lentry;
};

#define LPF_NOACCEPTOR		(1 << 0)
#define LPF_SKIP		(1 << 1)

#define	MDS_FIDCACHE_SIZE	65536
#define	IOS_FIDCACHE_SIZE	4096
#define	MSL_FIDCACHE_SIZE	2048

/* local (host-specific settings) configuration */
struct slcfg_local {
	char			*cfg_journal;
	char			*cfg_zpcachefn;
	char			*cfg_allowexe;
	size_t			 cfg_arc_max;
	size_t			 cfg_fidcachesz;
	size_t			 cfg_slab_cache_size;
	char			*cfg_fsroot;
	char			 cfg_prefmds[RES_NAME_MAX];
	char			 cfg_prefios[RES_NAME_MAX];
	char			 cfg_zpname[NAME_MAX + 1];
	char			*cfg_selftest;
	int			 cfg_async_io:1;
	int			 cfg_root_squash:1;
};

/* SLASH2 deployment settings shared by all nodes */
struct sl_config {
	char			 gconf_lroutes[256];
	char			 gconf_lnets[LNETS_MAX];
	int			 gconf_port;
	struct psclist_head	 gconf_routehd;
	struct psc_lockedlist	 gconf_sites;
	psc_spinlock_t		 gconf_lock;
	uint64_t		 gconf_fsuuid;
	struct psc_hashtbl	 gconf_res_hashtbl;
};

#define INIT_GCONF(cf)							\
	do {								\
		INIT_LISTHEAD(&(cf)->gconf_routehd);			\
		INIT_SPINLOCK(&(cf)->gconf_lock);			\
		pll_init(&(cf)->gconf_sites, struct sl_site,		\
		    site_lentry, &(cf)->gconf_lock);			\
		psc_hashtbl_init(&(cf)->gconf_res_hashtbl, 0,		\
		    struct sl_resource, res_hashkey, res_hentry, 191,	\
		    NULL, "res");					\
	} while (0)

#define CONF_LOCK()			spinlock(&globalConfig.gconf_lock)
#define CONF_ULOCK()			freelock(&globalConfig.gconf_lock)
#define CONF_RLOCK()			reqlock(&globalConfig.gconf_lock)
#define CONF_TRYRLOCK(lk)		tryreqlock(&globalConfig.gconf_lock, (lk))
#define CONF_URLOCK(lk)			ureqlock(&globalConfig.gconf_lock, (lk))
#define CONF_HASLOCK(lk)		psc_spin_haslock(&globalConfig.gconf_lock)

#define CONF_FOREACH_SITE(s)		PLL_FOREACH((s), &globalConfig.gconf_sites)
#define SITE_FOREACH_RES(s, r, i)	DYNARRAY_FOREACH((r), (i), &(s)->site_resources)
#define RES_FOREACH_MEMB(r, m, j)	DYNARRAY_FOREACH((m), (j), &(r)->res_members)
#define RESM_FOREACH_NID(m, n, k)	DYNARRAY_FOREACH((n), (k), &(m)->resm_nids)

#define CONF_FOREACH_SITE_CONT(s)	PLL_FOREACH_CONT((s), &globalConfig.gconf_sites)
#define SITE_FOREACH_RES_CONT(s, r, i)	DYNARRAY_FOREACH_CONT((r), (i), &(s)->site_resources)
#define RES_FOREACH_MEMB_CONT(r, m, j)	DYNARRAY_FOREACH_CONT((m), (j), &(r)->res_members)
#define RESM_FOREACH_NID_CONT(m, n, k)	DYNARRAY_FOREACH_CONT((n), (k), &(m)->resm_nids)

#define CLUSTER_FOREACH_RES(cluster, ri, i)				\
	for ((i) = 0, (ri) = NULL; RES_ISCLUSTER(cluster) ?		\
	    ((i) < psc_dynarray_len(&(cluster)->res_peers) ||		\
	      ((ri) = NULL)) && (((ri) = psc_dynarray_getpos(		\
		&(cluster)->res_peers, (i))) || 1) :			\
	    (ri) = (ri) ? NULL : (cluster); (i)++)

#define CONF_FOREACH_RES(s, r, i)					\
	CONF_FOREACH_SITE(s)						\
		SITE_FOREACH_RES((s), (r), (i))

#define CONF_FOREACH_RESM(s, r, i, m, j)				\
	CONF_FOREACH_SITE(s)						\
		SITE_FOREACH_RES((s), (r), (i))				\
			RES_FOREACH_MEMB((r), (m), (j))

/* XXX this can be rewritten with a goto (shudder) */
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

void			 slcfg_destroy_res(struct sl_resource *);
void			 slcfg_destroy_resm(struct sl_resm *);
void			 slcfg_destroy_site(struct sl_site *);

int			 slcfg_res_cmp(const void *, const void *);
int			 slcfg_site_cmp(const void *, const void *);

void			 slcfg_destroy(void);
void			 slcfg_parse(const char *);
void			 slcfg_resm_addaddr(char *, const char *);

struct sl_resource	*libsl_id2res(sl_ios_id_t);
void			 libsl_init(int);
char			*libsl_ios2name(sl_ios_id_t);
struct sl_resm		*libsl_ios2resm(sl_ios_id_t);
struct sl_resm		*libsl_try_ios2resm(sl_ios_id_t);
struct sl_resm		*libsl_nid2resm(lnet_nid_t);
void			 libsl_profile_dump(void);
struct sl_site		*libsl_resid2site(sl_ios_id_t);
struct sl_site		*libsl_siteid2site(sl_siteid_t);
sl_ios_id_t		 libsl_str2id(const char *);
struct sl_resource	*libsl_str2res(const char *);
struct sl_resm		*libsl_try_nid2resm(lnet_nid_t);

#define libsl_ios2name(iosid)	libsl_id2res(iosid)->res_name
#define libsl_nid2iosid(nid)	libsl_nid2resm(nid)->resm_res_id

void			yyerror(const char *, ...);
void			yywarn(const char *, ...);

/* this instance's resource member */
extern struct sl_resm	*nodeResm;

#define nodeSite	nodeResm->resm_site
#define sl_resprof	nodeResm->resm_res

extern struct slcfg_local *slcfg_local;
extern struct sl_config	 globalConfig;

extern int		 cfg_site_pri_sz;
extern int		 cfg_res_pri_sz;
extern int		 cfg_resm_pri_sz;

extern char		 cfg_filename[];
extern int		 cfg_lineno;
extern struct psclist_head cfg_lnetif_pairs;

extern uint32_t		 sl_sys_upnonce;
extern int		 sl_stk_version;
extern struct timespec	 pfl_uptime;
extern int		 pfl_log_rotate;

/*
 * Produce a global, unique identifier for a resource from its internal
 * identifier.
 * @site_id: site identifier.
 * @intres_id: resource identifier, internal to site.
 */
static __inline sl_ios_id_t
sl_global_id_build(sl_siteid_t site_id, uint32_t intres_id)
{
	static int site_id_warned;
	if (!site_id && !site_id_warned) {
		site_id_warned = 1;
		psclog_warnx("Use site ID of zero is not recommended.");
	}
	if (!intres_id)
		psc_fatalx("Resource ID must be non-zero and unique!");
	psc_assert(site_id != SITE_ID_ANY);
	psc_assert(site_id < (1 << SL_SITE_BITS) - 1);
	psc_assert(intres_id < (1 << SL_RES_BITS) - 1);
	return (((sl_ios_id_t)site_id << SL_SITE_BITS) | intres_id);
}

static __inline sl_siteid_t
sl_resid_to_siteid(sl_ios_id_t id)
{
	return ((id & SL_SITE_MASK) >> SL_SITE_BITS);
}

static __inline sl_ios_id_t
sl_resid_to_int(sl_ios_id_t id)
{
	return (id & ~SL_SITE_MASK);
}

#endif /* _SLCONFIG_H_ */
