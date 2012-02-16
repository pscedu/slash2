/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2012, Pittsburgh Supercomputing Center (PSC).
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
#include "psc_util/net.h"

#include "sltypes.h"

struct sl_site;
struct slashrpc_cservice;

#define SITE_NAME_MAX		32
#define INTRES_NAME_MAX		32
#define RES_NAME_MAX		(SITE_NAME_MAX + INTRES_NAME_MAX)
#define RESM_ADDRBUF_SZ		(RES_NAME_MAX + PSCRPC_NIDSTR_SIZE)	/* foo@BAR:1.1.1.1@tcp0 */
#define LNET_NAME_MAX		32

enum sl_res_type {
	SLREST_NONE,
	SLREST_ARCHIVAL_FS,
	SLREST_CLUSTER_NOSHARE_LFS,	/* Logical set of stand-alone servers */
	SLREST_MDS,
	SLREST_PARALLEL_COMPNT,		/* A member of a parallel fs */
	SLREST_PARALLEL_LFS,		/* Logical parallel fs */
	SLREST_STANDALONE_FS
};

#define RES_ISFS(res)							\
	((res)->res_type == SLREST_ARCHIVAL_FS	||			\
	 (res)->res_type == SLREST_PARALLEL_COMPNT	||		\
	 (res)->res_type == SLREST_STANDALONE_FS)

#define RES_ISCLUSTER(res)						\
	((res)->res_type == SLREST_CLUSTER_NOSHARE_LFS	||		\
	 (res)->res_type == SLREST_PARALLEL_LFS)

/* Resource (I/O system, MDS) */
struct sl_resource {
	char			 res_name[RES_NAME_MAX];
	char			*res_desc;
	sl_ios_id_t		 res_id;
	enum sl_res_type	 res_type;
	struct psc_dynarray	 res_peers;
	struct psc_dynarray	 res_members;
	char			 res_fsroot[PATH_MAX];
	char			 res_jrnldev[PATH_MAX];
	char			 res_selftest[BUFSIZ];
	struct sl_site		*res_site;
};

#define RES_MAXID		((UINT64_C(1) << (sizeof(sl_ios_id_t) * \
				    NBBY - SLASH_FID_SITE_BITS)) - 1)

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

/* Resource member (a machine in an I/O system) */
struct sl_resm {
	struct psc_dynarray	 resm_nids;	/* network interfaces */
	struct sl_resource	*resm_res;
	struct slashrpc_cservice*resm_csvc;	/* client RPC service */
	uint32_t		 resm_stkvers;	/* peer SLASH2 stack version */
	uint32_t		 resm_upnonce;	/* tracked peer's system uptime nonce */
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

/* Site (a collection of I/O systems) */
struct sl_site {
	char			 site_name[SITE_NAME_MAX];
	char			*site_desc;
	struct psc_listentry	 site_lentry;
	struct psc_dynarray	 site_resources;
	sl_siteid_t		 site_id;
};

/* highest allowed site ID */
#define SITE_MAXID		((1 << SLASH_FID_SITE_BITS) - 1)

static __inline void *
site_get_pri(struct sl_site *site)
{
	return (site + 1);
}

struct sl_lnetrt {
	union pfl_sockaddr	 lrt_addr;
	int			 lrt_mask;	/* # bits in network */
	uint32_t		 lrt_net;
	struct psclist_head	 lrt_lentry;
};

struct lnetif_pair {
	uint32_t		 net;
	char			 ifn[IFNAMSIZ];
	struct psclist_head	 lentry;
	int			 flags;
};

#define LPF_NOACCEPTOR		(1 << 0)
#define LPF_SKIP		(1 << 1)

struct sl_gconf {
	char			 gconf_allowexe[BUFSIZ];
	char			 gconf_routes[NAME_MAX];
	char			 gconf_lnets[LNETS_MAX];
	char			 gconf_fsroot[PATH_MAX];
	int			 gconf_port;
	char			 gconf_prefmds[RES_NAME_MAX];
	char			 gconf_prefios[RES_NAME_MAX];
	char			 gconf_journal[PATH_MAX];
	char			 gconf_zpcachefn[PATH_MAX];
	char			 gconf_zpname[NAME_MAX];
	int			 gconf_async_io;
	int			 gconf_fidnsdepth;

	struct psclist_head	 gconf_routehd;
	struct psc_lockedlist	 gconf_sites;
	psc_spinlock_t		 gconf_lock;
	uint64_t		 gconf_fsuuid;
};

#define GCONF_HASHTBL_SZ	63
#define INIT_GCONF(cf)							\
	do {								\
		memset((cf), 0, sizeof(*(cf)));				\
		INIT_LISTHEAD(&(cf)->gconf_routehd);			\
		INIT_SPINLOCK(&(cf)->gconf_lock);			\
		pll_init(&(cf)->gconf_sites, struct sl_site,		\
		    site_lentry, &(cf)->gconf_lock);			\
	} while (0)

#define CONF_LOCK()			spinlock(&globalConfig.gconf_lock)
#define CONF_ULOCK()			freelock(&globalConfig.gconf_lock)
#define CONF_RLOCK()			reqlock(&globalConfig.gconf_lock)
#define CONF_URLOCK(lk)			ureqlock(&globalConfig.gconf_lock, (lk))

#define CONF_FOREACH_SITE(s)		PLL_FOREACH((s), &globalConfig.gconf_sites)
#define SITE_FOREACH_RES(s, r, i)	DYNARRAY_FOREACH((r), (i), &(s)->site_resources)
#define RES_FOREACH_MEMB(r, m, j)	DYNARRAY_FOREACH((m), (j), &(r)->res_members)
#define RESM_FOREACH_NID(m, n, k)	DYNARRAY_FOREACH((n), (k), &(m)->resm_nids)

#define CONF_FOREACH_SITE_CONT(s)	PLL_FOREACH_CONT((s), &globalConfig.gconf_sites)
#define SITE_FOREACH_RES_CONT(s, r, i)	DYNARRAY_FOREACH_CONT((r), (i), &(s)->site_resources)
#define RES_FOREACH_MEMB_CONT(r, m, j)	DYNARRAY_FOREACH_CONT((m), (j), &(r)->res_members)
#define RESM_FOREACH_NID_CONT(m, n, k)	DYNARRAY_FOREACH_CONT((n), (k), &(m)->resm_nids)

#define CONF_FOREACH_RES(s, r, i)					\
	CONF_FOREACH_SITE(s)						\
		SITE_FOREACH_RES((s), (r), (i))

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

int                      slcfg_get_ioslist(sl_ios_id_t,
				   struct psc_dynarray *, int);

int			 slcfg_res_cmp(const void *, const void *);
int			 slcfg_resm_cmp(const void *, const void *);
int			 slcfg_site_cmp(const void *, const void *);

void			 slcfg_parse(const char *);
void			 slcfg_resm_addaddr(char *, const char *);

struct sl_resource	*libsl_id2res(sl_ios_id_t);
void			 libsl_init(int);
char			*libsl_ios2name(sl_ios_id_t);
struct sl_resm		*libsl_ios2resm(sl_ios_id_t);
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
#define nodeResProf	nodeResm->resm_res

extern struct sl_gconf	 globalConfig;

extern int		 cfg_site_pri_sz;
extern int		 cfg_res_pri_sz;
extern int		 cfg_resm_pri_sz;

extern char		 cfg_filename[];
extern int		 cfg_lineno;
extern struct psclist_head cfg_lnetif_pairs;

/**
 * sl_global_id_build - Produce a global, unique identifier for a
 *	resource from its internal identifier.
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

static __inline sl_ios_id_t
sl_resid_to_int(sl_ios_id_t id)
{
	return (id & ~SL_SITE_MASK);
}

#endif /* _SLCONFIG_H_ */
