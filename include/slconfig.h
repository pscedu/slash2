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

#define SITE_NAME_MAX	64
#define RES_NAME_MAX	64
#define FULL_NAME_MAX	(SITE_NAME_MAX + RES_NAME_MAX + 2)	/* '@' + NUL */
#define SL_PEER_MAX	16
#define DEVNAMEMAX	128
#define MAXNET		32

#define slashGetConfig run_yacc

enum sl_res_type {
	SLREST_PARALLEL_FS,
	SLREST_ARCHIVAL_FS,
	SLREST_CLUSTER_NOSHARE_FS,
	SLREST_COMPUTE
};

struct sl_resource {
	char			 res_name[RES_NAME_MAX];
	char			*res_desc;
	char			*res_peertmp[SL_PEER_MAX];
	sl_ios_id_t		 res_id;
	sl_ios_id_t		 res_mds;
	enum sl_res_type	 res_type;
	sl_ios_id_t		*res_peers;
	uint32_t		 res_npeers;
	lnet_nid_t		*res_nids;
	uint32_t		 res_nnids;
	char			 res_fsroot[PATH_MAX];
	struct psclist_head	 res_lentry;
	void			*res_pri;
};

#define INIT_RES(r) INIT_PSCLIST_ENTRY(&(r)->res_lentry)

struct sl_site {
	char			 site_name[SITE_NAME_MAX];
	char			*site_desc;
	uint32_t		 site_id;
	struct psclist_head	 site_resources;
	struct psclist_head	 site_lentry;
};

#define INIT_SITE(s)						\
	do {							\
		INIT_PSCLIST_ENTRY(&(s)->site_lentry);		\
		INIT_PSCLIST_HEAD(&(s)->site_resources);	\
	} while (0)

struct sl_nodeh {
	struct sl_resource	*node_res;
	struct sl_site		*node_site;
};

struct sl_gconf {
	char			 gconf_net[MAXNET];
	char			 gconf_fdbkeyfn[PATH_MAX];
	uint32_t		 gconf_netid;
	int			 gconf_port;
	int			 gconf_nsites;
	struct psclist_head	 gconf_sites;
	struct hash_table	 gconf_nids_hash;
	psc_spinlock_t		 gconf_lock;
};

#define GCONF_HASHTBL_SZ 63
#define INIT_GCONF(g)						\
	do {							\
		memset((g), 0, sizeof(*(g)));			\
		INIT_PSCLIST_HEAD(&(g)->gconf_sites);		\
		LOCK_INIT(&(g)->gconf_lock);			\
		init_hash_table(&(g)->gconf_nids_hash,		\
				GCONF_HASHTBL_SZ, "resnid");	\
	} while (0)

#define GCONF_LOCK()	spinlock(&globalConfig.gconf_lock)
#define GCONF_ULOCK()	freelock(&globalConfig.gconf_lock)

struct sl_resm {
	lnet_nid_t		 resm_nid;
	struct sl_resource	*resm_res;
	struct hash_entry	 resm_hentry;
	void			*resm_pri;
};

#define slresm_2_resid(r) (r)->resm_res->res_id

struct sl_resource	*slcfg_new_res(void);
struct sl_resm		*slcfg_new_resm(void);

extern struct sl_nodeh nodeInfo;
extern struct sl_gconf globalConfig;

static inline void
libsl_nid_associate(lnet_nid_t nid, struct sl_resource *res)
{
	struct sl_resm *resm = slcfg_new_resm();

	resm->resm_nid = nid;
	resm->resm_res = res;
	init_hash_entry(&resm->resm_hentry, (void *)&resm->resm_nid, resm);
	add_hash_entry(&globalConfig.gconf_nids_hash, &resm->resm_hentry);
}

int lnet_localnids_get(lnet_nid_t *, size_t);

#define MAX_LOCALNIDS 128
/*
 * libsl_resm_lookup - To be called after LNET initialization, determines
 * a node's resource membership.
 */
static inline struct sl_resm *
libsl_resm_lookup(void)
{
	lnet_nid_t nids[MAX_LOCALNIDS];
	char nidbuf[PSC_NIDSTR_SIZE];
	struct sl_resource *res=NULL;
	struct sl_resm *resm=NULL;
	struct hash_entry *e;
	int nnids, i;

	nnids = lnet_localnids_get(nids, nitems(nids));
	if (!nnids)
		return (NULL);

	for (i=0; i<nnids; i++) {
		e = get_hash_entry(&globalConfig.gconf_nids_hash,
		    nids[i], NULL, NULL);
		/* Every nid found by lnet must be a resource member.  */
		if (!e)
			psc_fatalx("nid %s is not a member of any resource",
				   psc_nid2str(nids[i], nidbuf));

		resm = e->private;
		if (!i)
			res = resm->resm_res;
		/* All nids must belong to the same resource */
		else if (res != resm->resm_res)
			psc_fatalx("nids must be members of same resource (%s)",
				psc_nid2str(nids[i], nidbuf));
	}
	return (resm);
}

static inline sl_ios_id_t
libsl_node2id(struct sl_nodeh *n)
{
	return (sl_global_id_build(n->node_site->site_id,
				   n->node_res->res_id,
				   n->node_res->res_mds));
}

static inline struct sl_site *
libsl_id2site(sl_ios_id_t id)
{
	uint32_t tmp = (id >> (SL_RES_BITS + SL_MDS_BITS));
	struct sl_site *s;

	psc_assert(tmp <= ((1 << SL_SITE_BITS))-1);

	psclist_for_each_entry(s, &globalConfig.gconf_sites, site_lentry)
		if (tmp == s->site_id)
			return (s);
	return (NULL);
}

static inline struct sl_resource *
libsl_id2res(sl_ios_id_t id)
{
	struct sl_resource *r;
	struct sl_site *s;

	if ((s = libsl_id2site(id)) == NULL)
		return NULL;

	/* The global ID is now stored as the resource id (res_id).
	 *  local id's are deprecated for now.
	 */
	//sl_ios_id_t    rid = sl_glid_to_resid(id);

	psclist_for_each_entry(r, &s->site_resources, res_lentry)
		if (id == r->res_id)
			return (r);
	return (NULL);
}

static inline struct sl_resm *
libsl_nid2resm(lnet_nid_t nid)
{
	struct hash_entry *e;

	e = get_hash_entry(&globalConfig.gconf_nids_hash, nid, NULL, NULL);
	if (!e)
		return (NULL);

	psc_assert(*e->hentry_id == nid);
	return (e->private);
}

static inline sl_ios_id_t
libsl_str2id(const char *res_name)
{
	sl_ios_id_t id = IOS_ID_ANY;
	const char *p = res_name;
	struct sl_resource *r;
	struct sl_site *s;

	p = strchr(res_name, '@');
	if (p == NULL)
		return (IOS_ID_ANY);
	GCONF_LOCK();
	psclist_for_each_entry(s, &globalConfig.gconf_sites, site_lentry)
		if (strcmp(s->site_name, p) == 0)
			psclist_for_each_entry(r, &s->site_resources,
			    res_lentry)
				if (strcmp(r->res_name, res_name) == 0) {
					id = r->res_id;
					break;
				}
	GCONF_ULOCK();
	return (id);
}

static inline void
libsl_profile_dump(void)
{
	struct sl_nodeh *z = &nodeInfo;
	struct sl_resource *r;
	uint32_t i;

	fprintf(stderr,
		"\nNode Info: resource %s\n"
		"\tdesc: %s "
		"\n\t id (global=%u, mds=%u)"
		"\n\t type %d, npeers %u, nnids %u"
		"\n\t fsroot %s\n",
		z->node_res->res_name,
		z->node_res->res_desc,
		z->node_res->res_id,
		z->node_res->res_mds,
		z->node_res->res_type,
		z->node_res->res_npeers,
		z->node_res->res_nnids,
		z->node_res->res_fsroot);

	for (i=0; i < z->node_res->res_npeers; i++) {
		r = libsl_id2res(z->node_res->res_peers[i]);
		if (!r)
			continue;
		fprintf(stderr,"\tpeer %d ;%s;\t%s",
			i, r->res_name, r->res_desc);
	}
	for (i=0; i < z->node_res->res_nnids; i++)
		fprintf(stderr,"\tnid %d ;%s;\n",
			i, libcfs_nid2str(z->node_res->res_nids[i]));
}

static inline uint32_t
libsl_str2restype(const char *res_type)
{
	if (!strcmp(res_type, "parallel_fs"))
		return (SLREST_PARALLEL_FS);
	else if (!strcmp(res_type, "archival_fs"))
		return (SLREST_ARCHIVAL_FS);
	else if (!strcmp(res_type, "cluster_noshare_fs"))
		return (SLREST_CLUSTER_NOSHARE_FS);
	else if (!strcmp(res_type, "compute"))
		return (SLREST_COMPUTE);
	psc_fatalx("invalid type");
}

static __inline struct sl_site *
slcfg_new_site(void)
{
	struct sl_site *site;

	site = PSCALLOC(sizeof(*site));
	INIT_SITE(site);
	return (site);
}

static inline void
libsl_init(int pscnet_mode)
{
	struct sl_nodeh *z = &nodeInfo;
	struct sl_resm *resm;

	//lnet_acceptor_port = globalConfig.gconf_port;
	//setenv("USOCK_CPORT", globalConfig.gconf_port, 1);
	//setenv("LNET_ACCEPT_PORT", globalConfig.gconf_port, 1);

	pscrpc_init_portals(pscnet_mode);

	if (pscnet_mode == PSCNET_SERVER) {
		resm = libsl_resm_lookup();
		if (!resm)
			psc_fatalx("No resource for this node");
		psc_errorx("Resource %s", resm->resm_res->res_name);
		z->node_res  = resm->resm_res;
		z->node_site = libsl_id2site(z->node_res->res_id);
		libsl_profile_dump();
	}
}

int run_yacc(const char *);

#endif /* _SLCONFIG_H_ */
