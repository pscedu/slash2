/* $Id$ */

#include "psc_ds/hash.h"
#include "psc_util/log.h"

#include "slconfig.h"

void
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
struct sl_resm *
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

sl_ios_id_t
libsl_node2id(struct sl_nodeh *n)
{
	return (sl_global_id_build(n->node_site->site_id,
				   n->node_res->res_id,
				   n->node_res->res_mds));
}

struct sl_site *
libsl_id2site(sl_ios_id_t id)
{
	uint32_t tmp = (id >> (SL_RES_BITS + SL_MDS_BITS));
	struct sl_site *s;

	psc_assert(tmp <= ((1 << SL_SITE_BITS))-1);

	PLL_FOREACH(s, &globalConfig.gconf_sites)
		if (tmp == s->site_id)
			return (s);
	return (NULL);
}

struct sl_resource *
libsl_id2res(sl_ios_id_t id)
{
	struct sl_resource *r;
	struct sl_site *s;
	int n;

	if ((s = libsl_id2site(id)) == NULL)
		return NULL;

	/* The global ID is now stored as the resource id (res_id).
	 *  local id's are deprecated for now.
	 */
	//sl_ios_id_t    rid = sl_glid_to_resid(id);

	for (n = 0; n < s->site_nres; n++) {
		r = s->site_resv[n];
		/* XXX this part doesn't make sense... */
		if (id == r->res_id)
			return (r);
	}
	return (NULL);
}

struct sl_resm *
libsl_nid2resm(lnet_nid_t nid)
{
	struct hash_entry *e;

	e = get_hash_entry(&globalConfig.gconf_nids_hash, nid, NULL, NULL);
	if (!e)
		return (NULL);

	psc_assert(*e->hentry_id == nid);
	return (e->private);
}

sl_ios_id_t
libsl_str2id(const char *res_name)
{
	sl_ios_id_t id = IOS_ID_ANY;
	const char *p = res_name;
	struct sl_resource *r;
	struct sl_site *s;
	int n;

	p = strchr(res_name, '@');
	if (p == NULL)
		return (IOS_ID_ANY);
	PLL_LOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		if (strcmp(s->site_name, p) == 0)
			for (n = 0; n < s->site_nres; n++) {
				r = s->site_resv[n];
				if (strcmp(r->res_name, res_name) == 0) {
					id = r->res_id;
					goto done;
				}
			}
 done:
	PLL_ULOCK(&globalConfig.gconf_sites);
	return (id);
}

void
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

uint32_t
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

void
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

int
slcfg_site_cmp(const void *a, const void *b)
{
	const struct sl_site *x = a, *y = b;

	if (x->site_id < y->site_id)
		return (-1);
	else if (x->site_id > y->site_id)
		return (1);
	return (0);
}

int
slcfg_res_cmp(const void *a, const void *b)
{
	const struct sl_resource *x = a, *y = b;

	if (x->res_id < y->res_id)
		return (-1);
	else if (x->res_id > y->res_id)
		return (1);
	return (0);
}

int
slcfg_resnid_cmp(const void *a, const void *b)
{
	const lnet_nid_t *x = a, *y = b;

	if (*x < *y)
		return (-1);
	else if (*x > *y)
		return (1);
	return (0);
}
