/* $Id$ */

#include "psc_ds/hash.h"
#include "psc_ds/dynarray.h"
#include "psc_util/log.h"

#include "slconfig.h"

struct psc_dynarray lnet_nids = DYNARRAY_INIT;

void
libsl_nid_associate(lnet_nid_t nid, struct sl_resource *res)
{
	char nidbuf[PSC_NIDSTR_SIZE];
	struct sl_resm *resm;
	int rc;

	psc_nid2str(nid, nidbuf);

	resm = PSCALLOC(sizeof(*resm));
	rc = snprintf(resm->resm_addrbuf, sizeof(resm->resm_addrbuf),
	    "%s:%s", res->res_name, nidbuf);
	if (rc == -1)
		psc_fatal("resource member %s:%s", res->res_name, nidbuf);
	if (rc >= (int)sizeof(resm->resm_addrbuf))
		psc_fatalx("resource member %s:%s: address too long",
		    res->res_name, nidbuf);

	resm->resm_nid = nid;
	resm->resm_res = res;
	slcfg_init_resm(resm);

	init_hash_entry(&resm->resm_hentry, (void *)&resm->resm_nid, resm);
	add_hash_entry(&globalConfig.gconf_nids_hash, &resm->resm_hentry);
}

/*
 * libsl_resm_lookup - Sanity check this node's resource membership.
 * Notes: must be called after LNET has been initialized.
 */
struct sl_resm *
libsl_resm_lookup(int ismds)
{
	char nidbuf[PSC_NIDSTR_SIZE];
	struct sl_resource *res=NULL;
	struct sl_resm *resm=NULL;
	struct hash_entry *e;
	lnet_nid_t *np;
	int i;

	DYNARRAY_FOREACH(np, i, &lnet_nids) {
		if (LNET_NETTYP(LNET_NIDNET(*np)) == LOLND)
			continue;

		e = get_hash_entry(&globalConfig.gconf_nids_hash,
		    *np, NULL, NULL);
		/* Every nid found by lnet must be a resource member. */
		if (!e)
			psc_fatalx("nid %s is not a member of any resource",
				   psc_nid2str(*np, nidbuf));

		resm = e->private;
		if (res == NULL)
			res = resm->resm_res;
		/* All nids must belong to the same resource */
		else if (res != resm->resm_res)
			psc_fatalx("nids must be members of same resource (%s)",
				psc_nid2str(*np, nidbuf));
	}
	if (ismds && res->res_type != SLREST_MDS)
		psc_fatal("%s: not configured as MDS", res->res_name);
	else if (!ismds && res->res_type == SLREST_MDS)
		psc_fatal("%s: not configured as ION", res->res_name);
	return (resm);
}

struct sl_site *
libsl_siteid2site(sl_siteid_t siteid)
{
	struct sl_site *s;

	PLL_LOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		if (s->site_id == siteid)
			break;
	PLL_ULOCK(&globalConfig.gconf_sites);
	return (s);
}

struct sl_site *
libsl_resid2site(sl_ios_id_t id)
{
	return (libsl_siteid2site(sl_resid_to_siteid(id)));
}

struct sl_resource *
libsl_id2res(sl_ios_id_t id)
{
	struct sl_resource *r;
	struct sl_site *s;
	int n;

	if ((s = libsl_resid2site(id)) == NULL)
		return (NULL);
	for (n = 0; n < s->site_nres; n++) {
		r = s->site_resv[n];
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

struct sl_resource *
libsl_str2res(const char *res_name)
{
	const char *site_name;
	struct sl_resource *r;
	struct sl_site *s;
	int n;

	site_name = strchr(res_name, '@');
	if (site_name == NULL)
		return (NULL);
	PLL_LOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		if (strcmp(s->site_name, site_name) == 0)
			for (n = 0; n < s->site_nres; n++) {
				r = s->site_resv[n];
				/* res_name includes '@SITE' in both */
				if (strcmp(r->res_name, res_name) == 0)
					goto done;
			}
	r = NULL;
 done:
	PLL_ULOCK(&globalConfig.gconf_sites);
	return (r);
}

sl_ios_id_t
libsl_str2id(const char *name)
{
	struct sl_resource *res;

	res = libsl_str2res(name);
	if (res)
		return (res->res_id);
	return (IOS_ID_ANY);
}

void
libsl_profile_dump(void)
{
	struct sl_nodeh *n = &nodeInfo;
	struct sl_resource *r;
	uint32_t i;

	r = n->node_res;

	fprintf(stderr,
	    "Node info: resource %s id %u\n"
	    "\tdesc: %s\n"
	    "\ttype %d, npeers %u, nnids %u\n"
	    "\tfsroot %s\n",
	    r->res_name, r->res_id,
	    r->res_desc,
	    r->res_type, r->res_npeers, r->res_nnids,
	    r->res_fsroot);

	for (i = 0; i < n->node_res->res_npeers; i++) {
		r = libsl_id2res(n->node_res->res_peers[i]);
		if (!r)
			continue;
		fprintf(stderr, "\tpeer %d ;%s;\t%s\n",
			i, r->res_name, r->res_desc);
	}
	for (i = 0; i < n->node_res->res_nnids; i++)
		fprintf(stderr, "\tnid %d ;%s;\n",
			i, libcfs_nid2str(n->node_res->res_nids[i]));
}

void
libsl_init(int pscnet_mode, int ismds)
{
	struct sl_nodeh *n = &nodeInfo;
	struct sl_resm *resm;

	//lnet_acceptor_port = globalConfig.gconf_port;
	//setenv("USOCK_CPORT", globalConfig.gconf_port, 1);
	//setenv("LNET_ACCEPT_PORT", globalConfig.gconf_port, 1);

	psc_assert(pscnet_mode == PSCNET_CLIENT ||
	    pscnet_mode == PSCNET_SERVER);

	pscrpc_init_portals(pscnet_mode);
	pscrpc_getlocalnids(&lnet_nids);

	if (pscnet_mode == PSCNET_SERVER) {
		resm = libsl_resm_lookup(ismds);
		if (!resm)
			psc_fatalx("No resource for this node");
		psc_errorx("Resource %s", resm->resm_res->res_name);
		n->node_res  = resm->resm_res;
		n->node_site = libsl_resid2site(n->node_res->res_id);
		libsl_profile_dump();
	}
}

int
slcfg_site_cmp(const void *a, const void *b)
{
	struct sl_site * const *px = a, *x = *px, * const *py = b, *y = *py;

	return (CMP(x->site_id, y->site_id));
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
