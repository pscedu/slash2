/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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

#include "psc_ds/hash.h"
#include "psc_ds/dynarray.h"
#include "psc_util/log.h"

#include "slconfig.h"

struct psc_dynarray lnet_nids = DYNARRAY_INIT;

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
	DYNARRAY_FOREACH(r, n, &s->site_resources)
		if (id == r->res_id)
			return (r);
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
			DYNARRAY_FOREACH(r, n, &s->site_resources)
				/* res_name includes '@SITE' in both */
				if (strcmp(r->res_name, res_name) == 0)
					goto done;
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
	struct sl_resource *p, *r = nodeResm->resm_res;
	struct sl_resm *resm;
	int n;

	fprintf(stderr,
	    "Node info: resource %s id %u\n"
	    "\tdesc: %s\n"
	    "\ttype %d, npeers %u, nnids %u\n"
	    "\tfsroot %s\n",
	    r->res_name, r->res_id,
	    r->res_desc,
	    r->res_type, r->res_npeers, psc_dynarray_len(&r->res_members),
	    r->res_fsroot);

	DYNARRAY_FOREACH(p, n, &r->res_peers)
		fprintf(stderr, "\tpeer %d: %s\t%s\n",
			n, p->res_name, p->res_desc);
	DYNARRAY_FOREACH(resm, n, &r->res_members)
		fprintf(stderr, "\tnid %d: %s\n", n, resm->resm_addrbuf);
}

void
libsl_init(int pscnet_mode, int ismds)
{
	//lnet_acceptor_port = globalConfig.gconf_port;
	//setenv("USOCK_CPORT", globalConfig.gconf_port, 1);
	//setenv("LNET_ACCEPT_PORT", globalConfig.gconf_port, 1);

	psc_assert(pscnet_mode == PSCNET_CLIENT ||
	    pscnet_mode == PSCNET_SERVER);

	pscrpc_init_portals(pscnet_mode);
	pscrpc_getlocalnids(&lnet_nids);

	if (pscnet_mode == PSCNET_SERVER) {
		nodeResm = libsl_resm_lookup(ismds);
		if (nodeResm == NULL)
			psc_fatalx("No resource member for this node");
		psc_errorx("Resource %s", nodeResm->resm_res->res_name);
		libsl_profile_dump();
	}
}

int
slcfg_site_cmp(const void *a, const void *b)
{
	struct sl_site * const *px = a, *x = *px;
	struct sl_site * const *py = b, *y = *py;

	return (CMP(x->site_id, y->site_id));
}

int
slcfg_res_cmp(const void *a, const void *b)
{
	const struct sl_resource * const *px = a, *x = *px;
	const struct sl_resource * const *py = b, *y = *py;

	return (CMP(x->res_id, y->res_id));
}

int
slcfg_resm_cmp(const void *a, const void *b)
{
	const struct sl_resm * const *px = a, *x = *px;
	const struct sl_resm * const *py = b, *y = *py;

	return (CMP(x->resm_nid, y->resm_nid));
}
