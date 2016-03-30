/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright (c) 2006-2015, Pittsburgh Supercomputing Center (PSC).
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

#include <sys/types.h>
#include <sys/socket.h>
#include <net/if.h>
#include <netinet/in.h>

#include <ctype.h>
#include <err.h>

#include "pfl/dynarray.h"
#include "pfl/hashtbl.h"
#include "pfl/list.h"
#include "pfl/log.h"
#include "pfl/net.h"
#include "pfl/str.h"

#include "slconfig.h"
#include "slerr.h"

struct psc_dynarray	sl_lnet_prids = DYNARRAY_INIT;

/*
 * Sanity check this node's resource membership.
 * Notes: must be called after LNET has been initialized.
 */
struct sl_resm *
libsl_resm_lookup(void)
{
	struct sl_resm *m, *resm = NULL;
	struct sl_resource *res = NULL;
	char nidbuf[PSCRPC_NIDSTR_SIZE];
	lnet_process_id_t *pp;
	int i;

	DYNARRAY_FOREACH(pp, i, &sl_lnet_prids) {
		if (LNET_NETTYP(LNET_NIDNET(pp->nid)) == LOLND)
			continue;

		m = libsl_try_nid2resm(pp->nid);
		if (m == NULL)
			continue;
		if (resm == NULL)
			resm = m;

#ifdef _SLASH_MDS
		if (m->resm_type != SLREST_MDS)
			continue;
#else
		if (m->resm_type == SLREST_MDS)
			continue;
#endif

		if (res == NULL)
			res = m->resm_res;
		/* All nids must belong to the same resource */
		else if (res != m->resm_res)
			psc_fatalx("nids must be members of same resource (%s)",
			    pscrpc_nid2str(pp->nid, nidbuf));
	}
	if (res == NULL)
		psc_fatalx("host is not a member in any profile");
	return (resm);
}

struct sl_site *
libsl_siteid2site(sl_siteid_t siteid)
{
	struct sl_site *s;

	CONF_LOCK();
	/* XXX hashtable or tree */
	CONF_FOREACH_SITE(s)
		if (s->site_id == siteid)
			break;
	CONF_ULOCK();
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
	uint64_t resid = id;

	return (psc_hashtbl_search(&globalConfig.gconf_res_hashtbl,
	    &resid));
}

struct sl_resm *
libsl_try_ios2resm(sl_ios_id_t id)
{
	struct sl_resource *r;

	r = libsl_id2res(id);
	if (r == NULL)
		return (NULL);
	psc_assert(RES_ISFS(r));
	return (res_getmemb(r));
}

struct sl_resm *
libsl_ios2resm(sl_ios_id_t id)
{
	struct sl_resm *m;

	m = libsl_try_ios2resm(id);
	psc_assert(m);
	return (m);
}

struct sl_resm *
libsl_try_nid2resm(lnet_nid_t nid)
{
	struct sl_resource *r;
	struct sl_resm_nid *n;
	struct sl_resm *m;
	struct sl_site *s;
	int i, j, k;

	CONF_FOREACH_RESM(s, r, i, m, j)
		DYNARRAY_FOREACH(n, k, &m->resm_nids)
			if (n->resmnid_nid == nid)
				return (m);
	return (NULL);
}

struct sl_resm *
libsl_nid2resm(lnet_nid_t nid)
{
	char nidbuf[PSCRPC_NIDSTR_SIZE];
	struct sl_resm *resm;

	resm = libsl_try_nid2resm(nid);
	if (resm == NULL)
		psc_fatalx("IOS %s not found in slcfg; "
		    "verify uniformity across all servers",
		    pscrpc_nid2str(nid, nidbuf));
	return (resm);
}

struct sl_resource *
libsl_str2res(const char *res_name)
{
	const char *site_name;
	struct sl_resource *r;
	struct sl_site *s;
	int n, locked;

	site_name = strchr(res_name, '@');
	if (site_name == NULL)
		return (NULL);
	site_name++;
	locked = CONF_RLOCK();
	CONF_FOREACH_SITE(s)
		if (strcasecmp(s->site_name, site_name) == 0)
			DYNARRAY_FOREACH(r, n, &s->site_resources)
				/* res_name includes '@SITE' in both */
				if (strcasecmp(r->res_name, res_name) == 0)
					goto done;
	r = NULL;
 done:
	CONF_URLOCK(locked);
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
	char buf[PSCRPC_NIDSTR_SIZE];
	struct sl_resource *p, *r = nodeResm->resm_res;
	struct sl_resm *resm;
	lnet_nid_t *np;
	int n, j;

	PSCLOG_LOCK();
	psclog_info("Node info: resource %s ID %#x type %d, nnids %u",
	    r->res_name, r->res_id, r->res_type,
	    psc_dynarray_len(&r->res_members));

	DYNARRAY_FOREACH(p, n, &r->res_peers)
		psclog_info("\tpeer %d: %s\t%s",
		    n, p->res_name, p->res_desc);
	DYNARRAY_FOREACH(resm, n, &r->res_members)
		DYNARRAY_FOREACH(np, j, &resm->resm_nids)
			psclog_info("\tnid %s", pscrpc_nid2str(*np, buf));

	PSCLOG_UNLOCK();
}

void
libsl_init(int nmsgs)
{
	int ri, mode = PSCNET_SERVER, rc, k, i;
	char netbuf[PSCRPC_NIDSTR_SIZE], ltmp[LNETS_MAX];
	char lnetstr[LNETS_MAX], pbuf[6], *p, *t;
	struct lnetif_pair *lp, *lpnext;
	struct sl_resource *r;
	struct sl_site *s;

	sl_errno_init();

	rc = snprintf(pbuf, sizeof(pbuf), "%d",
	    globalConfig.gconf_port);
	if (rc >= (int)sizeof(pbuf)) {
		rc = -1;
		errno = ENAMETOOLONG;
	}
	if (rc == -1)
		psc_fatal("LNET port %d", globalConfig.gconf_port);

	if (globalConfig.gconf_lroutes[0])
		setenv("LNET_ROUTES", globalConfig.gconf_lroutes, 0);

	setenv("USOCK_CPORT", pbuf, 0);
	setenv("LNET_ACCEPT_PORT", pbuf, 0);

	if (setenv("USOCK_PORTPID", "0", 1) == -1)
		err(1, "setenv");

	if (getenv("LNET_NETWORKS") || getenv("LNET_IP2NETS")) {
		psclog_info("using LNET_NETWORKS (%s) from "
		    "environment", getenv("LNET_NETWORKS"));
		goto skiplnet;
	}

	lnetstr[0] = '\0';
	psclist_for_each_entry_safe(lp, lpnext, &cfg_lnetif_pairs,
	    lentry) {
		if (lp->flags & LPF_SKIP)
			goto next;

		pscrpc_net2str(lp->net, netbuf);
		k = snprintf(ltmp, sizeof(ltmp), "%s%s(%s)",
		    lnetstr[0] == '\0' ? "" : ",", netbuf,
		    lp->ifn);
		if (k >= (int)sizeof(ltmp)) {
			k = -1;
			errno = ENAMETOOLONG;
		}
		if (k == -1)
			psc_fatal("error formatting Lustre network");

		if (strlcat(lnetstr, ltmp,
		    sizeof(lnetstr)) >= sizeof(lnetstr))
			psc_fatalx("too many Lustre networks");

 next:
		psclist_del(&lp->lentry, &cfg_lnetif_pairs);
		PSCFREE(lp);
	}

	setenv("LNET_NETWORKS", lnetstr, 0);

 skiplnet:
#ifdef _SLASH_CLIENT
	mode = PSCNET_MTCLIENT;
#endif
	pscrpc_init_portals(mode, nmsgs);
	pscrpc_getlocalprids(&sl_lnet_prids);

#ifdef _SLASH_CLIENT
	setenv("PREF_IOS", slcfg_local->cfg_prefios, 0);
	setenv("MDS", slcfg_local->cfg_prefmds, 0);
	goto out;
#endif

	nodeResm = libsl_resm_lookup();
	if (nodeResm == NULL)
		psc_fatalx("no resource member found for this node");

	/*
	 * Merge resource-specific settings into our local
	 * configuration.
	 */
	for (i = 0, p = (void *)slcfg_local,
	    t = (void *)sl_resprof->res_localcfg;
	    i < (int)sizeof(*slcfg_local); p++, t++, i++)
		if (*t)
			*p = *t;

	if (nodeResm->resm_type == SLREST_ARCHIVAL_FS) {
#ifndef HAVE_AIO
		psc_fatalx("asynchronous I/O not supported on "
		    "this platform");
#endif
		slcfg_local->cfg_async_io = 1;
	}

	psclog_diag("node is a member of resource '%s'",
	    nodeResm->resm_res->res_name);
	libsl_profile_dump();

#ifdef _SLASH_CLIENT
 out:
#endif
	CONF_FOREACH_RES(s, r, ri)
		PSCFREE(r->res_localcfg);
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

void
slcfg_destroy(void)
{
	struct sl_site *s, *s_next;
	struct sl_resource *r;
	struct sl_resm_nid *n;
	struct sl_resm *m;
	int i, j, k;

	PLL_FOREACH_SAFE(s, s_next, &globalConfig.gconf_sites) {
		SITE_FOREACH_RES(s, r, i) {
			psc_hashent_remove(
			    &globalConfig.gconf_res_hashtbl, r);
			if (RES_ISCLUSTER(r))
				goto release_res;
			RES_FOREACH_MEMB(r, m, j) {
				RESM_FOREACH_NID(m, n, k)
					PSCFREE(n);
				slcfg_destroy_resm(m);
				psc_dynarray_free(&m->resm_nids);
				PSCFREE(m);
			}
 release_res:
			slcfg_destroy_res(r);
			psc_dynarray_free(&r->res_members);
			PSCFREE(r);
		}
		slcfg_destroy_site(s);
		psc_dynarray_free(&s->site_resources);
		PSCFREE(s);
	}
	psc_hashtbl_destroy(&globalConfig.gconf_res_hashtbl);
}
