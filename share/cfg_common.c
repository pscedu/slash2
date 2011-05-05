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

#include <sys/types.h>
#include <sys/socket.h>

#include <net/if.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include <err.h>
#include <netdb.h>

#include "pfl/hashtbl.h"
#include "pfl/str.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_util/log.h"
#include "psc_util/net.h"

#include "slconfig.h"
#include "slerr.h"

struct psc_dynarray	lnet_prids = DYNARRAY_INIT;

/*
 * libsl_resm_lookup - Sanity check this node's resource membership.
 * Notes: must be called after LNET has been initialized.
 */
struct sl_resm *
libsl_resm_lookup(int ismds)
{
	char nidbuf[PSCRPC_NIDSTR_SIZE];
	struct sl_resource *res = NULL;
	struct sl_resm *resm = NULL;
	lnet_process_id_t *pp;
	int i;

	DYNARRAY_FOREACH(pp, i, &lnet_prids) {
		if (LNET_NETTYP(LNET_NIDNET(pp->nid)) == LOLND)
			continue;

		resm = psc_hashtbl_search(&globalConfig.gconf_nid_hashtbl,
		    NULL, NULL, &pp->nid);
		/* Every nid found by lnet must be a resource member. */
		if (resm == NULL)
			psc_fatalx("nid %s is not a member of any resource",
			    pscrpc_nid2str(pp->nid, nidbuf));

		if (res == NULL)
			res = resm->resm_res;
		/* All nids must belong to the same resource */
		else if (res != resm->resm_res)
			psc_fatalx("nids must be members of same resource (%s)",
			    pscrpc_nid2str(pp->nid, nidbuf));
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
libsl_try_nid2resm(lnet_nid_t nid)
{
	return (psc_hashtbl_search(&globalConfig.gconf_nid_hashtbl,
	    NULL, NULL, &nid));
}

struct sl_resm *
libsl_nid2resm(lnet_nid_t nid)
{
	char nidbuf[PSCRPC_NIDSTR_SIZE];
	struct sl_resm *resm;

	resm = libsl_try_nid2resm(nid);
	if (resm)
		return (resm);
	psc_fatalx("IOS %s not found in SLASH configuration, "
	    "verify uniformity across all servers.",
	    pscrpc_nid2str(nid, nidbuf));
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
	locked = PLL_RLOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		if (strcasecmp(s->site_name, site_name) == 0)
			DYNARRAY_FOREACH(r, n, &s->site_resources)
				/* res_name includes '@SITE' in both */
				if (strcasecmp(r->res_name, res_name) == 0)
					goto done;
	r = NULL;
 done:
	PLL_URLOCK(&globalConfig.gconf_sites, locked);
	return (r);
}

/* Use to convert SLASH2_PIOS_ID name to its ID */
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

	PSCLOG_LOCK();
	psclog_info("Node info: resource %s ID %#x type %d, npeers %u, "
	    "nnids %u",
	    r->res_name, r->res_id, r->res_type, r->res_npeers,
	    psc_dynarray_len(&r->res_members));

	DYNARRAY_FOREACH(p, n, &r->res_peers)
		psclog_info("\tpeer %d: %s\t%s",
		    n, p->res_name, p->res_desc);
	DYNARRAY_FOREACH(resm, n, &r->res_members)
		psclog_info("\tnid %d: %s", n, resm->resm_addrbuf);
	PSCLOG_UNLOCK();
}

int
slcfg_ifcmp(const char *a, const char *b)
{
	char *p, ia[IFNAMSIZ], ib[IFNAMSIZ];

	strlcpy(ia, a, IFNAMSIZ);
	strlcpy(ib, b, IFNAMSIZ);

	p = strchr(ia, ':');
	if (p)
		*p = '\0';

	p = strchr(ib, ':');
	if (p)
		*p = '\0';

	return (strcmp(ia, ib));
}

#define LNETWORKS_STR_SIZE 256

void
libsl_init(int pscnet_mode, int ismds)
{
	struct {
		char			*net;
		char			 ifn[IFNAMSIZ];
		struct psclist_head	 lentry;
	} *lent, *lnext;
	char ltmp[LNETWORKS_STR_SIZE], lnetstr[LNETWORKS_STR_SIZE];
	char pbuf[6], *p, addrbuf[HOST_NAME_MAX];
	struct addrinfo hints, *res, *res0;
	int netcmp, error, rc, j, k;
	PSCLIST_HEAD(lnets_hd);
	struct sl_resource *r;
	struct ifaddrs *ifa;
	struct sl_resm *m;
	struct sl_site *s;

	psc_assert(pscnet_mode == PSCNET_CLIENT ||
	    pscnet_mode == PSCNET_SERVER);

	rc = snprintf(pbuf, sizeof(pbuf), "%d", globalConfig.gconf_port);
	if (rc == -1)
		psc_fatal("LNET port %d", globalConfig.gconf_port);
	if (rc >= (int)sizeof(pbuf))
		psc_fatalx("LNET port %d: too long", globalConfig.gconf_port);

	setenv("USOCK_CPORT", pbuf, 0);
	setenv("LNET_ACCEPT_PORT", pbuf, 0);

	if (setenv("USOCK_PORTPID", "0", 1) == -1)
		err(1, "setenv");

	if (getenv("LNET_NETWORKS")) {
		psclog_notice("using LNET_NETWORKS (%s) from "
		    "environment", getenv("LNET_NETWORKS"));
		goto skiplnet;
	}

	pflnet_getifaddrs(&ifa);

	lent = PSCALLOC(sizeof(*lent));
	INIT_PSC_LISTENTRY(&lent->lentry);

	PLL_LOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		DYNARRAY_FOREACH(r, j, &s->site_resources)
			DYNARRAY_FOREACH(m, k, &r->res_members) {
				if (ismds ^ (r->res_type == SLREST_MDS))
					continue;

				p = strchr(m->resm_addrbuf, ':');
				psc_assert(p);
				strlcpy(addrbuf, p + 1, sizeof(addrbuf));
				p = strrchr(addrbuf, '@');
				psc_assert(p);
				*p = '\0';

				/* get numerical addresses */
				memset(&hints, 0, sizeof(hints));
				hints.ai_family = PF_INET;
				hints.ai_socktype = SOCK_STREAM;
				error = getaddrinfo(addrbuf, NULL, &hints, &res0);
				if (error)
					psc_fatalx("%s: %s", addrbuf, gai_strerror(error));

				for (res = res0; res; res = res->ai_next) {
					/* get destination routing interface */
					pflnet_getifnfordst(ifa, res->ai_addr, lent->ifn);
					lent->net = strrchr(m->resm_addrbuf, '@') + 1;

					/*
					 * Ensure mutual exclusion of this
					 * interface and lustre network,
					 * ignoring any interface aliases.
					 */
					netcmp = 1;
					psclist_for_each_entry(lnext,
					    &lnets_hd, lentry) {
						netcmp = strcmp(lnext->net,
						    lent->net);

						if (netcmp ^
						    slcfg_ifcmp(lent->ifn, lnext->ifn))
							psc_fatalx("network/interface "
							    "pair %s:%s conflicts with "
							    "%s:%s",
							    lent->net, lent->ifn,
							    lnext->net, lnext->ifn);
						/* if the same, don't process more */
						if (!netcmp)
							break;
					}

					if (netcmp) {
						psclist_add(&lent->lentry, &lnets_hd);

						lent = PSCALLOC(sizeof(*lent));
						INIT_PSC_LISTENTRY(&lent->lentry);
					}
				}
				freeaddrinfo(res0);
			}
	PLL_ULOCK(&globalConfig.gconf_sites);

	PSCFREE(lent);
	pflnet_freeifaddrs(ifa);

	lnetstr[0] = '\0';
	psclist_for_each_entry_safe(lent, lnext, &lnets_hd, lentry) {
		k = snprintf(ltmp, sizeof(ltmp), "%s%s(%s)",
		    lnetstr[0] == '\0' ? "" : ",", lent->net, lent->ifn);
		if (k == -1)
			psc_fatal("error formatting lustre network");
		if (k >= (int)sizeof(ltmp))
			psc_fatalx("lustre network too long");

		if (strlcat(lnetstr, ltmp,
		    sizeof(lnetstr)) >= sizeof(lnetstr))
			psc_fatalx("too many lustre networks");

		psclist_del(&lent->lentry, &lnets_hd);
		PSCFREE(lent);
	}

	setenv("LNET_NETWORKS", lnetstr, 0);

 skiplnet:

	pscrpc_init_portals(pscnet_mode);
	pscrpc_getlocalprids(&lnet_prids);

	if (pscnet_mode == PSCNET_SERVER) {
		nodeResm = libsl_resm_lookup(ismds);
		if (nodeResm == NULL)
			psc_fatalx("No resource member for this node");

		if (nodeResm->resm_res->res_fsroot[0] != '\0')
			strlcpy(globalConfig.gconf_fsroot, nodeResm->resm_res->res_fsroot,
			    sizeof(globalConfig.gconf_fsroot));

		psc_info("Resource %s", nodeResm->resm_res->res_name);
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
