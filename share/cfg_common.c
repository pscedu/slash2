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

/**
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
			continue;

		if (ismds && resm->resm_type != SLREST_MDS)
			continue;
		if (!ismds && resm->resm_type == SLREST_MDS)
			continue;

		if (res == NULL)
			res = resm->resm_res;
		/* All nids must belong to the same resource */
		else if (res != resm->resm_res)
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
	site_name++;
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
	psclog_info("Node info: resource %s ID %#x type %d, nnids %u",
	    r->res_name, r->res_id, r->res_type,
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

struct lnetif_pair {
	uint32_t		 net;
	char			 ifn[IFNAMSIZ];
	struct psclist_head	 lentry;
};

void
slcfg_add_lnet(struct psclist_head *hd, struct ifaddrs *ifa,
    const void *sa, uint32_t net, struct lnetif_pair **lentp)
{
	char buf[PSCRPC_NIDSTR_SIZE], ibuf[PSCRPC_NIDSTR_SIZE];
	struct lnetif_pair *i, *lent = *lentp;
	int netcmp = 1;

	/* get destination routing interface */
	pflnet_getifnfordst(ifa, sa, lent->ifn);
	lent->net = net;

	pscrpc_net2str(lent->net, buf);

	/*
	 * Ensure mutual exclusion of this interface and Lustre network,
	 * ignoring any interface aliases.
	 */
	psclist_for_each_entry(i, hd, lentry) {
		netcmp = i->net != lent->net;

		if (netcmp ^ slcfg_ifcmp(lent->ifn, i->ifn)) {
			pscrpc_net2str(i->net, ibuf);
			psc_fatalx("network/interface pair %s:%s "
			    "conflicts with %s:%s",
			    buf, lent->ifn,
			    ibuf, i->ifn);
		}

		/* if the same, don't process more */
		if (!netcmp)
			return;
	}

	psclist_add(&lent->lentry, hd);
	*lentp = lent = PSCALLOC(sizeof(*lent));
	INIT_PSC_LISTENTRY(&lent->lentry);
}

void
libsl_init(int pscnet_mode, int ismds)
{
	char lnetstr[LNETS_MAX], pbuf[6], *p, addrbuf[HOST_NAME_MAX];
	char netbuf[PSCRPC_NIDSTR_SIZE], ltmp[LNETS_MAX];
	struct addrinfo hints, *res, *res0;
	struct lnetif_pair *lent, *lnext;
	struct sockaddr_in sin;
	struct sl_resource *r;
	struct ifaddrs *ifa;
	struct sl_resm *m;
	struct sl_site *s;
	PSCLIST_HEAD(lnets_hd);
	int error, rc, j, k, l;
	lnet_nid_t *nidp;
	uint32_t net;

	psc_assert(pscnet_mode == PSCNET_CLIENT ||
	    pscnet_mode == PSCNET_SERVER);

	rc = snprintf(pbuf, sizeof(pbuf), "%d",
	    globalConfig.gconf_port);
	if (rc >= (int)sizeof(pbuf)) {
		rc = -1;
		errno = ENAMETOOLONG;
	}
	if (rc == -1)
		psc_fatal("LNET port %d", globalConfig.gconf_port);

	setenv("USOCK_CPORT", pbuf, 0);
	setenv("LNET_ACCEPT_PORT", pbuf, 0);

	if (setenv("USOCK_PORTPID", "0", 1) == -1)
		err(1, "setenv");

	if (getenv("LNET_NETWORKS")) {
		psclog_info("using LNET_NETWORKS (%s) from "
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
				*p++ = '\0';

				net = libcfs_str2net(p);

				/* get numerical addresses */
				memset(&hints, 0, sizeof(hints));
				hints.ai_family = AF_INET;
				hints.ai_socktype = SOCK_STREAM;
				error = getaddrinfo(addrbuf, NULL,
				    &hints, &res0);
				if (error)
					psc_fatalx("%s: %s", addrbuf,
					    gai_strerror(error));

				for (res = res0; res; res = res->ai_next)
					slcfg_add_lnet(&lnets_hd, ifa,
					    res->ai_addr, net, &lent);
				freeaddrinfo(res0);

				DYNARRAY_FOREACH(nidp, l, &m->resm_nids) {
					memset(&sin, 0, sizeof(sin));
#ifdef HAVE_SALEN
					sin.sin_len = sizeof(sin);
#endif
					sin.sin_family = AF_INET;
					sin.sin_addr.s_addr =
					    LNET_NIDADDR(*nidp);
					slcfg_add_lnet(&lnets_hd, ifa,
					    &sin, LNET_NIDNET(*nidp),
					    &lent);
				}
			}
	PLL_ULOCK(&globalConfig.gconf_sites);

	PSCFREE(lent);
	pflnet_freeifaddrs(ifa);

	lnetstr[0] = '\0';
	psclist_for_each_entry_safe(lent, lnext, &lnets_hd, lentry) {
		pscrpc_net2str(lent->net, netbuf);
		k = snprintf(ltmp, sizeof(ltmp), "%s%s(%s)",
		    lnetstr[0] == '\0' ? "" : ",", netbuf, lent->ifn);
		if (k >= (int)sizeof(ltmp)) {
			k = -1;
			errno = ENAMETOOLONG;
		}
		if (k == -1)
			psc_fatal("error formatting Lustre network");

		if (strlcat(lnetstr, ltmp,
		    sizeof(lnetstr)) >= sizeof(lnetstr))
			psc_fatalx("too many Lustre networks");

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
			psc_fatalx("no resource member found for this node");

		if (nodeResm->resm_res->res_fsroot[0] != '\0')
			strlcpy(globalConfig.gconf_fsroot,
			    nodeResm->resm_res->res_fsroot,
			    sizeof(globalConfig.gconf_fsroot));

		if (nodeResm->resm_res->res_jrnldev[0] != '\0')
			strlcpy(globalConfig.gconf_journal,
			    nodeResm->resm_res->res_jrnldev,
			    sizeof(globalConfig.gconf_journal));

		if (nodeResm->resm_type == SLREST_ARCHIVAL_FS) {
#ifndef HAVE_AIO
			psc_fatalx("asynchronous I/O not supported on "
			    "this platform");
#endif
			globalConfig.gconf_async_io = 1;
		}

		psclog_info("node is a member of resource '%s'",
		    nodeResm->resm_res->res_name);
		libsl_profile_dump();
	} else {
		setenv("SLASH2_PIOS_ID", globalConfig.gconf_prefios, 0);
		setenv("SLASH_MDS_NID", globalConfig.gconf_prefmds, 0);
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
