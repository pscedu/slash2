/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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
#include <arpa/inet.h>

#include <linux/netlink.h>
#include <linux/rtnetlink.h>

#include <err.h>
#include <netdb.h>

#include "psc_ds/dynarray.h"
#include "psc_ds/hash2.h"
#include "psc_ds/list.h"
#include "psc_util/log.h"
#include "psc_util/strlcat.h"
#include "psc_util/strlcpy.h"

#include "slconfig.h"
#include "slerr.h"

struct psc_dynarray	lnet_nids = DYNARRAY_INIT;
struct ifconf		sl_ifconf;

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
	lnet_nid_t *np;
	int i;

	DYNARRAY_FOREACH(np, i, &lnet_nids) {
		if (LNET_NETTYP(LNET_NIDNET(*np)) == LOLND)
			continue;

		resm = psc_hashtbl_search(&globalConfig.gconf_nid_hashtbl,
		    NULL, NULL, np);
		/* Every nid found by lnet must be a resource member. */
		if (resm == NULL)
			psc_fatalx("nid %s is not a member of any resource",
			    psc_nid2str(*np, nidbuf));

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
libsl_try_nid2resm(lnet_nid_t nid)
{
	return (psc_hashtbl_search(&globalConfig.gconf_nid_hashtbl,
	    NULL, NULL, &nid));
}

struct sl_resm *
libsl_nid2resm(lnet_nid_t nid)
{
	char nidbuf[PSC_NIDSTR_SIZE];
	struct sl_resm *resm;

	resm = libsl_try_nid2resm(nid);
	if (resm)
		return (resm);
	psc_fatalx("IOS %s not found in SLASH configuration, "
	    "verify uniformity across all servers.",
	    psc_nid2str(nid, nidbuf));
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
	    "Node info: resource %s ID %u\n"
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

__static void
slcfg_getifname(int ifidx, char ifn[IFNAMSIZ])
{
	struct ifreq ifr;
	int rc, s;

	ifr.ifr_ifindex = ifidx;
	s = socket(AF_INET, SOCK_DGRAM, 0);
	if (s == -1)
		psc_fatal("socket");

	rc = ioctl(s, SIOCGIFNAME, &ifr);
	if (rc == -1)
		psc_fatal("ioctl SIOCGIFNAME");
	close(s);
	strlcpy(ifn, ifr.ifr_name, IFNAMSIZ);
}

__static void
slcfg_getifaddrs(void)
{
	int rc, s;

	s = socket(AF_INET, SOCK_DGRAM, 0);
	if (s == -1)
		psc_fatal("socket");

	sl_ifconf.ifc_buf = NULL;
	rc = ioctl(s, SIOCGIFCONF, &sl_ifconf);
	if (rc == -1)
		psc_fatal("ioctl SIOCGIFCONF");

	/*
	 * If an interface is being added during this run,
	 * there is no way to determine that we didn't get
	 * them all with this approach.
	 */
	sl_ifconf.ifc_buf = PSCALLOC(sl_ifconf.ifc_len);
	rc = ioctl(s, SIOCGIFCONF, &sl_ifconf);
	if (rc == -1)
		psc_fatal("ioctl SIOCGIFCONF");

	close(s);
}

__static void
slcfg_getif(struct sockaddr *sa, char ifn[IFNAMSIZ])
{
	struct {
		struct nlmsghdr	nmh;
		struct rtmsg	rtm;
#define RT_SPACE 8192
		char		buf[RT_SPACE];
	} rq;
	struct sockaddr_in *sin;
	struct rtattr *rta;
	struct ifreq *ifr;
	int n, s, ifidx;
	ssize_t rc;

	psc_assert(sa->sa_family == AF_INET);
	sin = (void *)sa;

	/*
	 * Scan interfaces for addr since netlink
	 * will always give us the lo interface.
	 */
	ifr = (void *)sl_ifconf.ifc_buf;
	for (n = 0; n < sl_ifconf.ifc_len; n += sizeof(*ifr), ifr++) {
		if (ifr->ifr_addr.sa_family == sa->sa_family &&
		    memcmp(&sin->sin_addr,
		    &((struct sockaddr_in *)&ifr->ifr_addr)->sin_addr,
		    sizeof(sin->sin_addr)) == 0) {
			strlcpy(ifn, ifr->ifr_name, IFNAMSIZ);
			return;
		}
	}

	/* use netlink to find the destination interface */
	s = socket(PF_NETLINK, SOCK_RAW, NETLINK_ROUTE);
	if (s == -1)
		psc_fatal("socket");

	memset(&rq, 0, sizeof(rq));
	rq.nmh.nlmsg_len = NLMSG_SPACE(sizeof(rq.rtm)) +
	    RTA_LENGTH(sizeof(sin->sin_addr));
	rq.nmh.nlmsg_flags = NLM_F_REQUEST;
	rq.nmh.nlmsg_type = RTM_GETROUTE;

	rq.rtm.rtm_family = sa->sa_family;
	rq.rtm.rtm_protocol = RTPROT_UNSPEC;
	rq.rtm.rtm_table = RT_TABLE_MAIN;
	/* # bits filled in target addr */
	rq.rtm.rtm_dst_len = sizeof(sin->sin_addr) * NBBY;
	rq.rtm.rtm_scope = RT_SCOPE_LINK;

	rta = (void *)((char *)&rq + NLMSG_SPACE(sizeof(rq.rtm)));
	rta->rta_type = RTA_DST;
	rta->rta_len = RTA_LENGTH(sizeof(sin->sin_addr));
	memcpy(RTA_DATA(rta), &sin->sin_addr,
	    sizeof(sin->sin_addr));

	errno = 0;
	rc = write(s, &rq, rq.nmh.nlmsg_len);
	if (rc != (ssize_t)rq.nmh.nlmsg_len)
		psc_fatal("routing socket length mismatch");

	rc = read(s, &rq, sizeof(rq));
	if (rc == -1)
		psc_fatal("routing socket read");
	close(s);

	if (rq.nmh.nlmsg_type == NLMSG_ERROR) {
		struct nlmsgerr *nlerr;

		nlerr = NLMSG_DATA(&rq.nmh);
		psc_fatalx("netlink: %s",
		    slstrerror(nlerr->error));
	}

	rc -= NLMSG_SPACE(sizeof(rq.rtm));
	while (rc > 0) {
		if (rta->rta_type == RTA_OIF &&
		    RTA_PAYLOAD(rta) == sizeof(ifidx)) {
			memcpy(&ifidx, RTA_DATA(rta),
			    sizeof(ifidx));
			slcfg_getifname(ifidx, ifn);
			return;
		}
		rta = RTA_NEXT(rta, rc);
	}
	psc_fatalx("no route for addr");
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

void
libsl_init(int pscnet_mode, int ismds)
{
	struct {
		char			*net;
		char			 ifn[IFNAMSIZ];
		struct psclist_head	 lentry;
	} *lent, *lnext;
	char *p, pbuf[6], lnetstr[256], addrbuf[HOST_NAME_MAX];
	struct addrinfo hints, *res, *res0;
	int netcmp, error, rc, j, k;
	PSCLIST_HEAD(lnets_hd);
	struct sl_resource *r;
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

	slcfg_getifaddrs();

	lent = PSCALLOC(sizeof(*lent));

	PLL_LOCK(&globalConfig.gconf_sites);
	PLL_FOREACH(s, &globalConfig.gconf_sites)
		DYNARRAY_FOREACH(r, j, &s->site_resources)
			DYNARRAY_FOREACH(m, k, &r->res_members) {
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
					psc_fatalx("%s", gai_strerror(error));

				for (res = res0; res; res = res->ai_next) {
					/* get destination routing interface */
					slcfg_getif(res->ai_addr, lent->ifn);
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
						psclist_xadd(&lent->lentry, &lnets_hd);
						lent = PSCALLOC(sizeof(*lent));
					}
				}
				freeaddrinfo(res0);
			}
	PLL_ULOCK(&globalConfig.gconf_sites);

	PSCFREE(lent);

	lnetstr[0] = '\0';
	psclist_for_each_entry_safe(lent, lnext, &lnets_hd, lentry) {
		if (lnetstr[0] != '\0')
			if (psc_strlcat(lnetstr, ",",
			    sizeof(lnetstr)) >= sizeof(lnetstr))
				psc_fatalx("too many lustre networks");

		if (psc_strlcat(lnetstr, lent->net,
		    sizeof(lnetstr)) >= sizeof(lnetstr))
			psc_fatalx("too many lustre networks");
		if (psc_strlcat(lnetstr, "(",
		    sizeof(lnetstr)) >= sizeof(lnetstr))
			psc_fatalx("too many lustre networks");
		if (psc_strlcat(lnetstr, lent->ifn,
		    sizeof(lnetstr)) >= sizeof(lnetstr))
			psc_fatalx("too many lustre networks");
		if (psc_strlcat(lnetstr, ")",
		    sizeof(lnetstr)) >= sizeof(lnetstr))
			psc_fatalx("too many lustre networks");

		psclist_del(&lent->lentry);
		PSCFREE(lent);
	}

	setenv("LNET_NETWORKS", lnetstr, 0);

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
