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

/*
 * Interface for controlling live operation of slashd.
 */

#include "pfl/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "creds.h"
#include "ctl.h"
#include "ctl_mds.h"
#include "ctlsvr.h"
#include "mdsio.h"
#include "repl_mds.h"
#include "slashd.h"
#include "slconfig.h"

struct psc_lockedlist		 psc_mlists;

struct sl_mds_nsstats		 slm_nsstats_aggr;	/* aggregate stats */

const char *slm_nslogst_acts[] = {
	"propagate",
	"replay"
};

const char *slm_nslogst_ops[] = {
	"create",
	"link",
	"mkdir",
	"rename",
	"rmdir",
	"symlink",
	"unlink",
	"#aggr"
};

const char *slm_nslogst_fields[] = {
	"failures",
	"pending",
	"successes"
};

int
lookup(const char **p, int n, const char *key)
{
	int j;

	for (j = 0; j < n; j++)
		if (strcasecmp(key, p[j]) == 0)
			return (j);
	return (-1);
}

int
slmctlparam_namespace_stats_process(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, struct sl_mds_nsstats *st,
    int d_val, int o_val, int s_val, int val)
{
	int d_start, o_start, s_start, rc, set, i_d, i_o, i_s;
	char nbuf[15];

	set = (mh->mh_type == PCMT_SETPARAM);

	d_start = d_val == -1 ? 0 : d_val;
	o_start = o_val == -1 ? 0 : o_val;
	s_start = s_val == -1 ? 0 : s_val;

	rc = 1;
	for (i_d = d_start; i_d < NS_NDIRS &&
	    (i_d == d_val || d_val == -1); i_d++) {
		levels[3] = (char *)slm_nslogst_acts[i_d];
		for (i_o = o_start; i_o < NS_NOPS + 1 &&
		    (i_o == o_val || d_val == -1); i_o++) {
			levels[4] = (char *)slm_nslogst_ops[i_o];
			for (i_s = s_start; i_s < NS_NSUMS &&
			    (i_s == s_val || s_val == -1); i_s++) {
				if (set)
					psc_atomic32_set(
					    &st->ns_stats[i_d][i_o][i_s],
					    val);
				else {
					levels[5] = (char *)
					    slm_nslogst_fields[i_s];
					snprintf(nbuf, sizeof(nbuf), "%d",
					    psc_atomic32_read(
					    &st->ns_stats[i_d][i_o][i_s]));
					rc = psc_ctlmsg_param_send(fd, mh, pcp,
					    PCTHRNAME_EVERYONE, levels, 6, nbuf);
					if (!rc)
						goto out;
				}
			}
		}
	}
 out:
	return (rc);
}

int
slmctlparam_namespace_stats(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels,
    __unusedx struct psc_ctlparam_node *pcn)
{
	const char *p_site, *p_act, *p_op, *p_field;
	int rc, d_val, o_val, s_val, val;
	struct sl_resm *r;
	char *str;
	long l;

	if (nlevels > 6)
		return (psc_ctlsenderr(fd, mh, "invalid field"));

	/* namespace.stats.<site|#AGGR>.<activity>.<op>.<field> */
	/* namespace.stats.#aggr.replay.mkdir.successes */

	levels[0] = "namespace";
	levels[1] = "stats";

	p_site	= nlevels > 2 ? levels[2] : "*";
	p_act	= nlevels > 3 ? levels[3] : "*";
	p_op	= nlevels > 4 ? levels[4] : "*";
	p_field	= nlevels > 5 ? levels[5] : "*";

	d_val = lookup(slm_nslogst_acts, nitems(slm_nslogst_acts), p_act);
	o_val = lookup(slm_nslogst_ops, nitems(slm_nslogst_ops), p_op);
	s_val = lookup(slm_nslogst_fields, nitems(slm_nslogst_fields), p_field);

	if (d_val == -1 && strcmp(p_act, "*"))
		return (psc_ctlsenderr(fd, mh,
		    "invalid namespace.stats activity: %s", p_act));
	if (o_val == -1 && strcmp(p_op, "*"))
		return (psc_ctlsenderr(fd, mh,
		    "invalid namespace.stats operation: %s", p_op));
	if (s_val == -1 && strcmp(p_act, "*"))
		return (psc_ctlsenderr(fd, mh,
		    "invalid namespace.stats field: %s", p_field));

	val = 0; /* gcc */

	if (mh->mh_type == PCMT_SETPARAM) {
		if (nlevels != 6)
			return (psc_ctlsenderr(fd, mh, "invalid field"));

		str = NULL;
		l = strtol(pcp->pcp_value, &str, 10);
		if (l == LONG_MAX || l == LONG_MIN || *str != '\0' ||
		    str == pcp->pcp_value || l > 1 || l < 0)
			return (psc_ctlsenderr(fd, mh,
			    "invalid namespace.stats value: %s",
			    pcp->pcp_field));
		val = (int)l;
	}

	if (strcasecmp(p_site, "#aggr") == 0 ||
	    strcmp(p_site, "*") == 0) {
		levels[2] = "#aggr";
		rc = slmctlparam_namespace_stats_process(fd, mh, pcp,
		    levels, &slm_nsstats_aggr, d_val, o_val, s_val, val);
		if (!rc || strcmp(p_site, "#aggr"))
			return (rc);
	}

	rc = 1;
	SL_MDS_WALK(r,
		struct sl_mds_peerinfo *peerinfo;

		peerinfo = res2rpmi(r->resm_res)->rpmi_info;
		if (peerinfo == NULL)
			continue;
		if (strcasecmp(p_site, r->resm_site->site_name) &&
		    strcmp(p_site, "*"))
			continue;

		levels[2] = r->resm_site->site_name;
		rc = slmctlparam_namespace_stats_process(fd, mh, pcp,
		    levels, &peerinfo->sp_stats, d_val, o_val, s_val,
		    val);
		if (!rc || strcmp(p_site, r->resm_site->site_name) == 0)
			SL_MDS_WALK_SETLAST();
	);
	return (rc);
}

/**
 * slmctlcmd_exit - Handle an EXIT command to terminate execution.
 */
__dead int
slmctlcmd_exit(__unusedx int fd, __unusedx struct psc_ctlmsghdr *mh,
    __unusedx void *m)
{
	mdsio_exit();
	exit(0);
}

__static int
slmctlrep_replpair_send(int fd, struct psc_ctlmsghdr *mh,
    struct slmctlmsg_replpair *scrp, struct sl_resm *m0,
    struct sl_resm *m1)
{
	struct resm_mds_info *rmmi0, *rmmi1;
	struct slm_resmlink *srl;

	rmmi0 = m0->resm_pri;
	rmmi1 = m1->resm_pri;
	srl = repl_busytable + MDS_REPL_BUSYNODES(
	    MIN(rmmi0->rmmi_busyid, rmmi1->rmmi_busyid),
	    MAX(rmmi0->rmmi_busyid, rmmi1->rmmi_busyid));

	memset(scrp, 0, sizeof(*scrp));
	strlcpy(scrp->scrp_addrbuf[0], m0->resm_addrbuf,
	    sizeof(scrp->scrp_addrbuf[0]));
	strlcpy(scrp->scrp_addrbuf[1], m1->resm_addrbuf,
	    sizeof(scrp->scrp_addrbuf[1]));
	scrp->scrp_avail = srl->srl_avail;
	scrp->scrp_used = srl->srl_used;
	return (psc_ctlmsg_sendv(fd, mh, scrp));
}

/**
 * slmctlrep_getreplpairs - Send a response to a "GETREPLPAIRS" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @m: control message to examine and reuse.
 */
int
slmctlrep_getreplpairs(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slmctlmsg_replpair *scrp = m;
	struct sl_resm *resm, *resm0;
	struct sl_resource *r, *r0;
	struct sl_site *s, *s0;
	int i, j, i0, j0, rc = 1;

	PLL_LOCK(&globalConfig.gconf_sites);
	spinlock(&repl_busytable_lock);
	CONF_FOREACH_RESM(s, r, i, resm, j) {
		j0 = j + 1;
		RES_FOREACH_MEMB_CONT(r, resm0, j0) {
			rc = slmctlrep_replpair_send(fd, mh, scrp, resm,
			    resm0);
			if (!rc)
				goto done;
		}
		i0 = i + 1;
		SITE_FOREACH_RES_CONT(s, r0, i0)
			RES_FOREACH_MEMB(r0, resm0, j0) {
				rc = slmctlrep_replpair_send(fd, mh,
				    scrp, resm, resm0);
				if (!rc)
					goto done;
			}
		s0 = pll_next_item(&globalConfig.gconf_sites, s);
		CONF_FOREACH_SITE_CONT(s0)
			SITE_FOREACH_RES(s0, r0, i0)
				RES_FOREACH_MEMB(r0, resm0, j0) {
					rc = slmctlrep_replpair_send(fd,
					    mh, scrp, resm, resm0);
					if (!rc)
						goto done;
				}
	}
 done:
	freelock(&repl_busytable_lock);
	PLL_ULOCK(&globalConfig.gconf_sites);
	return (rc);
}

struct psc_ctlop slmctlops[] = {
	PSC_CTLDEFOPS,
	{ slctlrep_getconns,		sizeof(struct slctlmsg_conn ) },
	{ slctlrep_getfcmhs,		sizeof(struct slctlmsg_fcmh ) },
	{ slmctlrep_getreplpairs,	sizeof(struct slmctlmsg_replpair ) }
};

psc_ctl_thrget_t psc_ctl_thrgets[] = {
/* BMAPTIMEO	*/ NULL,
/* COH		*/ NULL,
/* CTL		*/ psc_ctlthr_get,
/* CTLAC	*/ psc_ctlacthr_get,
/* CURSOR	*/ NULL,
/* JNAMESPACE	*/ NULL,
/* JRECLAIM	*/ NULL,
/* JRNL		*/ NULL,
/* ZFS_KSTAT	*/ NULL,
/* LNETAC	*/ NULL,
/* NBRQ		*/ NULL,
/* RCM		*/ NULL,
/* RMC		*/ NULL,
/* RMI		*/ NULL,
/* RMM		*/ NULL,
/* TIOS		*/ NULL,
/* UPSCHED	*/ NULL,
/* USKLNDPL	*/ NULL,
/* WORKER	*/ NULL
};

PFLCTL_SVR_DEFS;

void
slmctlthr_main(const char *fn)
{
//	psc_ctlparam_register("faults", psc_ctlparam_faults);
	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("pause", psc_ctlparam_pause);
	psc_ctlparam_register("pool", psc_ctlparam_pool);
	psc_ctlparam_register("rlim.nofile", psc_ctlparam_rlim_nofile);
	psc_ctlparam_register("namespace.stats", slmctlparam_namespace_stats);
	psc_ctlparam_register("run", psc_ctlparam_run);

	psc_ctlthr_main(fn, slmctlops, nitems(slmctlops), SLMTHRT_CTLAC);
}
