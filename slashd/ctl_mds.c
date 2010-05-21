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

/*
 * Interface for controlling live operation of a slashd instance.
 */

#include "pfl/cdefs.h"
#include "psc_util/ctl.h"
#include "psc_util/ctlsvr.h"

#include "creds.h"
#include "ctl_mds.h"
#include "mdsio.h"
#include "slashd.h"
#include "slconfig.h"

struct psc_lockedlist psc_mlists;

struct sl_mds_nsstats	 sl_mds_nsstats_aggr;	/* aggregate stats */

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
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels)
{
	const char *p_site, *p_act, *p_op, *p_field;
	int i_r, rc, d_val, o_val, s_val, val;
	struct sl_resource *r;
	struct sl_site *s;
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
		    levels, &sl_mds_nsstats_aggr, d_val, o_val, s_val, val);
		if (!rc || strcmp(p_site, "#aggr"))
			return (rc);
	}

	rc = 1;
	CONF_LOCK();
	CONF_FOREACH_SITE(s)
		SITE_FOREACH_RES(s, r, i_r)
			if (r->res_type == SLREST_MDS &&
			    res2rpmi(r)->rpmi_peerinfo &&
			    (strcmp(p_site, "*") == 0 ||
			     strcasecmp(p_site, s->site_name)) == 0) {
				levels[2] = s->site_name;
				rc = slmctlparam_namespace_stats_process(
				    fd, mh, pcp, levels,
				    &res2rpmi(r)->rpmi_peerinfo->sp_stats,
				    d_val, o_val, s_val, val);
				if (!rc || strcmp(p_site, s->site_name) == 0)
					goto out;

				break;	/* goto next site */
			}
 out:
	CONF_UNLOCK();
	return (rc);
}

/*
 * slmctlcmd_exit - handle an EXIT command to terminate execution.
 */
__dead int
slmctlcmd_exit(__unusedx int fd, __unusedx struct psc_ctlmsghdr *mh,
    __unusedx void *m)
{
	mdsio_exit();
	exit(0);
}

struct psc_ctlop slmctlops[] = {
	PSC_CTLDEFOPS
};

void (*psc_ctl_getstats[])(struct psc_thread *, struct psc_ctlmsg_stats *) = {
/* 0 */	psc_ctlthr_stat
};
int psc_ctl_ngetstats = nitems(psc_ctl_getstats);

int (*psc_ctl_cmds[])(int, struct psc_ctlmsghdr *, void *) = {
	slmctlcmd_exit,
};
int psc_ctl_ncmds = nitems(psc_ctl_cmds);

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

	psc_ctlthr_main(fn, slmctlops, nitems(slmctlops));
}
