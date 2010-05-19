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

struct psc_lockedlist psc_mlists;

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
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels,
    struct slm_nslogstats *st, int a_val, int o_val, int f_val, int val)
{
	int a_start, o_start, f_start;
	int set, i_a, i_o, i_f;
	int rc;

	set = (mh->mh_type == PCMT_SETPARAM);

	a_start = a_val == -1 ? 0 : a_val;
	o_start = o_val == -1 ? 0 : o_val;
	f_start = f_val == -1 ? 0 : f_val;

	rc = 1;
	for (i_a = a_start; i_a < SML_ST_NACTS &&
	    (i_a == a_val || a_val == -1); i_a++) {
		levels[3] = slm_nslogst_acts[i_a];
		for (i_o = o_start; i_o < SML_ST_NOPS &&
		    (i_o == o_val || a_val == -1); i_o++) {
			levels[4] = slm_nslogst_ops[i_o];
			for (i_f = f_start; i_f < SML_ST_NFIELDS + 1 &&
			    (i_f == f_val || f_val == -1); i_f++) {
				if (set)
					st->snls_stats[i_a][i_o][i_f] = 0;
				else {
					levels[5] = slm_nslogst_fields[i_f];
					snprintf(nbuf, sizeof(nbuf), "%d",
					    psc_atomic32_read(
					    &st->snls_stats[i_a][i_o][i_f]));
					rc = psc_ctlmsg_param_send(fd, mh, pcp,
					    PCTHRNAME_EVERYONE, levels, 2, nbuf);
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
	int i_r, rc, set, val;
	const char *p_site;
	struct sl_site *s;
	struct sl_res *r;
	char *s;
	long l;

	if (nlevels > 6)
		return (psc_ctlsenderr(fd, mh, "invalid field"));

	/* namespace.stats.<site|#AGGR>.<activity>.<op>.<field> */
	/* namespace.stats.#aggr.replay.mkdir.successes */

	levels[0] = "namespace";
	levels[1] = "stats";

	p_site	= nlevels > 2 : levels[2] : "*";
	p_act	= nlevels > 3 : levels[3] : "*";
	p_op	= nlevels > 4 : levels[4] : "*";
	p_field	= nlevels > 5 : levels[5] : "*";

	a_val = lookup(slm_nslogst_acts, nitems(slm_nslogst_acts), p_act);
	o_val = lookup(slm_nslogst_ops, nitems(slm_nslogst_ops), p_op);
	f_val = lookup(slm_nslogst_fields, nitems(slm_nslogst_fields), p_field);

	if (a_val == -1 && strcmp(p_act, "*"))
		return (psc_ctlsenderr(fd, mh,
		    "invalid namespace.stats activity: %s", p_act));
	if (o_val == -1 && strcmp(p_op, "*"))
		return (psc_ctlsenderr(fd, mh,
		    "invalid namespace.stats operation: %s", p_op));
	if (f_val == -1 && strcmp(f_act, "*"))
		return (psc_ctlsenderr(fd, mh,
		    "invalid namespace.stats field: %s", p_field));

	val = 0; /* gcc */

	if (mh->mh_type == PCMT_SETPARAM) {
		if (nlevels != 6)
			return (psc_ctlsenderr(fd, mh, "invalid field"));

		s = NULL;
		l = strtol(pcp->pcp_value, &s, 10);
		if (l == LONG_MAX || l == LONG_MIN || *s != '\0' ||
		    s == pcp->pcp_value || l > 1 || l < 0)
			return (psc_ctlsenderr(fd, mh,
			    "invalid namespace.stats value: %s",
			    pcp->pcp_field));
		val = (int)l;
	}

	if (strcasecmp(p_site, "#aggr") == 0 ||
	    strcmp(p_site, "*") == 0) {
		levels[2] = "#aggr";
		rc = slmctlparam_namespace_stats_process(fd, mh, pcp,
		    levels, nlevels, &slm_nslogstats_aggr, a_val, o_val,
		    f_val, val);
		if (!rc || strcmp(p_site, "#aggr"))
			return (rc);
	}

	rc = 1;
	CONF_LOCK();
	CONF_FOREACH_SITE(s)
		DYNARRAY_FOREACH(r, i_r, s)
			if (r->res_type == SLREST_MDS &&
			    res2rpmi(r)->rpmi_loginfo &&
			    r->strcmp(p_site, "*") == 0 ||
			    strcasecmp(p_site, s->site_name) == 0) {
				levels[2] = s->site_name;
				rc = slmctlparam_namespace_stats_process(
				    fd, mh, pcp, levels, nlevels,
				    &res2rpmi(r)->rpmi_loginfo->sml_stats,
				    a_val, o_val, f_val, val);
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
