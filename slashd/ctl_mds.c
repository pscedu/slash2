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
	"setsize",
	"setattr",
	"symlink",
	"unlink",
	"reclaim",
	"#aggr"
};

const char *slm_nslogst_fields[] = {
	"failures",
	"pending",
	"successes"
};

int
lookup(const char **tab, int n, const char *key)
{
	int j;

	for (j = 0; j < n; j++)
		if (strcasecmp(key, tab[j]) == 0)
			return (j);
	return (-1);
}

int
slmctlparam_namespace_stats_process(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels,
    struct sl_mds_nsstats *st, int d_val, int o_val, int s_val)
{
	int d_start, o_start, s_start, set, i_d, i_o, i_s;
	char nbuf[15];

	set = (mh->mh_type == PCMT_SETPARAM);

	d_start = d_val == -1 ? 0 : d_val;
	o_start = o_val == -1 ? 0 : o_val;
	s_start = s_val == -1 ? 0 : s_val;

	for (i_d = d_start; i_d < NS_NDIRS &&
	    (i_d == d_val || d_val == -1); i_d++) {
		levels[3] = (char *)slm_nslogst_acts[i_d];
		for (i_o = o_start; i_o < NS_NOPS + 1 &&
		    (i_o == o_val || o_val == -1); i_o++) {
			levels[4] = (char *)slm_nslogst_ops[i_o];
			for (i_s = s_start; i_s < NS_NSUMS &&
			    (i_s == s_val || s_val == -1); i_s++) {
				if (mh->mh_type == PCMT_SETPARAM)
					return (psc_ctlsenderr(fd, mh,
					    "field is read-only: %s",
					    slm_nslogst_fields[i_s]));

				levels[5] = (char *)
				    slm_nslogst_fields[i_s];
				snprintf(nbuf, sizeof(nbuf), "%d",
				    psc_atomic32_read(
				    &st->ns_stats[i_d][i_o][i_s]));
				if (!psc_ctlmsg_param_send(fd, mh, pcp,
				    PCTHRNAME_EVERYONE, levels, 6, nbuf))
					return (0);
			}
		}
	}
	return (1);
}

int
slmctlparam_namespace_stats(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels,
    __unusedx struct psc_ctlparam_node *pcn)
{
	char p_site[SITE_NAME_MAX], p_act[32], p_op[32], p_field[32];
	int site_found = 0, rc = 1, a_val, o_val, f_val;
	struct sl_resm *m;

	if (nlevels > 6)
		return (psc_ctlsenderr(fd, mh, "invalid field"));

	/* namespace.stats.<site|#AGGR>.<activity>.<op>.<field> */
	/* ex: namespace.stats.#aggr.replay.mkdir.successes */

	levels[0] = "namespace";
	levels[1] = "stats";

	if (strlcpy(p_site, nlevels > 2 ? levels[2] : "*",
	    sizeof(p_site)) >= sizeof(p_site))
		return (psc_ctlsenderr(fd, mh, "invalid site"));
	if (strlcpy(p_act, nlevels > 3 ? levels[3] : "*",
	    sizeof(p_act)) >= sizeof(p_act))
		return (psc_ctlsenderr(fd, mh, "invalid activity"));
	if (strlcpy(p_op, nlevels > 4 ? levels[4] : "*",
	    sizeof(p_op)) >= sizeof(p_op))
		return (psc_ctlsenderr(fd, mh, "invalid op"));
	if (strlcpy(p_field, nlevels > 5 ? levels[5] : "*",
	    sizeof(p_field)) >= sizeof(p_field))
		return (psc_ctlsenderr(fd, mh, "invalid site"));

	a_val = lookup(slm_nslogst_acts, nitems(slm_nslogst_acts), p_act);
	o_val = lookup(slm_nslogst_ops, nitems(slm_nslogst_ops), p_op);
	f_val = lookup(slm_nslogst_fields, nitems(slm_nslogst_fields), p_field);

	if (a_val == -1 && strcmp(p_act, "*"))
		return (psc_ctlsenderr(fd, mh,
		    "invalid namespace.stats activity: %s", p_act));
	if (o_val == -1 && strcmp(p_op, "*"))
		return (psc_ctlsenderr(fd, mh,
		    "invalid namespace.stats operation: %s", p_op));
	if (f_val == -1 && strcmp(p_field, "*"))
		return (psc_ctlsenderr(fd, mh,
		    "invalid namespace.stats field: %s", p_field));

	if (strcasecmp(p_site, "#aggr") == 0 ||
	    strcmp(p_site, "*") == 0) {
		levels[2] = "#aggr";
		rc = slmctlparam_namespace_stats_process(fd, mh, pcp,
		    levels, &slm_nsstats_aggr, a_val, o_val, f_val);
		if (!rc || strcmp(p_site, "#aggr") == 0)
			return (rc);
		site_found = 1;
	}

	SL_MDS_WALK(m,
		struct sl_mds_peerinfo *peerinfo;

		if (strcasecmp(p_site, m->resm_site->site_name) &&
		    strcmp(p_site, "*"))
			continue;
		site_found = 1;

		peerinfo = res2rpmi(m->resm_res)->rpmi_info;
		if (peerinfo == NULL)
			continue;

		levels[2] = m->resm_site->site_name;
		rc = slmctlparam_namespace_stats_process(fd, mh, pcp,
		    levels, &peerinfo->sp_stats, a_val, o_val, f_val);
		if (!rc || strcmp(p_site, m->resm_site->site_name) == 0)
			SL_MDS_WALK_SETLAST();
	);
	if (!site_found)
		return (psc_ctlsenderr(fd, mh, "invalid site: %s",
		    p_site));
	return (rc);
}

const char *slm_resmds_fields[] = {
	"xid"
};

const char *slm_resios_fields[] = {
	"disable_bia",
	"disable_gc",
	"xid"
};

int
slmctlparam_resources(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels,
    __unusedx struct psc_ctlparam_node *pcn)
{
	char p_site[SITE_NAME_MAX], p_res[RES_NAME_MAX], p_field[32];
	char *p, *str, nbuf[20], resname[RES_NAME_MAX];
	int site_found = 0, res_found = 0, field_found = 0;
	int set, i, f_val, val = 0;
	struct sl_mds_peerinfo *sp;
	struct sl_mds_iosinfo *si;
	struct sl_resource *r;
	struct sl_site *s;
	long l;

	if (nlevels > 4)
		return (psc_ctlsenderr(fd, mh, "invalid field"));

	set = mh->mh_type == PCMT_SETPARAM;

	if (set && nlevels != 4)
		return (psc_ctlsenderr(fd, mh, "invalid field"));

	/* resources.<site>.<res>.<field> */
	/* ex: namespace.stats.foo.myres.xid */
	/* ex: namespace.stats.foo.myres.disable_bia */

	levels[0] = "resources";

	if (strlcpy(p_site, nlevels > 1 ? levels[1] : "*",
	    sizeof(p_site)) >= sizeof(p_site))
		return (psc_ctlsenderr(fd, mh, "invalid site"));
	if (strlcpy(p_res, nlevels > 2 ? levels[2] : "*",
	    sizeof(p_res)) >= sizeof(p_res))
		return (psc_ctlsenderr(fd, mh, "invalid resource"));
	if (strlcpy(p_field, nlevels > 3 ? levels[3] : "*",
	    sizeof(p_field)) >= sizeof(p_field))
		return (psc_ctlsenderr(fd, mh, "invalid field"));

	if (set) {
		str = NULL;
		l = strtol(pcp->pcp_value, &str, 10);
		if (l == LONG_MAX || l == LONG_MIN || *str != '\0' ||
		    str == pcp->pcp_value || l > 1 || l < 0)
			return (psc_ctlsenderr(fd, mh,
			    "invalid resources value: %s",
			    pcp->pcp_field));
		val = (int)l;
	}

	CONF_FOREACH_RES(s, r, i) {
		strlcpy(resname, r->res_name, sizeof(resname));
		p = strchr(resname, '@');
		if (p)
			*p = '\0';

		if (strcmp(p_site, "*") && strcmp(p_site, s->site_name))
			continue;
		site_found = 1;
		if (strcmp(p_res, "*") && strcmp(p_res, resname))
			continue;
		res_found = 1;

		if (RES_ISCLUSTER(r)) {
			if (strcmp(p_res, "*"))
				return (psc_ctlsenderr(fd, mh,
				    "invalid resource"));
			continue;
		}

		levels[1] = s->site_name;
		levels[2] = resname;

		if (r->res_type == SLREST_MDS) {
			sp = res2mdsinfo(r);
			f_val = lookup(slm_resmds_fields,
			    nitems(slm_resmds_fields), p_field);
			if (f_val == -1 && strcmp(p_field, "*"))
				return (psc_ctlsenderr(fd, mh,
				    "invalid resources field: %s", p_field));
			if (strcmp(p_field, "*") == 0 ||
			    strcmp(p_field, "xid") == 0) {
				if (set)
					return (psc_ctlsenderr(fd, mh,
					    "xid: field is read-only"));
				levels[3] = "xid";
				snprintf(nbuf, sizeof(nbuf), "%"PRIu64,
				    sp->sp_xid);
				if (!psc_ctlmsg_param_send(fd, mh, pcp,
				    PCTHRNAME_EVERYONE, levels, 4, nbuf))
					return (0);
				field_found = 1;
			}
		} else {
			si = res2iosinfo(r);
			f_val = lookup(slm_resios_fields,
			    nitems(slm_resios_fields), p_field);
			if (f_val == -1 && strcmp(p_field, "*"))
				return (psc_ctlsenderr(fd, mh,
				    "invalid resources field: %s", p_field));
			if (strcmp(p_field, "*") == 0 ||
			    strcmp(p_field, "xid") == 0) {
				if (set)
					return (psc_ctlsenderr(fd, mh,
					    "xid: field is read-only"));
				levels[3] = "xid";
				snprintf(nbuf, sizeof(nbuf), "%"PRIu64,
				    si->si_xid);
				if (!psc_ctlmsg_param_send(fd, mh, pcp,
				    PCTHRNAME_EVERYONE, levels, 4, nbuf))
					return (0);
				field_found = 1;
			}
			if (strcmp(p_field, "*") == 0 ||
			    strcmp(p_field, "disable_bia") == 0) {
				if (set) {
					if (val)
						si->si_flags |= SIF_DISABLE_BIA;
					else
						si->si_flags &= ~SIF_DISABLE_BIA;
				} else {
					levels[3] = "disable_bia";
					snprintf(nbuf, sizeof(nbuf),
					    "%d", si->si_flags &
					    SIF_DISABLE_BIA ? 1 : 0);
					if (!psc_ctlmsg_param_send(fd,
					    mh, pcp, PCTHRNAME_EVERYONE,
					    levels, 4, nbuf))
						return (0);
				}
				field_found = 1;
			}
			if (strcmp(p_field, "*") == 0 ||
			    strcmp(p_field, "disable_gc") == 0) {
				if (set) {
					if (val)
						si->si_flags |= SIF_DISABLE_GC;
					else
						si->si_flags &= ~SIF_DISABLE_GC;
				} else {
					levels[3] = "disable_gc";
					snprintf(nbuf, sizeof(nbuf),
					    "%d", si->si_flags &
					    SIF_DISABLE_GC ? 1 : 0);
					if (!psc_ctlmsg_param_send(fd,
					    mh, pcp, PCTHRNAME_EVERYONE,
					    levels, 4, nbuf))
						return (0);
				}
				field_found = 1;
			}
		}
	}
	if (!site_found)
		return (psc_ctlsenderr(fd, mh, "invalid site: %s",
		    p_site));
	if (!res_found)
		return (psc_ctlsenderr(fd, mh, "invalid resource: %s",
		    p_res));
	if (!field_found)
		return (psc_ctlsenderr(fd, mh, "invalid field: %s",
		    p_field));
	return (1);
}

/**
 * slmctlcmd_stop - Handle a STOP command to terminate execution.
 */
__dead int
slmctlcmd_stop(__unusedx int fd, __unusedx struct psc_ctlmsghdr *mh,
    __unusedx void *m)
{
	mdsio_exit();
	/* XXX journal_close */
	exit(0);
}

__static int
slmctlrep_replpair_send(int fd, struct psc_ctlmsghdr *mh,
    struct slmctlmsg_replpair *scrp, struct sl_resm *m0,
    struct sl_resm *m1)
{
	struct resm_mds_info *rmmi0, *rmmi1;
	struct slm_resmlink *srl;

	rmmi0 = resm2rmmi(m0);
	rmmi1 = resm2rmmi(m1);
	srl = repl_busytable + MDS_REPL_BUSYNODES(
	    MIN(rmmi0->rmmi_busyid, rmmi1->rmmi_busyid),
	    MAX(rmmi0->rmmi_busyid, rmmi1->rmmi_busyid));

	memset(scrp, 0, sizeof(*scrp));
	strlcpy(scrp->scrp_addrbuf[0], m0->resm_res->res_name,
	    sizeof(scrp->scrp_addrbuf[0]));
	strlcpy(scrp->scrp_addrbuf[1], m1->resm_res->res_name,
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

	CONF_LOCK();
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
	CONF_ULOCK();
	return (rc);
}

/**
 * slmctlrep_getstatfs - Send a response to a "GETSTATFS" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @m: control message to examine and reuse.
 */
int
slmctlrep_getstatfs(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slmctlmsg_statfs *scsf = m;
	struct sl_resource *r;
	int i, rc = 1;

	CONF_LOCK();
	SITE_FOREACH_RES(nodeSite, r, i) {
		if (!RES_ISFS(r))
			continue;

		strlcpy(scsf->scsf_resname, r->res_name,
		    sizeof(scsf->scsf_resname));
		memcpy(&scsf->scsf_ssfb, &res2iosinfo(r)->si_ssfb,
		    sizeof(scsf->scsf_ssfb));

		rc = psc_ctlmsg_sendv(fd, mh, scsf);
		if (!rc)
			goto done;
	}
 done:
	CONF_ULOCK();
	return (rc);
}

void
slmctlparam_nextfid_get(char *val)
{
	spinlock(&slm_fid_lock);
	snprintf(val, PCP_VALUE_MAX, SLPRI_FID, slm_next_fid);
	freelock(&slm_fid_lock);
}

int
slmctlparam_nextfid_set(const char *val)
{
	unsigned long l;
	char *endp;
	int rc = 0;

	l = strtol(val, &endp, 0);
	spinlock(&slm_fid_lock);
	if (endp == val || *endp)
		rc = -1;
	else {
		if (FID_GET_SITEID(l) == FID_GET_SITEID(slm_next_fid))
			FID_SET_SITEID(l, 0);
		if (l > FID_MAX_INUM || l <= FID_GET_INUM(slm_next_fid))
			rc = -1;
		else
			slm_next_fid = l | ((uint64_t)nodeSite->site_id <<
			    SLASH_FID_SITE_SHFT);
	}
	freelock(&slm_fid_lock);
	return (rc);
}

struct psc_ctlop slmctlops[] = {
	PSC_CTLDEFOPS,
	{ slctlrep_getconns,		sizeof(struct slctlmsg_conn ) },
	{ slctlrep_getfcmhs,		sizeof(struct slctlmsg_fcmh ) },
	{ slmctlrep_getreplpairs,	sizeof(struct slmctlmsg_replpair ) },
	{ slmctlrep_getstatfs,		sizeof(struct slmctlmsg_statfs ) },
	{ slmctlcmd_stop,		0 }
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
/* LNETAC	*/ NULL,
/* NBRQ		*/ NULL,
/* RCM		*/ NULL,
/* RESMON	*/ NULL,
/* RMC		*/ NULL,
/* RMI		*/ NULL,
/* RMM		*/ NULL,
/* TIOS		*/ NULL,
/* UPSCHED	*/ NULL,
/* USKLNDPL	*/ NULL,
/* WORKER	*/ NULL,
/* ZFS_KSTAT	*/ NULL
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
	psc_ctlparam_register("run", psc_ctlparam_run);

	psc_ctlparam_register("resources", slmctlparam_resources);
	psc_ctlparam_register("namespace.stats", slmctlparam_namespace_stats);
	psc_ctlparam_register_simple("nextfid", slmctlparam_nextfid_get,
	    slmctlparam_nextfid_set);

	psc_ctlthr_main(fn, slmctlops, nitems(slmctlops), SLMTHRT_CTLAC);
}
