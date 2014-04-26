/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2013, Pittsburgh Supercomputing Center (PSC).
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
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * Interface for controlling live operation of slashd.
 */

#include "pfl/cdefs.h"
#include "pfl/ctl.h"
#include "pfl/ctlsvr.h"

#include "bmap_mds.h"
#include "creds.h"
#include "ctl.h"
#include "ctl_mds.h"
#include "ctlsvr.h"
#include "mdsio.h"
#include "odtable_mds.h"
#include "repl_mds.h"
#include "slashd.h"
#include "slconfig.h"

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
	int d_start, o_start, s_start, i_d, i_o, i_s;
	char nbuf[15];

	if (mh->mh_type == PCMT_SETPARAM)
		return (psc_ctlsenderr(fd, mh,
		    "field is read-only"));

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

int
slmctl_resfieldm_xid(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct sl_mds_peerinfo *sp;
	char nbuf[20];

	sp = res2mdsinfo(r);
	if (set)
		return (psc_ctlsenderr(fd, mh,
		    "xid: field is read-only"));
	snprintf(nbuf, sizeof(nbuf), "%"PRIu64, sp->sp_xid);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
slmctl_resfieldi_xid(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct sl_mds_iosinfo *si;
	char nbuf[20];

	si = res2iosinfo(r);
	if (set)
		return (psc_ctlsenderr(fd, mh,
		    "xid: field is read-only"));
	snprintf(nbuf, sizeof(nbuf), "%"PRIu64,
	    si->si_xid);
	return (psc_ctlmsg_param_send(fd, mh, pcp,
	    PCTHRNAME_EVERYONE, levels, nlevels, nbuf));
}

int
slmctl_resfieldi_batchno(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct sl_mds_iosinfo *si;
	char nbuf[20];

	si = res2iosinfo(r);
	if (set)
		return (psc_ctlsenderr(fd, mh,
		    "batcho: field is read-only"));
	snprintf(nbuf, sizeof(nbuf), "%"PRIu64, si->si_batchno);
	return (psc_ctlmsg_param_send(fd, mh, pcp,
	    PCTHRNAME_EVERYONE, levels, nlevels, nbuf));
}

int
slmctl_resfieldi_disable_bia(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct sl_mds_iosinfo *si;
	char nbuf[20];

	si = res2iosinfo(r);
	if (set) {
		if (pcp->pcp_flags & (PCPF_ADD | PCPF_SUB))
			return (psc_ctlsenderr(fd, mh,
			    "invalid operation"));
		if (strcmp(pcp->pcp_value, "0"))
			si->si_flags |= SIF_DISABLE_BIA;
		else
			si->si_flags &= ~SIF_DISABLE_BIA;
		return (1);
	}
	snprintf(nbuf, sizeof(nbuf), "%d",
	    si->si_flags & SIF_DISABLE_BIA ? 1 : 0);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
slmctl_resfieldi_disable_gc(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct sl_mds_iosinfo *si;
	char nbuf[20];

	si = res2iosinfo(r);
	if (set) {
		if (pcp->pcp_flags & (PCPF_ADD | PCPF_SUB))
			return (psc_ctlsenderr(fd, mh,
			    "invalid operation"));
		if (strcmp(pcp->pcp_value, "0"))
			si->si_flags |= SIF_DISABLE_GC;
		else
			si->si_flags &= ~SIF_DISABLE_GC;
		return (1);
	}
	snprintf(nbuf, sizeof(nbuf), "%d",
	    si->si_flags & SIF_DISABLE_GC ? 1 : 0);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
slmctl_resfieldi_preclaim(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct sl_mds_iosinfo *si;
	char nbuf[20];

	si = res2iosinfo(r);
	if (set)
		return (psc_ctlsenderr(fd, mh,
		    "preclaim: field is read-only"));
	snprintf(nbuf, sizeof(nbuf), "%d",
	    si->si_flags & SIF_PRECLAIM_NOTSUP ? 0 : 1);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
slmctl_resfieldi_upschq(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct resprof_mds_info *rpmi;
	char nbuf[20];

	if (set)
		return (psc_ctlsenderr(fd, mh,
		    "upschq: field is read-only"));
	rpmi = res2rpmi(r);
	snprintf(nbuf, sizeof(nbuf), "%d",
	    psc_dynarray_len(&rpmi->rpmi_upschq));
	return (psc_ctlmsg_param_send(fd, mh, pcp,
	    PCTHRNAME_EVERYONE, levels, nlevels, nbuf));
}

const struct slctl_res_field slctl_resmds_fields[] = {
	{ "xid",		slmctl_resfieldm_xid },
	{ NULL, NULL },
};

const struct slctl_res_field slctl_resios_fields[] = {
	{ "batchno",		slmctl_resfieldi_batchno },
	{ "disable_bia",	slmctl_resfieldi_disable_bia },
	{ "disable_gc",		slmctl_resfieldi_disable_gc },
	{ "preclaim",		slmctl_resfieldi_preclaim },
	{ "upschq",		slmctl_resfieldi_upschq },
	{ "xid",		slmctl_resfieldi_xid },
	{ NULL, NULL },
};

/**
 * slmctlcmd_stop - Handle a STOP command to terminate execution.
 */
__dead int
slmctlcmd_stop(__unusedx int fd, __unusedx struct psc_ctlmsghdr *mh,
    __unusedx void *m)
{
	mdsio_exit();
	/* XXX journal_close */
	pscthr_killall();
	/* XXX wait */
	pscrpc_exit_portals();
	exit(0);
}

__static int
slmctlrep_replpair_send(int fd, struct psc_ctlmsghdr *mh,
    struct slmctlmsg_replpair *scrp, struct sl_resm *m0,
    struct sl_resm *m1, int busyonly)
{
	struct resm_mds_info *rmmi0, *rmmi1;
	struct slm_resmlink *srl;

	rmmi0 = resm2rmmi(m0);
	rmmi1 = resm2rmmi(m1);
	srl = repl_busytable + MDS_REPL_BUSYNODES(
	    MIN(rmmi0->rmmi_busyid, rmmi1->rmmi_busyid),
	    MAX(rmmi0->rmmi_busyid, rmmi1->rmmi_busyid));

	if (busyonly && srl->srl_used == 0)
		return (1);

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
slmctlrep_getreplpairs(int fd, struct psc_ctlmsghdr *mh, void *mb)
{
	int busyonly = 0, i, j, i0, j0, rc = 1;
	struct slmctlmsg_replpair *scrp = mb;
	struct sl_resource *r, *r0;
	struct sl_resm *m, *m0;
	struct sl_site *s, *s0;

	if (strcasecmp(scrp->scrp_addrbuf[0], SLMC_RP_ADDRCLASS_BUSY) == 0)
		busyonly = 1;

	CONF_LOCK();
	spinlock(&repl_busytable_lock);
	CONF_FOREACH_RESM(s, r, i, m, j) {
		if (!RES_ISFS(r))
			continue;

		/* stats between members within this resource */
		j0 = j + 1;
		RES_FOREACH_MEMB_CONT(r, m0, j0) {
			rc = slmctlrep_replpair_send(fd, mh, scrp, m,
			    m0, busyonly);
			if (!rc)
				goto done;
		}

		/* stats between resources within this site */
		i0 = i + 1;
		SITE_FOREACH_RES_CONT(s, r0, i0) {
			if (!RES_ISFS(r0))
				continue;
			RES_FOREACH_MEMB(r0, m0, j0) {
				rc = slmctlrep_replpair_send(fd, mh,
				    scrp, m, m0, busyonly);
				if (!rc)
					goto done;
			}
		}

		/* stats between resources of other sites */
		s0 = pll_next_item(&globalConfig.gconf_sites, s);
		CONF_FOREACH_SITE_CONT(s0)
			SITE_FOREACH_RES(s0, r0, i0) {
				if (!RES_ISFS(r0))
					continue;
				RES_FOREACH_MEMB(r0, m0, j0) {
					rc = slmctlrep_replpair_send(fd,
					    mh, scrp, m, m0, busyonly);
					if (!rc)
						goto done;
				}
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
	struct resprof_mds_info *rpmi;
	struct sl_resource *r;
	struct sl_site *s;
	int i, rc = 1;

	CONF_LOCK();
	CONF_FOREACH_RES(s, r, i) {
		if (!RES_ISFS(r))
			continue;

		memset(scsf, 0, sizeof(*scsf));

		rpmi = res2rpmi(r);
		RPMI_LOCK(rpmi);
		scsf->scsf_flags = res2iosinfo(r)->si_flags;
		strlcpy(scsf->scsf_resname, r->res_name,
		    sizeof(scsf->scsf_resname));
		memcpy(&scsf->scsf_ssfb, &res2iosinfo(r)->si_ssfb,
		    sizeof(scsf->scsf_ssfb));
		RPMI_ULOCK(rpmi);

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

int
slctlmsg_bmap_send(int fd, struct psc_ctlmsghdr *mh,
    struct slctlmsg_bmap *scb, struct bmap *b)
{
	scb->scb_fg = b->bcm_fcmh->fcmh_fg;
	scb->scb_bno = b->bcm_bmapno;
	BHGEN_GET(b, &scb->scb_bgen);
	scb->scb_flags = b->bcm_flags;
	scb->scb_opcnt = psc_atomic32_read(&b->bcm_opcnt);
	return (psc_ctlmsg_sendv(fd, mh, scb));
}

/**
 * slmctlrep_getbml - Send a response to a "GETBML" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @m: control message to examine and reuse.
 */
int
slmctlrep_getbml(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slmctlmsg_bml *scbl = m;
	struct bmap_mds_lease *bml, *t;
	struct psc_lockedlist *pll;
	struct bmap_mds_info *bmi;
	int rc = 1;

	pll = &mdsBmapTimeoTbl.btt_leases;
	PLL_LOCK(pll);
	PLL_FOREACH(bml, pll) {
		bmi = bml->bml_bmi;
		memset(scbl, 0, sizeof(*scbl));
		BML_LOCK(bml);
		strlcpy(scbl->scbl_resname,
		    bml->bml_ios && bml->bml_ios != IOS_ID_ANY ?
		    libsl_id2res(bml->bml_ios)->res_name : "<any>",
		    sizeof(scbl->scbl_resname));
		scbl->scbl_fg = bml_2_bmap(bml)->bcm_fcmh->fcmh_fg;
		scbl->scbl_bno = bml_2_bmap(bml)->bcm_bmapno;
		scbl->scbl_seq = bml->bml_seq;
		scbl->scbl_key = bmi->bmi_assign ?
		    bmi->bmi_assign->odtr_key : BMAPSEQ_ANY;
		scbl->scbl_flags = bml->bml_flags;
		scbl->scbl_start = bml->bml_start;
		scbl->scbl_expire = bml->bml_expire;
		pscrpc_id2str(bml->bml_cli_nidpid, scbl->scbl_client);
		for (t = bml; t->bml_chain != bml; t = t->bml_chain)
			scbl->scbl_ndups++;
		BML_ULOCK(bml);

		rc = psc_ctlmsg_sendv(fd, mh, scbl);
		if (!rc)
			break;
	}
	PLL_ULOCK(pll);
	return (rc);
}

struct psc_ctlop slmctlops[] = {
	PSC_CTLDEFOPS,
	{ slctlrep_getbmap,		sizeof(struct slctlmsg_bmap) },
	{ slctlrep_getconn,		sizeof(struct slctlmsg_conn) },
	{ slctlrep_getfcmh,		sizeof(struct slctlmsg_fcmh) },
	{ slmctlrep_getreplpairs,	sizeof(struct slmctlmsg_replpair) },
	{ slmctlrep_getstatfs,		sizeof(struct slmctlmsg_statfs) },
	{ slmctlcmd_stop,		0 },
	{ slmctlrep_getbml,		sizeof(struct slmctlmsg_bml) }
};

psc_ctl_thrget_t psc_ctl_thrgets[] = {
/* BATCHRQ	*/ NULL,
/* BKDB		*/ NULL,
/* BMAPTIMEO	*/ NULL,
/* COH		*/ NULL,
/* CONN		*/ NULL,
/* CTL		*/ psc_ctlthr_get,
/* CTLAC	*/ psc_ctlacthr_get,
/* CURSOR	*/ NULL,
/* DBWORKER	*/ NULL,
/* JNAMESPACE	*/ NULL,
/* JRECLAIM	*/ NULL,
/* JRNL		*/ NULL,
/* LNETAC	*/ NULL,
/* NBRQ		*/ NULL,
/* RCM		*/ NULL,
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
	psc_ctlparam_register("faults", psc_ctlparam_faults);
	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("opstats", psc_ctlparam_opstats);
	psc_ctlparam_register("pause", psc_ctlparam_pause);
	psc_ctlparam_register("pool", psc_ctlparam_pool);
	psc_ctlparam_register("rlim", psc_ctlparam_rlim);
	psc_ctlparam_register("run", psc_ctlparam_run);
	psc_ctlparam_register("rusage", psc_ctlparam_rusage);

	psc_ctlparam_register("namespace.stats", slmctlparam_namespace_stats);
	psc_ctlparam_register_simple("nextfid", slmctlparam_nextfid_get,
	    slmctlparam_nextfid_set);
	psc_ctlparam_register("resources", slctlparam_resources);
	psc_ctlparam_register_simple("uptime", slctlparam_uptime_get,
	    NULL);
	psc_ctlparam_register_simple("version", slctlparam_version_get,
	    NULL);

	psc_ctlthr_main(fn, slmctlops, nitems(slmctlops), SLMTHRT_CTLAC);
}
