/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2006-2016, Pittsburgh Supercomputing Center
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
#include "mdslog.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slconfig.h"
#include "slconn.h"
#include "up_sched_res.h"

struct slm_nsstats		 slm_nsstats_aggr;	/* aggregate stats */

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
slmctl_resfieldm_xid(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct sl_mds_peerinfo *sp;
	char nbuf[24];

	sp = res2mdsinfo(r);
	if (set)
		return (psc_ctlsenderr(fd, mh, NULL,
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
	char nbuf[24];

	si = res2iosinfo(r);
	if (set)
		return (psc_ctlsenderr(fd, mh, NULL,
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
	char nbuf[24];

	si = res2iosinfo(r);
	if (set)
		return (psc_ctlsenderr(fd, mh, NULL,
		    "batcho: field is read-only"));
	snprintf(nbuf, sizeof(nbuf), "%"PRIu64, si->si_batchno);
	return (psc_ctlmsg_param_send(fd, mh, pcp,
	    PCTHRNAME_EVERYONE, levels, nlevels, nbuf));
}

int
slmctl_resfieldi_disable_write(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	char nbuf[8];

	si = res2iosinfo(r);
	if (set) {
		if (pcp->pcp_flags & (PCPF_ADD | PCPF_SUB))
			return (psc_ctlsenderr(fd, mh, NULL,
			    "invalid operation"));
		rpmi = res2rpmi(r);
		RPMI_LOCK(rpmi);
		if (strcmp(pcp->pcp_value, "0"))
			si->si_flags |= SIF_DISABLE_LEASE;
		else
			si->si_flags &= ~SIF_DISABLE_LEASE;
		RPMI_ULOCK(rpmi);
		return (1);
	}
	snprintf(nbuf, sizeof(nbuf), "%d",
	    si->si_flags & SIF_DISABLE_LEASE ? 1 : 0);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

int
slmctl_resfieldi_disable_gc(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels, int set,
    struct sl_resource *r)
{
	struct resprof_mds_info *rpmi;
	struct sl_mds_iosinfo *si;
	char nbuf[8];

	si = res2iosinfo(r);
	if (set) {
		if (pcp->pcp_flags & (PCPF_ADD | PCPF_SUB))
			return (psc_ctlsenderr(fd, mh, NULL,
			    "invalid operation"));
		rpmi = res2rpmi(r);
		RPMI_LOCK(rpmi);
		if (strcmp(pcp->pcp_value, "0"))
			si->si_flags |= SIF_DISABLE_GC;
		else
			si->si_flags &= ~SIF_DISABLE_GC;
		RPMI_ULOCK(rpmi);
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
	char nbuf[8];

	si = res2iosinfo(r);
	if (set)
		return (psc_ctlsenderr(fd, mh, NULL,
		    "preclaim: field is read-only"));
	snprintf(nbuf, sizeof(nbuf), "%d",
	    si->si_flags & SIF_PRECLAIM_NOTSUP ? 0 : 1);
	return (psc_ctlmsg_param_send(fd, mh, pcp, PCTHRNAME_EVERYONE,
	    levels, nlevels, nbuf));
}

const struct slctl_res_field slctl_resmds_fields[] = {
	{ "xid",		slmctl_resfieldm_xid },
	{ NULL, NULL },
};

const struct slctl_res_field slctl_resios_fields[] = {
	{ "batchno",		slmctl_resfieldi_batchno },
	{ "disable_write",	slmctl_resfieldi_disable_write },
	{ "disable_gc",		slmctl_resfieldi_disable_gc },
	{ "preclaim",		slmctl_resfieldi_preclaim },
	{ "xid",		slmctl_resfieldi_xid },
	{ NULL, NULL },
};

/*
 * Handle a STOP command to terminate execution.
 */
__dead int
slmctlcmd_stop(__unusedx int fd, __unusedx struct psc_ctlmsghdr *mh,
    __unusedx void *m)
{
	/* pfl_odt_close(ptrunc); */
	/* pfl_odt_close(bml); */

	sqlite3_close_v2(db_handle);

	mdsio_exit();
	/* XXX journal_close */
	pscthr_killall();
	/* XXX wait */
	pscrpc_exit_portals();

	exit(0);
}

int
slmctlcmd_upsch_query(__unusedx int fd,
    __unusedx struct psc_ctlmsghdr *mh, void *m)
{
	struct slmctlmsg_upsch_query *scuq = m;

	/*
	 * XXX if there are any bind parameters, users can craft code
	 * that will make our va_arg crash
	 */
	dbdo(NULL, NULL, scuq->scuq_query);
	return (0);
}

/*
 * Send a response to a "GETREPLQUEUED" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @m: control message to examine and reuse.
 */
int
slmctlrep_getreplqueued(int fd, struct psc_ctlmsghdr *mh, void *mb)
{
	int i, rc = 1;
	struct slmctlmsg_replqueued *scrq = mb;
	struct sl_resource *r;
	struct rpmi_ios *si;
	struct sl_site *s;

	CONF_LOCK();
	CONF_FOREACH_RES(s, r, i) {
		if (!RES_ISFS(r))
			continue;

		si = res2rpmi_ios(r);

		memset(scrq, 0, sizeof(*scrq));
		strlcpy(scrq->scrq_resname, r->res_name,
		    sizeof(scrq->scrq_resname));
		scrq->scrq_repl_egress_pending = si->si_repl_egress_pending;
		scrq->scrq_repl_ingress_pending = si->si_repl_ingress_pending;
		scrq->scrq_repl_egress_aggr = si->si_repl_egress_aggr;
		scrq->scrq_repl_ingress_aggr = si->si_repl_ingress_aggr;
		rc = psc_ctlmsg_sendv(fd, mh, scrq, NULL);

		if (!rc)
			goto done;
	}
 done:
	CONF_ULOCK();
	return (rc);
}

/*
 * Send a response to a "GETSTATFS" inquiry.
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

		rc = psc_ctlmsg_sendv(fd, mh, scsf, NULL);
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
			    SLASH_FID_MDSID_SHFT);
	}
	freelock(&slm_fid_lock);
	return (rc);
}

static char cmdbuf[PCP_VALUE_MAX] = "N/A";

void
slmctlparam_execute_get(char *val)
{
	snprintf(val, PCP_VALUE_MAX, "%s", cmdbuf);
}

int
slmctlparam_execute_set(const char *val)
{
	int rc;
	strlcpy(cmdbuf, val, PCP_VALUE_MAX);
	rc = system(cmdbuf);
	if (rc == -1)
		rc = -errno;
	else if (WIFEXITED(rc))
		rc = WEXITSTATUS(rc);
	/*
 	 * rc = 127 means "command not found", which means possible 
 	 * problem with $PATH or a typo.
 	 */
	psclog(PLL_WARN, "Executed command %s, rc = %d", cmdbuf, rc);
	if (rc) {
		cmdbuf[0] = 'N';
		cmdbuf[1] = '/';
		cmdbuf[2] = 'A';
		cmdbuf[3] = '\0';
	}
	return (rc);
}

void
slmctlparam_reboots_get(char *val)
{
	int rc;
	void *h;
	uint64_t size;
        int32_t boot = 0;
        char *fn = "boot.log";

        rc = mds_open_file(fn, O_RDONLY, &h); 
        if (rc) 
		goto out;
        rc = mds_read_file(h, &boot, sizeof(boot), &size, 0);
        mds_release_file(h);
 out:
	snprintf(val, PCP_VALUE_MAX, "%d", boot);
}

int
slmctlparam_reboots_set(const char *val)
{
	int rc;
	void *h;
	uint64_t size;
        int32_t boot = 0;
        char *fn = "boot.log";

	boot = strtol(val, NULL, 0);
	if (boot < 0) {
		rc = -1;
		goto out;
	}
        rc = mds_open_file(fn, O_WRONLY, &h); 
        if (rc) 
		goto out;

	rc = mds_write_file(h, &boot, sizeof(boot), &size, 0);
        mds_release_file(h);

 out:
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
	return (psc_ctlmsg_sendv(fd, mh, scb, NULL));
}

/*
 * Send a response to a "GETBML" inquiry.
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
	int rc = 1;

	pll = &slm_bmap_leases.btt_leases;
	PLL_LOCK(pll);
	PLL_FOREACH(bml, pll) {
		memset(scbl, 0, sizeof(*scbl));
		strlcpy(scbl->scbl_resname,
		    bml->bml_ios && bml->bml_ios != IOS_ID_ANY ?
		    libsl_id2res(bml->bml_ios)->res_name : "<any>",
		    sizeof(scbl->scbl_resname));
		scbl->scbl_fg = bml_2_bmap(bml)->bcm_fcmh->fcmh_fg;
		scbl->scbl_bno = bml_2_bmap(bml)->bcm_bmapno;
		scbl->scbl_seq = bml->bml_seq;
		scbl->scbl_key = BMAPSEQ_ANY;
		scbl->scbl_flags = bml->bml_flags;
		scbl->scbl_start = bml->bml_start;
		scbl->scbl_expire = bml->bml_expire;
		pscrpc_id2str(bml->bml_cli_nidpid, scbl->scbl_client);
		for (t = bml; t->bml_chain != bml; t = t->bml_chain)
			scbl->scbl_ndups++;

		rc = psc_ctlmsg_sendv(fd, mh, scbl, NULL);
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
	{ slmctlrep_getreplqueued,	sizeof(struct slmctlmsg_replqueued) },
	{ slmctlrep_getstatfs,		sizeof(struct slmctlmsg_statfs) },
	{ slmctlcmd_stop,		0 },
	{ slmctlrep_getbml,		sizeof(struct slmctlmsg_bml) },
	{ slmctlcmd_upsch_query,	0 },
};

void
slmctlparam_upsch_get(char *val)
{
	int upsch_total = 0;

	dbdo(slm_upsch_tally_cb, &upsch_total,
	    " SELECT	count (*)"
	    " FROM	upsch");

	snprintf(val, PCP_VALUE_MAX, "%d", upsch_total);
}

void
slmctlparam_commit_get(char *val)
{
	snprintf(val, PCP_VALUE_MAX, "%lu", mds_cursor.pjc_commit_txg);
}

void
slmctlthr_spawn(const char *fn)
{
	pfl_journal_register_ctlops(slmctlops);
	pflrpc_register_ctlops(slmctlops);

	psc_ctlparam_register("faults", psc_ctlparam_faults);
	psc_ctlparam_register("log.console", psc_ctlparam_log_console);

#ifdef Linux
	psc_ctlparam_register("log.file", psc_ctlparam_log_file);
#endif
	psc_ctlparam_register("log.format", psc_ctlparam_log_format);
	psc_ctlparam_register("log.level", psc_ctlparam_log_level);
	psc_ctlparam_register("log.points", psc_ctlparam_log_points);
	psc_ctlparam_register("opstats", psc_ctlparam_opstats);
	psc_ctlparam_register("pause", psc_ctlparam_pause);

	psc_ctlparam_register("pool", psc_ctlparam_pool);
	psc_ctlparam_register("rlim", psc_ctlparam_rlim);
	psc_ctlparam_register("run", psc_ctlparam_run);
	psc_ctlparam_register("rusage", psc_ctlparam_rusage);

	psc_ctlparam_register_simple("sys.execute",
	    slmctlparam_execute_get, slmctlparam_execute_set);

	psc_ctlparam_register_var("sys.crc_check",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_crc_check);

	psc_ctlparam_register_var("sys.conn_debug",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &sl_conn_debug);

	psc_ctlparam_register_simple("sys.commit_txg",
	    slmctlparam_commit_get, NULL);
	psc_ctlparam_register_simple("sys.next_fid",
	    slmctlparam_nextfid_get, slmctlparam_nextfid_set);

	psc_ctlparam_register_var("sys.nbrq_outstanding",
	    PFLCTL_PARAMT_INT, 0, &sl_nbrqset->set_remaining);
	psc_ctlparam_register("sys.resources", slctlparam_resources);
	psc_ctlparam_register_simple("sys.uptime",
	    slctlparam_uptime_get, NULL);
	psc_ctlparam_register_simple("sys.version",
	    slctlparam_version_get, NULL);
	psc_ctlparam_register_var("sys.datadir", PFLCTL_PARAMT_STR, 0,
	    (char *)sl_datadir);

	psc_ctlparam_register_var("sys.force_dio",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_force_dio);

	psc_ctlparam_register_var("sys.global",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_global_mount);
	psc_ctlparam_register_var("sys.max_ios",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_max_ios);

	psc_ctlparam_register_var("sys.pid", PFLCTL_PARAMT_INT, 0,
	    &pfl_pid);
	psc_ctlparam_register_var("sys.ptrunc",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_ptrunc_enabled);
	psc_ctlparam_register_var("sys.preclaim",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_preclaim_enabled);

	psc_ctlparam_register_var("sys.reclaim_xid",
	    PFLCTL_PARAMT_UINT64, 0, &reclaim_prg.cur_xid);
	psc_ctlparam_register_var("sys.reclaim_batchno",
	    PFLCTL_PARAMT_UINT64, 0, &reclaim_prg.cur_batchno);
	psc_ctlparam_register_var("sys.reclaim_cursor",
	    PFLCTL_PARAMT_UINT64, 0, &slm_reclaim_proc_batchno);

	psc_ctlparam_register_simple("sys.reboots",
	    slmctlparam_reboots_get, slmctlparam_reboots_set);

	psc_ctlparam_register_var("sys.rpc_timeout",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &pfl_rpc_timeout);

#ifdef Linux
	psc_ctlparam_register("sys.rss", psc_ctlparam_get_rss);
#endif

	psc_ctlparam_register_var("sys.bmaxseqno", PFLCTL_PARAMT_UINT64,
	    0, &slm_bmap_leases.btt_maxseq);
	psc_ctlparam_register_var("sys.bminseqno", PFLCTL_PARAMT_UINT64,
	    0, &slm_bmap_leases.btt_minseq);

	psc_ctlparam_register_simple("sys.upsch",
	    slmctlparam_upsch_get, NULL);

	psc_ctlparam_register_var("sys.upsch_batch_size",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_upsch_batch_size);

	psc_ctlparam_register_simple("sys.upsch_batch_inflight",
	    slrcp_batch_get_max_inflight, slrcp_batch_set_max_inflight);

	psc_ctlparam_register_var("sys.upsch_page_interval",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_upsch_page_interval);

	psc_ctlparam_register_var("sys.min_space_reserve",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_min_space_reserve_pct);

	psc_ctlparam_register_var("sys.upsch_repl_expire",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_upsch_repl_expire);

	psc_ctlparam_register_var("sys.upsch_preclaim_expire",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_upsch_preclaim_expire);

	psc_ctlparam_register_var("sys.upsch_bandwidth",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_upsch_bandwidth);

	psc_ctlparam_register_var("sys.quiesce",
	    PFLCTL_PARAMT_INT, PFLCTL_PARAMF_RDWR, &slm_quiesce);

	psc_ctlthr_main(fn, slmctlops, nitems(slmctlops), 0, SLMTHRT_CTLAC);
}
