/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2009-2016, Pittsburgh Supercomputing Center
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

#include <stdio.h>

#include "libcfs/kp30.h"
#include "lnet/lib-types.h"
#include "lnet/lib-lnet.h"

#include "pfl/ctl.h"
#include "pfl/ctlsvr.h"
#include "pfl/lock.h"
#include "pfl/rpc.h"
#include "pfl/str.h"

#include "ctl.h"
#include "ctlsvr.h"
#include "fidcache.h"
#include "slconfig.h"
#include "slconn.h"

void
slctl_fillconn(struct slctlmsg_conn *scc,
    struct slrpc_cservice *csvc)
{
	struct pscrpc_import *imp;
	lnet_peer_t *lp;

	memset(scc, 0, sizeof(*scc));

	if (csvc == NULL)
		return;

	scc->scc_flags = csvc->csvc_flags;
	scc->scc_refcnt = csvc->csvc_refcnt;

	imp = csvc->csvc_import;
	if (imp == NULL || imp->imp_connection == NULL)
		return;

	lp = lnet_find_peer_locked(imp->imp_connection->c_peer.nid);
	if (lp == NULL)
		return;

	scc->scc_txcr = lp->lp_txcredits;
}

/*
 * Send a response to a "GETCONN" inquiry.
 * @fd: client socket descriptor.
 * @mh: already filled-in control message header.
 * @m: control message to examine and reuse.
 */
int
slctlrep_getconn(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slrpc_cservice *csvc;
	struct slctlmsg_conn *scc = m;
	struct pscrpc_import *imp;
	struct sl_resm_nid *mn;
	struct sl_resource *r;
	struct sl_resm *resm;
	struct sl_site *s;
	struct timespec tv1, tv2;
	int i, j, rc = 1;
	struct sl_exp_cli *expc;

	_PFL_GETTIMESPEC(CLOCK_MONOTONIC, &tv1);
	CONF_LOCK();
	CONF_FOREACH_RESM(s, r, i, resm, j) {
		if (resm == nodeResm ||
		    (RES_ISCLUSTER(r)))
			continue;

		pfl_rwlock_rdlock(&sl_conn_lock);
		if (!resm->resm_csvc) {
			pfl_rwlock_unlock(&sl_conn_lock);
			continue;
		}
		/*
 		 * csvc associated with a resource never goes
 		 * away, so we don't take a lock here.
 		 */
		CSVC_LOCK(resm->resm_csvc);
		slctl_fillconn(scc, resm->resm_csvc);
		CSVC_ULOCK(resm->resm_csvc);
		pfl_rwlock_unlock(&sl_conn_lock);

		/* XXX fix which nid is being used */
		mn = psc_dynarray_getpos(&resm->resm_nids, 0);
		strlcpy(scc->scc_addrbuf, mn->resmnid_addrbuf,
		    sizeof(scc->scc_addrbuf));
		scc->scc_stkvers = r->res_stkvers;
		if (scc->scc_stkvers < sl_stk_version)
			scc->scc_flags |= CSVCF_CTL_OLDER;
		else if (scc->scc_stkvers > sl_stk_version)
			scc->scc_flags |= CSVCF_CTL_NEWER;
		scc->scc_type = resm->resm_type;
		tv2.tv_sec = r->res_uptime;
		tv2.tv_nsec = 0;
		timespecsub(&tv1, &tv2, &tv2);
		scc->scc_uptime = tv2.tv_sec;

		rc = psc_ctlmsg_sendv(fd, mh, scc, NULL);
		if (!rc)
			goto done;
	}
 done:
	CONF_ULOCK();

	if (!rc)
		return (rc);

	CONF_LOCK();
	PLL_LOCK(&sl_clients);
	PLL_FOREACH(csvc, &sl_clients) {

		CSVC_LOCK(csvc);
		if (csvc->csvc_flags & CSVCF_TOFREE) {
			CSVC_ULOCK(csvc);
			continue;
		}
		slctl_fillconn(scc, csvc);
		CSVC_ULOCK(csvc);

		imp = csvc->csvc_import;
		if (imp && imp->imp_connection)
			pscrpc_id2str(imp->imp_connection->c_peer,
			    scc->scc_addrbuf);
		else
			strlcpy(scc->scc_addrbuf, "?",
			    sizeof(scc->scc_addrbuf));
		expc = (void *)csvc->csvc_params.scp_csvcp;
		scc->scc_stkvers = expc->expc_stkvers;
		if (scc->scc_stkvers < sl_stk_version)
			scc->scc_flags |= CSVCF_CTL_OLDER;
		else if (scc->scc_stkvers > sl_stk_version)
			scc->scc_flags |= CSVCF_CTL_NEWER;
		scc->scc_type = SLCTL_REST_CLI;

		tv2.tv_sec = expc->expc_uptime;
		tv2.tv_nsec = 0;
		timespecsub(&tv1, &tv2, &tv2);
		scc->scc_uptime = tv2.tv_sec;

		rc = psc_ctlmsg_sendv(fd, mh, scc, NULL);
		if (!rc)
			break;
	}
	PLL_ULOCK(&sl_clients);
	CONF_ULOCK();
	return (rc);
}

__static int
slctlmsg_fcmh_send(int fd, struct psc_ctlmsghdr *mh,
    struct slctlmsg_fcmh *scf, struct fidc_membh *f)
{
	scf->scf_fg = f->fcmh_fg;
	scf->scf_ptruncgen = f->fcmh_sstb.sst_ptruncgen;
	scf->scf_utimgen = f->fcmh_sstb.sst_utimgen;
	scf->scf_st_mode = f->fcmh_sstb.sst_mode;
	scf->scf_uid = f->fcmh_sstb.sst_uid;
	scf->scf_gid = f->fcmh_sstb.sst_gid;
	scf->scf_flags = f->fcmh_flags;
	scf->scf_refcnt = f->fcmh_refcnt;
	scf->scf_size = fcmh_2_fsz(f);
	scf->scf_blksize = f->fcmh_sstb.sst_blksize;
	return (psc_ctlmsg_sendv(fd, mh, scf, NULL));
}

int
slctlrep_getfcmh(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slctlmsg_fcmh *scf = m;
	struct psc_hashbkt *b;
	struct fidc_membh *f;
	int rc;

	rc = 1;
	if (scf->scf_fg.fg_fid == FID_ANY) {
		PSC_HASHTBL_FOREACH_BUCKET(b, &sl_fcmh_hashtbl) {
			psc_hashbkt_lock(b);
			PSC_HASHBKT_FOREACH_ENTRY(&sl_fcmh_hashtbl, f,
			    b) {
				if (scf->scf_fg.fg_gen == SLCTL_FCL_BUSY &&
				    (f->fcmh_flags & FCMH_IDLE))
					continue;
				rc = slctlmsg_fcmh_send(fd, mh, scf, f);
				if (!rc)
					break;
			}
			psc_hashbkt_unlock(b);
			if (!rc)
				break;
		}
	} else {
		if (sl_fcmh_peek_fid(scf->scf_fg.fg_fid, &f)) {
			rc = psc_ctlsenderr(fd, mh, NULL,
			    "FID "SLPRI_FID" not in cache",
			    scf->scf_fg.fg_fid);
		} else {
			rc = slctlmsg_fcmh_send(fd, mh, scf, f);
			fcmh_op_done(f);
		}
	}
	return (rc);
}

int
slctlrep_getbmap(int fd, struct psc_ctlmsghdr *mh, void *m)
{
	struct slctlmsg_bmap *scb = m;
	struct psc_hashbkt *hb;
	struct fidc_membh *f;
	struct bmap *b;
	int rc;

	rc = 1;
	PSC_HASHTBL_FOREACH_BUCKET(hb, &sl_fcmh_hashtbl) {
		psc_hashbkt_lock(hb);
		PSC_HASHBKT_FOREACH_ENTRY(&sl_fcmh_hashtbl, f, hb) {
			pfl_rwlock_rdlock(&f->fcmh_rwlock);
			RB_FOREACH(b, bmaptree, &f->fcmh_bmaptree) {
				rc = slctlmsg_bmap_send(fd, mh, scb, b);
				if (!rc)
					break;
			}
			pfl_rwlock_unlock(&f->fcmh_rwlock);
			if (!rc)
				break;
		}
		psc_hashbkt_unlock(hb);
		if (!rc)
			break;
	}
	return (rc);
}

int
slctlparam_resources(int fd, struct psc_ctlmsghdr *mh,
    struct psc_ctlmsg_param *pcp, char **levels, int nlevels,
    __unusedx struct psc_ctlparam_node *pcn)
{
	int site_found = 0, res_found = 0, field_found = 0, set, i;
	char p_site[SITE_NAME_MAX], p_res[RES_NAME_MAX], p_field[32];
	char *p, resname[RES_NAME_MAX];
	const struct slctl_res_field *cfv, *cf;
	struct sl_resource *r;
	struct sl_site *s;

	if (nlevels > 5)
		return (psc_ctlsenderr(fd, mh, NULL, "invalid field"));

	set = mh->mh_type == PCMT_SETPARAM;

	if (set && nlevels != 5)
		return (psc_ctlsenderr(fd, mh, NULL, "invalid field"));

	/* sys.resources.<site>.<res>.<field> */
	/* ex: sys.resources.SITE.RES.FIELD */
	/* ex: sys.resources.SITE.* */
	/* ex: sys.resources.*.*.FIELD */

	levels[0] = "sys";
	levels[1] = "resources";

	if (strlcpy(p_site, nlevels > 2 ? levels[2] : "*",
	    sizeof(p_site)) >= sizeof(p_site))
		return (psc_ctlsenderr(fd, mh, NULL, "invalid site"));
	if (strlcpy(p_res, nlevels > 3 ? levels[3] : "*",
	    sizeof(p_res)) >= sizeof(p_res))
		return (psc_ctlsenderr(fd, mh, NULL, "invalid resource"));
	if (strlcpy(p_field, nlevels > 4 ? levels[4] : "*",
	    sizeof(p_field)) >= sizeof(p_field))
		return (psc_ctlsenderr(fd, mh, NULL, "invalid field"));

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
				return (psc_ctlsenderr(fd, mh, NULL,
				    "invalid resource"));
			continue;
		}

		levels[2] = s->site_name;
		levels[3] = resname;

		cfv = r->res_type == SLREST_MDS ?
		    slctl_resmds_fields : slctl_resios_fields;
		for (cf = cfv; cf->name; cf++) {
			if (strcmp(p_field, "*") == 0 ||
			    strcmp(p_field, cf->name) == 0) {
				field_found = 1;
				levels[4] = (char *)cf->name;
				if (!cf->cbf(fd, mh, pcp, levels, 5,
				    set, r))
					return (0);
			}
		}
		if (!field_found && strcmp(p_field, "*") &&
		    strcmp(p_res, "*") && strcmp(p_site, "*"))
			return (psc_ctlsenderr(fd, mh, NULL,
			    "invalid resources field: %s", p_field));
	}
	if (!site_found)
		return (psc_ctlsenderr(fd, mh, NULL, "invalid site: %s",
		    p_site));
	if (!res_found)
		return (psc_ctlsenderr(fd, mh, NULL, "invalid resource: %s",
		    p_res));
	if (!field_found)
		return (psc_ctlsenderr(fd, mh, NULL, "invalid field: %s",
		    p_field));
	return (1);
}

void
slctlparam_version_get(char *val)
{
	snprintf(val, PCP_VALUE_MAX, "%d", sl_stk_version);
}

void
slctlparam_uptime_get(char *val)
{
	struct timespec tv, delta;

	_PFL_GETTIMESPEC(CLOCK_MONOTONIC, &tv);
	timespecsub(&tv, &pfl_uptime, &delta);
	snprintf(val, PCP_VALUE_MAX, "%ldd%ldh%ldm",
	    delta.tv_sec / (60 * 60 * 24),
	    (delta.tv_sec % (60 * 60 * 24)) / (60 * 60),
	    (delta.tv_sec % (60 * 60)) / 60);
}

void
slctlparam_logrotate_get(char *val)
{
	snprintf(val, PCP_VALUE_MAX, "%d", pfl_log_rotate);
}

int
slctlparam_logrotate_set(const char *val)
{
	int newval;

	newval = strtol(val, NULL, 0);
	if (newval < 0)
		return (1);
	if (newval)
		pfl_log_rotate = newval;
	else
		pfl_log_rotate = PSC_MAX_LOG_PER_FILE;
	return (0);
}

