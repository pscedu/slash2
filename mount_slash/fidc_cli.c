/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2013, Pittsburgh Supercomputing Center (PSC).
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

#define PSC_SUBSYS SLSS_FCMH
#include "slsubsys.h"

#include <sys/types.h>
#include <sys/stat.h>

#include <stdio.h>
#include <string.h>

#include "pfl/cdefs.h"
#include "pfl/hashtbl.h"
#include "pfl/str.h"
#include "pfl/time.h"
#include "pfl/list.h"
#include "pfl/listcache.h"
#include "pfl/rsx.h"
#include "pfl/alloc.h"
#include "pfl/atomic.h"
#include "pfl/ctlsvr.h"

#include "cache_params.h"
#include "dircache.h"
#include "fid.h"
#include "fidc_cli.h"
#include "fidcache.h"
#include "mount_slash.h"
#include "rpc_cli.h"

/*
 * XXX check client attributes when generation number changes
 *
 */

void
slc_fcmh_refresh_age(struct fidc_membh *f)
{
	struct timeval tmp = { FCMH_ATTR_TIMEO, 0 };
	struct fcmh_cli_info *fci;

	fci = fcmh_2_fci(f);
	PFL_GETTIMEVAL(&fci->fci_age);
	timeradd(&fci->fci_age, &tmp, &fci->fci_age);

	if (fcmh_isdir(f) &&
	    (f->fcmh_flags & FCMH_CLI_INITDIRCACHE) == 0) {
		pll_init(&fci->fci_dc_pages, struct dircache_page,
		    dcp_lentry, &f->fcmh_lock);
		f->fcmh_flags |= FCMH_CLI_INITDIRCACHE;
	}
}

int
slc_fcmh_ctor(struct fidc_membh *f, __unusedx int flags)
{
	struct fcmh_cli_info *fci;
	struct sl_resource *res;
	struct sl_site *s;
	sl_siteid_t siteid;
	int i;

	OPSTAT_INCR(SLC_OPST_SLC_FCMH_CTOR);

	fci = fcmh_get_pri(f);
	slc_fcmh_refresh_age(f);
	INIT_PSC_LISTENTRY(&fci->fci_lentry);
	siteid = FID_GET_SITEID(fcmh_2_fid(f));

	if (fcmh_2_fid(f) >= SLFID_MIN &&
	    siteid != slc_rmc_resm->resm_siteid) {
		s = libsl_siteid2site(siteid);
		if (s == NULL) {
			psclog_errorx("fid "SLPRI_FID" has "
			    "invalid site ID %d",
			    fcmh_2_fid(f), siteid);
			return (ESTALE);
		}
		SITE_FOREACH_RES(s, res, i)
			if (res->res_type == SLREST_MDS) {
				fci->fci_resm = psc_dynarray_getpos(
				    &res->res_members, 0);
				return (0);
			}
		psclog_errorx("fid "SLPRI_FID" has invalid site ID %d",
		    fcmh_2_fid(f), siteid);
		return (ESTALE);
	}
	fci->fci_resm = slc_rmc_resm;

	return (0);
}

void
slc_fcmh_dtor(struct fidc_membh *f)
{
	/* XXX consolidate into pool stats */
	OPSTAT_INCR(SLC_OPST_SLC_FCMH_DTOR);

	if (f->fcmh_flags & FCMH_CLI_INITDIRCACHE)
		dircache_purge(f);
}

#if PFL_DEBUG > 0
void
dump_fcmh_flags(int flags)
{
	int seq = 0;

	_dump_fcmh_flags_common(&flags, &seq);
	PFL_PRFLAG(FCMH_CLI_HAVEREPLTBL, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_FETCHREPLTBL, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_INITDIRCACHE, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_TRUNC, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_DIRTY_ATTRS, &flags, &seq);
	PFL_PRFLAG(FCMH_CLI_DIRTY_QUEUE, &flags, &seq);
	if (flags)
		printf(" unknown: %x", flags);
	printf("\n");
}
#endif

struct sl_fcmh_ops sl_fcmh_ops = {
/* ctor */		slc_fcmh_ctor,
/* dtor */		slc_fcmh_dtor,
/* getattr */		msl_stat,
/* postsetattr */	slc_fcmh_refresh_age,
/* modify */		NULL
};
