/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2009, Pittsburgh Supercomputing Center (PSC).
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

#include <inttypes.h>
#include <unistd.h>

#include "pfl/cdefs.h"
#include "psc_ds/listcache.h"
#include "psc_util/journal.h"
#include "psc_util/log.h"
#include "psc_util/thread.h"

#include "jflush.h"
#include "mdslog.h"
#include "slashd.h"

struct psc_listcache dirtyMdsData;

__static void *
mdsfssyncthr_begin(__unusedx void *arg)
{
	struct jflush_item *jfi;
	struct psc_journal_xidhndl *xh;
	jflush_handler jfih;
	void *data;

	while (1) {
		jfi = lc_getwait(&dirtyMdsData);
		spinlock(&jfi->jfi_lock);

		psc_assert(jfi->jfi_data);
		psc_assert(jfi->jfi_handler);
		psc_assert(jfi->jfi_xh);
		psc_assert(jfi->jfi_state & JFI_HAVE_XH);
		psc_assert(jfi->jfi_state & JFI_QUEUED);

		if (jfi->jfi_state & JFI_BUSY) {
			freelock(&jfi->jfi_lock);
			lc_addtail(&dirtyMdsData, jfi);
			psc_info("fssync jfi(%p) xh(%p) BUSY",
				 jfi, jfi->jfi_xh);
			if (lc_sz(&dirtyMdsData) == 1)
				usleep(100);
			continue;
		}

		/* Copy the data items so that the lock may be released
		 *  prior to the sync function being run.
		 */
		xh = jfi->jfi_xh;
		data = jfi->jfi_data;
		jfih = jfi->jfi_handler;

		psc_assert(xh->pjx_pj == mdsJournal);

		/*
		 * Detach the transaction handle from the journal flush item,
		 * so that the latter can start a new transaction while the
		 * current one is being closed.
		 */
		jfi->jfi_xh = NULL;
		jfi->jfi_state &= ~JFI_QUEUED;
		jfi->jfi_state &= ~JFI_HAVE_XH;

		freelock(&jfi->jfi_lock);
		/* Now run the app-specific data flush code.
		 */
		psc_info("fssync jfi(%p) xh(%p) xid(%"PRIu64") data(%p)",
			  jfi, xh, xh->pjx_xid, data);
		(jfih)(data);

		psc_assert(xh->pjx_pj == mdsJournal);

		if (pjournal_xend(xh))
			psc_fatal("pjournal_xend() failed");
	}
}

void
slmfssyncthr_init(void)
{
	lc_reginit(&dirtyMdsData, struct jflush_item, jfi_lentry,
		   "dirtyMdsData");
	pscthr_init(SLMTHRT_FSSYNC, 0, mdsfssyncthr_begin,
	    NULL, 0, "slmfssyncthr");
}
