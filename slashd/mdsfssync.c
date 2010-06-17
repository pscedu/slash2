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

void
mdsfssyncthr_begin(__unusedx struct psc_thread *thr)
{
	struct jflush_item *jfi;
	struct psc_journal_xidhndl *xh;

	while (pscthr_run()) {
		jfi = lc_getwait(&dirtyMdsData);
		spinlock(&jfi->jfi_lock);

		psc_assert(jfi->jfi_item);
		psc_assert(jfi->jfi_handler);
		psc_assert(jfi->jfi_xh);
		psc_assert(jfi->jfi_state & JFI_HAVE_XH);
		psc_assert(jfi->jfi_state & JFI_QUEUED);

		if (jfi->jfi_state & JFI_BUSY) {
			lc_addtail(&dirtyMdsData, jfi);
			freelock(&jfi->jfi_lock);
			psc_info("fssync jfi(%p) xh(%p) BUSY",
				 jfi, jfi->jfi_xh);
			if (lc_sz(&dirtyMdsData) == 1)
				usleep(100);
			continue;
		}

		xh = jfi->jfi_xh;

		psc_assert(xh->pjx_pj == mdsJournal);

		/*
		 * Detach the transaction handle from the journal flush item,
		 * so that the latter can start a new transaction while the
		 * current one is being closed.
		 */
		jfi->jfi_xh = NULL;
		jfi->jfi_state &= ~JFI_HAVE_XH;

		/* lc_getwait() above removed it from the queue */
		jfi->jfi_state &= ~JFI_QUEUED;

		freelock(&jfi->jfi_lock);
		psc_info("fssync jfi(%p) xh(%p) xid(%"PRIu64") data(%p)",
			  jfi, xh, xh->pjx_xid, jfi->jfi_item);
		/* 
		 * Now run the journal flush item specific flush handler.
		 * Note that the item could contain newer data since the
		 * log was written.
		 */
		(jfi->jfi_handler)(jfi->jfi_item);

		if (pjournal_xend(xh))
			psc_fatal("pjournal_xend() failed");
	}
}

void
slmfssyncthr_spawn(void)
{
	lc_reginit(&dirtyMdsData, struct jflush_item,
	    jfi_lentry, "dirtyMdsData");
	pscthr_init(SLMTHRT_FSSYNC, 0, mdsfssyncthr_begin,
	    NULL, 0, "slmfssyncthr");
}
