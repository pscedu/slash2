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

#include "psc_ds/listcache.h"
#include "psc_util/journal.h"
#include "psc_util/lock.h"
#include "psc_util/log.h"

#include "jflush.h"

/**
 * jfi_prep - Prepare a journal flush item and make sure it has an
 *	associated transaction handle.
 */
void
jfi_prep(struct jflush_item *jfi, struct psc_journal *pj)
{
	spinlock(&jfi->jfi_lock);
	if (jfi->jfi_state & JFI_HAVE_XH) {
		psc_assert(jfi->jfi_xh);
		/* XXX The following check will fail when the
		 *   mdsfssync thread pulls us from the
		 *   dirtyMdsData list.
		 */
		//if (jfi->jfi_state & JFI_QUEUED)
		//       psc_assert(psclist_conjoint(&jfi->jfi_lentry));
	} else {
		psc_assert(jfi->jfi_xh == NULL);
		psc_assert(!(jfi->jfi_state & JFI_QUEUED));
		psc_assert(psclist_disjoint(&jfi->jfi_lentry));
		jfi->jfi_xh = pjournal_xnew(pj);
		jfi->jfi_state |= JFI_HAVE_XH;
		if (jfi->jfi_prepcb)
			(jfi->jfi_prepcb)(jfi->jfi_data);
	}
	jfi->jfi_state |= JFI_BUSY;

	freelock(&jfi->jfi_lock);
}

/**
 * jfs_schedule - Clear the busy flag of a journal flush item and link
 *     it to the given list if it is not already on the list.
 */    
void
jfi_schedule(struct jflush_item *jfi, struct psc_listcache *lc)
{
	spinlock(&jfi->jfi_lock);

	psc_assert(jfi->jfi_xh && (jfi->jfi_state & JFI_HAVE_XH));
	psc_assert(jfi->jfi_state & JFI_BUSY);

	jfi->jfi_state &= ~JFI_BUSY;

	if (!(jfi->jfi_state & JFI_QUEUED)) {
		psc_assert(psclist_disjoint(&jfi->jfi_lentry));
		jfi->jfi_state |= JFI_QUEUED;
		lc_addqueue(lc, jfi);
	}
	freelock(&jfi->jfi_lock);
}
