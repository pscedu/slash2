/* $Id$ */

#include <inttypes.h>

#include "psc_types.h"
#include "psc_ds/listcache.h"
#include "psc_util/assert.h"
#include "psc_util/cdefs.h"
#include "psc_util/journal.h"
#include "psc_util/thread.h"

#include "jflush.h"
#include "slashdthr.h"

list_cache_t dirtyMdsData;

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
		/* Copy the data items so that the lock may be released
		 *  prior to the sync function being run.
		 */
		xh = jfi->jfi_xh;
		data = jfi->jfi_data;
		jfih = jfi->jfi_handler;
		/* Mark the appropriate state changes in the JFI.
		 */
		pjournal_xidhndl_free(jfi->jfi_xh);
		jfi->jfi_xh = NULL;
		jfi->jfi_state &= ~JFI_QUEUED;
		jfi->jfi_state &= ~JFI_HAVE_XH;

		freelock(&jfi->jfi_lock);
		/* Now run the app-specific data flush code.
		 */
		psc_trace("fssync jfi(%p) xh(%p) xid(%"PRIu64") data(%p)",
			  jfi, xh, xh->pjx_xid, data);
		(jfih)(data);

		if (pjournal_xend(xh, PJET_VOID, NULL, 0))
			psc_fatal("pjournal_xend() failed");
	}
}

void
mdsfssync_init(void)
{
        lc_reginit(&dirtyMdsData, struct jflush_item, jfi_lentry,
		   "dirtyMdsData");
        pscthr_init(SLTHRT_MDSFSSYNC, 0, mdsfssyncthr_begin,
	    NULL, 0, "mdsfssyncthr");
}
