/* $Id$ */

#include "psc_types.h"
#include "psc_ds/listcache.h"
#include "psc_util/assert.h"
#include "psc_util/cdefs.h"
#include "psc_util/journal.h"
#include "psc_util/thread.h"

#include "jflush.h"
#include "slashd.h"

struct psc_thread mdsFsSync;
list_cache_t dirtyMdsData;
struct psc_listcache dirtyInodes;

__dead __static void *
mdsfssyncthr_begin(__unusedx void *arg)
{
	struct jflush_item *jfi;
	struct psc_journal_xidhndl *xh;
	jflush_handler jfih;
	void *data;
	int type;
	
	while (1) {
		jfi = lc_getwait(&dirtyInodes);
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
		jfi->jfi_xh = NULL;		
		jfi->jfi_state &= ~JFI_QUEUED;
		jfi->jfi_state &= ~JFI_HAVE_XH;
		
		freelock(&jfi->jfi_lock);
		/* Now run the app-specific data flush code.
		 */
		psc_trace("fssync jfi(%p) xh(%p) xid(%"_P_U64"u) data(%p)", 
			  jfi, xh, xh->pjx_xid, data); 
		(jfih)(data);

		if (pjournal_xend(xh, PJET_VOID, NULL))
			psc_fatal("pjournal_xend() failed");
	}
}

void
mdsfssync_init(void)
{
        lc_reginit(&dirtyInodes, struct jflush_item, jfi_lentry, 
		   "dirtyMdsData");
        pscthr_init(&mdsFsSync, SLTHRT_MDSFSSYNC, mdsfssyncthr_begin,
                    NULL, "mdsfssyncthr");
}
