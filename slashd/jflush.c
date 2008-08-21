#include "psc_util/lock.h"
#include "psc_util/assert.h"
#include "psc_ds/listcache.h"
#include "psc_util/journal.h"

#include "jflush.h"

void
jfi_prep(struct jflush_item *jfi, struct psc_journal *pj)
{
        spinlock(&jfi->jfi_lock);
        if (jfi->jfi_state & JFI_HAVE_XH) {
                psc_assert(jfi->jfi_xh);
                if (jfi->jfi_state & JFI_QUEUED)
                        psc_assert(psclist_conjoint(&jfi->jfi_lentry));
        } else {
                psc_assert(!(jfi->jfi_state & JFI_QUEUED));
                psc_assert(psclist_disjoint(&jfi->jfi_lentry));
                jfi->jfi_xh = pjournal_nextxid(pj);
                jfi->jfi_state |= JFI_HAVE_XH;
        }
        freelock(&jfi->jfi_lock);
}

void
jfi_schedule(struct jflush_item *jfi, list_cache_t *lc)
{
        spinlock(&jfi->jfi_lock);
        psc_assert(jfi->jfi_xh && (jfi->jfi_state & JFI_HAVE_XH));
        if (jfi->jfi_state & JFI_QUEUED)
                psc_assert(psclist_conjoint(&jfi->jfi_lentry));
        else {
                psc_assert(psclist_disjoint(&jfi->jfi_lentry));
                jfi->jfi_state &= ~JFI_QUEUED;
                lc_queue(lc, &jfi->jfi_lentry);
        }
        freelock(&jfi->jfi_lock);
}

