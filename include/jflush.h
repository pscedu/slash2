/* $Id$ */

#ifndef _SL_JFLUSH_
#define _SL_JFLUSH_

#include "psc_util/journal.h"

#include "sljournal.h"

typedef void (*jflush_handler)(void *);

struct jflush_item {
	psc_spinlock_t              jfi_lock;
	struct psc_journal_xidhndl *jfi_xh;
	struct psclist_head         jfi_lentry;
	jflush_handler              jfi_handler;
	void                       *jfi_data;
	int                         jfi_state;
	int                         jfi_type;
};

enum jfi_states {
	JFI_QUEUED  = (1<<0),
	JFI_HAVE_XH = (1<<1),
	JFI_BUSY    = (1<<2)
};

static inline void
jfi_init(struct jflush_item *j, jflush_handler handler, void *data)
{
	LOCK_INIT(&j->jfi_lock);
	INIT_PSCLIST_ENTRY(&j->jfi_lentry);
	j->jfi_handler = handler;
	j->jfi_data = data;
	j->jfi_state = 0;
}

void jfi_prep(struct jflush_item *, struct psc_journal *);
void jfi_schedule(struct jflush_item *, list_cache_t *);

#endif /* _SL_JFLUSH_ */
