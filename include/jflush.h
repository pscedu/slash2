#ifndef __SL_JFLUSH__
#define __SL_JFLUSH__ 1

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
	JFI_HAVE_XH = (1<<1)
};

#endif
