/* $Id$ */

#include <sys/types.h>

#include <dirent.h>

#include "psc_ds/hash.h"
#include "psc_ds/list.h"
#include "psc_util/atomic.h"

#include "fid.h"

struct dircache {
	struct hash_entry	 dc_hent;	/* hash table entry */
	struct psclist_head	 dc_lruent;	/* LRU list entry */
	slfid_t			 dc_fid;	/* file ID */
	int			 dc_flags;	/* operation flags */
	atomic_t		 dc_refcnt;	/* how many are using us */
	psc_spinlock_t		 dc_lock;	/* exclusitivity control */

	int			 dc_fd;		/* underlying directory filedes */
};

/* dircache entry flags */
#define DCF_WANTDESTROY	(1<<1)	/* dc entry should vanish */

#define DIRCACHE_SIZE	128	/* max #entries */
#define DIRCACHE_HIWAT	96	/* upper bound for reaping */

void	dircache_init(void);
void	dircache_ref(void *);
void	dircache_rel(struct dircache *);
void	dircache_free(struct dircache *);
int	dircache_read(struct dircache *, int, struct dirent *, int);
struct dircache *
	dircache_get(slfid_t);
