/* $Id$ */

#include <sys/types.h>

#include <dirent.h>

#include "psc_ds/hash.h"
#include "psc_ds/list.h"
#include "psc_util/atomic.h"

struct dircache {
	struct hash_entry	 dc_hent;	/* hash table entry */
	struct list_entry	 dc_lruent;	/* LRU list entry */
	int			 dc_flags;
	atomic_t		 dc_refcnt;

	int			 dc_fd;		/* underlying directory filedes */
};

#define DCF_WANTDESTROY	(1<<1)

#define DIRCACHE_SIZE	128	/* max #entries */
#define DIRCACHE_HIWAT	96	/* upper bound for reaping */

int getdents(int, struct dirent *, unsigned int);
#define dircache_read(dc, dirp, n) getdents(dc->dc_fd, dirp, n);

void	dircache_init(void);
void	dircache_ref(void *);
void	dircache_rel(struct dircache *);
off_t	dircache_seek(struct dircache *, off_t);
void	dircache_free(struct dircache *);
struct dircache *
	dircache_get(const char *);
