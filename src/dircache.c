/* $Id$ */

#include <sys/types.h>

#include <stdio.h>

#include "psc_ds/hash.h"
#include "psc_util/atomic.h"
#include "psc_util/list.h"
#include "psc_util/lock.h"
#include "psc_util/waitq.h"

#include "dircache.h"

atomic_t		 dircache_nents = ATOMIC_INIT(0);
struct list_head	 dircache_lru = PSCLIST_HEAD_INIT(&dircache);
psc_spinlock_t		 dircache_lru_lock = LOCK_INITIALIZER;
struct hash_table	 dircache;
psc_waitq_t		 dircache_wq;

void
dircache_init(void)
{
	hash_tbl_init(&dircache);
	psc_waitq_init(&dircache_wq);
}

void
dircache_move_lru(struct dircache *dc)
{
	if (psclist_first(&dircache_lru) == dc ||
	    dc->dc_flags & DCF_WANTDESTROY)
		return;
	spinlock(&dircache_lru_lock);
	if (psclist_next(&dc->dc_lruent))
		psclist_del(&dc->dc_lruent);
	else
		atomic_inc(&dircache_nents);
	psclist_xadd(&dc->dc_lruent, &dircache_lru);
	freelock(&dircache_lru_lock);
}

void
dircache_ref(void *arg)
{
	struct dircache *dc = arg;

	atomic_inc(&dc->dc_refcnt);
	dircache_move_lru(dc);
}

void
dircache_release(struct dircache *dc)
{
	atomic_dec(&dc->dc_refcnt);
	if (dc->dc_flags & DCF_WANTDESTROY)
		dircache_destroy(dc);
}

off_t
dircache_seek(struct dircache *dc, off_t offset)
{
	dircache_move_lru(dc);
	return (lseek(dc->dc_fd, offset * sizeof(struct dirent), SEEK_SET));
}

__static void
dircache_destroy(struct dircache *dc)
{
	close(dc->dc_fd);
	free(dc);
	psc_waitq_wakeup(&dircache_wq);
}

void
dircache_free(struct dircache *dc)
{
	dc->dc_flags |= DCF_WANTDESTROY;

	del_hash_entry(&dircache, &dc->dc_ent);

	spinlock(&dircache_lru_lock);
	psclist_del(&dircache_lru);
	atomic_dec(&dircache_nents);
	freelock(&dircache_lru_lock);

	if (atomic_read(&dc->dc_refcnt) == 0)
		dircache_destroy(dc);
}

void
dircache_reap(void)
{
	struct dircache *dc, *next;

	for (; atomic_read(&dircache_nents) > DIRCACHE_HIWAT; dc = next) {
		if (dc == NULL)
			dc = psclist_last_entry(&dircache_lru,
			    struct dircache, dc_lruent);
		next = psclist_prev(&dc->dc_lruent);
		if (atomic_read(dc->dc_refcnt) == 0)
			dircache_free(dc);
        }
}

__static struct dircache *
dircache_alloc(void)
{
	struct dircache *dc;

	for (;;) {
		if (atomic_read(&dircache_nents) < DIRCACHE_SIZE)
			return (PSCALLOC(sizeof(*dc)));
		dircache_reap();
		if (atomic_read(&dircache_nents) < DIRCACHE_SIZE)
			psc_waitq_wait(&dircache_wq, NULL);
	}
}

struct dircache *
dircache_get(const char *path)
{
	struct hash_entry_str *e;
	struct dircache *dc;

	e = get_hash_entry_str(&dircache, path, dircache_ref);
	if (e)
		goto found;
	LOCK_HASHTBL(&dircache);
	e = get_hash_entry_str(&dircache, path, dircache_ref);
	if (e) {
 found:
		dc = e->private;
		psc_assert((dc->dc_flags & DCF_WANTDESTROY) == 0);
	} else {
		dc = dircache_alloc();
		if ((dc->dc_fd = open(path, O_RDONLY)) == -1) {
			free(dc);
			ULOCK_HASHTBL(&dircache);
			return (NULL);
		}
		dircache_ref(dc);
		e = PSCALLOC(sizeof(*e));
		init_hash_entry_str(e, path, dc);
		add_hash_entry_str(&dircache, e);
	}
	ULOCK_HASHTBL(&dircache);
 done:
	return (e ? e->private : NULL);
}
