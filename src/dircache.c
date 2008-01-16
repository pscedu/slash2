/* $Id$ */

#include <sys/types.h>
#include <sys/syscall.h>

#include <fcntl.h>
#include <stdio.h>
#include <unistd.h>

#include "psc_types.h"
#include "psc_ds/hash.h"
#include "psc_ds/list.h"
#include "psc_util/atomic.h"
#include "psc_util/alloc.h"
#include "psc_util/lock.h"
#include "psc_util/waitq.h"

#include "dircache.h"
#include "fid.h"

atomic_t		 dircache_nents = ATOMIC_INIT(0);
struct psclist_head	 dircache_lru = PSCLIST_HEAD_INIT(dircache_lru);
psc_spinlock_t		 dircache_lru_lock = LOCK_INITIALIZER;
struct hash_table	 dircache;
psc_waitq_t		 dircache_wq;

void
dircache_init(void)
{
	init_hash_table(&dircache, 2 * DIRCACHE_SIZE / 3, "dircache");
	psc_waitq_init(&dircache_wq);
}

__static void
dircache_move_lru(struct dircache *dc)
{
	if (psclist_first_entry(&dircache_lru,
	    struct dircache, dc_lruent) == dc ||
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

__static void
dircache_destroy(struct dircache *dc)
{
	close(dc->dc_fd);
	free(dc);
	psc_waitq_wakeup(&dircache_wq);
}

void
dircache_rel(struct dircache *dc)
{
	atomic_dec(&dc->dc_refcnt);
	if (dc->dc_flags & DCF_WANTDESTROY)
		dircache_destroy(dc);
}

void
dircache_free(struct dircache *dc)
{
	dc->dc_flags |= DCF_WANTDESTROY;

	del_hash_entry(&dircache, *dc->dc_hent.hentry_id);

	spinlock(&dircache_lru_lock);
	psclist_del(&dircache_lru);
	atomic_dec(&dircache_nents);
	freelock(&dircache_lru_lock);

	if (atomic_read(&dc->dc_refcnt) == 0)
		dircache_destroy(dc);
}

__static void
dircache_reap(void)
{
	struct dircache *dc, *next;

	for (; atomic_read(&dircache_nents) > DIRCACHE_HIWAT; dc = next) {
		if (dc == NULL)
			dc = psclist_last_entry(&dircache_lru,
			    struct dircache, dc_lruent);
		next = psclist_prev_entry(&dc->dc_lruent,
		    struct dircache, dc_lruent);
		if (atomic_read(&dc->dc_refcnt) == 0)
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
dircache_get(slash_fid_t *fidp)
{
	struct hash_entry *e;
	struct dircache *dc;
	char fn[PATH_MAX];
	int fd;

	e = get_hash_entry(&dircache, fidp->fid_inum, NULL, dircache_ref);
	if (e) {
		dc = e->private;
		psc_assert((dc->dc_flags & DCF_WANTDESTROY) == 0);
		return (e->private);
	}
	spinlock(&dircache.htable_lock);
	e = get_hash_entry(&dircache, fidp->fid_inum, NULL, dircache_ref);
	if (e) {
		dc = e->private;
		psc_assert((dc->dc_flags & DCF_WANTDESTROY) == 0);
	} else {
		if (fid_makepath(fidp, fn))
			goto done;
		if ((fd = open(fn, O_RDONLY)) == -1)
			goto done;
		dc = dircache_alloc();
		LOCK_INIT(&dc->dc_lock);
		dc->dc_fd = fd;
		dc->dc_fid = *fidp;
		dircache_ref(dc);
		e = PSCALLOC(sizeof(*e));
		init_hash_entry(e, &dc->dc_fid.fid_inum, dc);
		add_hash_entry(&dircache, e);
	}
 done:
	freelock(&dircache.htable_lock);
	return (e ? e->private : NULL);
}

int
dircache_read(struct dircache *dc, int off, struct dirent *buf, int n)
{
	off_t pos;
	int rc;

	dircache_move_lru(dc);
	spinlock(&dc->dc_lock);
	pos = lseek(dc->dc_fd, off * sizeof(*buf), SEEK_SET);
	if (pos == (off_t)-1) {
		rc = -1;
		goto done;
	}
	rc = syscall(SYS_getdents, dc->dc_fd, buf, n);
 done:
	freelock(&dc->dc_lock);
	return (rc);
}
