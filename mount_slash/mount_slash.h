/* $Id$ */

#ifndef _MOUNT_SLASH_H_
#define _MOUNT_SLASH_H_

#include <sys/types.h>

#include <stdarg.h>

#include "psc_ds/tree.h"
#include "psc_rpc/service.h"

#include "bmap.h"
#include "fidcache.h"
#include "inode.h"
#include "msl_fuse.h"
#include "offtree.h"
#include "slconfig.h"

struct pscrpc_request;

/* thread types */
#define MSTHRT_CTL	0	/* control interface */
#define MSTHRT_FS	1	/* fuse filesystem syscall handlers */
#define MSTHRT_RCM	2	/* service RPC reqs for client from MDS */
#define MSTHRT_LNETAC	3	/* lustre net accept thr */
#define MSTHRT_USKLNDPL	4	/* userland socket lustre net dev poll thr */
#define MSTHRT_EQPOLL	5	/* LNET event queue polling */
#define MSTHRT_TINTV	6	/* timer interval thread */
#define MSTHRT_TIOS	7	/* timer iostat updater */
#define MSTHRT_FUSE	8	/* fuse internal manager */
#define MSTHRT_BMAPFLSH	9	/* async buffer thread */

/* async RPC pointers */
#define MSL_IO_CB_POINTER_SLOT 1
#define MSL_WRITE_CB_POINTER_SLOT 2
#define MSL_OFTRQ_CB_POINTER_SLOT 3

/* I/O flags */
#define MSL_READ 0
#define MSL_WRITE 1

/* file handle entry access flags */
#define FHENT_READ	(1 << 0)
#define FHENT_WRITE	(1 << 1)

struct msrcm_thread {
	struct pscrpc_thread	 mrcm_prt;
};

struct msfs_thread {
};

/* msl_fbr (mount_slash fhent bmap ref).
 *
 */
struct msl_fbr {
	struct bmapc_memb		*mfbr_bmap;
	atomic_t			 mfbr_wr_ref;
	atomic_t			 mfbr_rd_ref;
	SPLAY_ENTRY(msl_fbr)		 mfbr_tentry;
};

SPLAY_HEAD(fhbmap_cache, msl_fbr);

struct msl_fhent {			 /* XXX rename */
	psc_spinlock_t			 mfh_lock;
	struct fidc_membh		*mfh_fcmh;
	struct fhbmap_cache		 mfh_fhbmap_cache;
};

struct io_server_conn {
	struct psclist_head		 isc_lentry;
	struct slashrpc_cservice	*isc_csvc;
};

void	rpc_initsvc(void);
int	msrmc_connect(const char *);
int	msric_connect(const char *);
int	msrcm_handler(struct pscrpc_request *);

void	*msctlthr_begin(void *);

struct slashrpc_cservice *ion_get(void);

#define msl_read(fh, buf, size, off)  msl_io((fh), (buf), (size), (off), MSL_READ)
#define msl_write(fh, buf, size, off) msl_io((fh), (buf), (size), (off), MSL_WRITE)

struct pscrpc_import *
	msl_bmap_to_import(struct bmapc_memb *, int);
void	msl_bmap_fhcache_clear(struct msl_fhent *);
int	msl_dio_cb(struct pscrpc_request *, struct pscrpc_async_args *);
void	msl_init(void);
int	msl_io(struct msl_fhent *, char *, size_t, off_t, int);
int	msl_io_cb(struct pscrpc_request *, struct pscrpc_async_args *);
int	msl_io_rpc_cb(struct pscrpc_request *, struct pscrpc_async_args *);
int	msl_io_rpcset_cb(struct pscrpc_request_set *, void *, int);

struct msl_fhent *
	msl_fhent_new(struct fidc_membh *);

void	mseqpollthr_spawn(void);
void	msctlthr_spawn(void);
void	mstimerthr_spawn(void);

int	slash_lookup_cache(const struct slash_creds *, fuse_ino_t, const char *,
	    struct slash_fidgen *, struct stat *);

int	checkcreds(const struct stat *, const struct slash_creds *, int);
int	translate_pathname(const char *, char []);

#define mds_import	(mds_csvc->csvc_import)

extern struct slashrpc_cservice *mds_csvc;
extern char ctlsockfn[];
extern sl_ios_id_t prefIOS;

static inline void
msl_fbr_ref(struct msl_fbr *r, int rw)
{
	psc_assert(r->mfbr_bmap);

	if (rw == FHENT_READ) {
		atomic_inc(&r->mfbr_bmap->bcm_rd_ref);
		atomic_inc(&r->mfbr_rd_ref);

	} else if (rw == FHENT_WRITE) {
		atomic_inc(&r->mfbr_bmap->bcm_wr_ref);
		atomic_inc(&r->mfbr_wr_ref);
	} else
		abort();
}


static inline void
msl_fbr_unref(const struct msl_fbr *r)
{
	psc_assert(r->mfbr_bmap);
	atomic_sub(atomic_read(&r->mfbr_rd_ref), &r->mfbr_bmap->bcm_rd_ref);
	atomic_sub(atomic_read(&r->mfbr_wr_ref), &r->mfbr_bmap->bcm_wr_ref);
}


static inline struct msl_fbr *
msl_fbr_new(struct bmapc_memb *b, int rw)
{
	struct msl_fbr *r = PSCALLOC(sizeof(*r));

	r->mfbr_bmap = b;
	msl_fbr_ref(r, rw);

	return (r);
}

static inline int
fhbmap_cache_cmp(const void *x, const void *y)
{
	const struct msl_fbr *rx = x, *ry = y;

	return (bmapc_cmp(rx->mfbr_bmap, ry->mfbr_bmap));
}

static inline struct srt_fd_buf *
mslfh_2_fdb(struct msl_fhent *mfh)
{
	psc_assert(mfh->mfh_fcmh);
	psc_assert(mfh->mfh_fcmh->fcmh_fcoo);
	return (&mfh->mfh_fcmh->fcmh_fcoo->fcoo_fdb);
}

static inline size_t
mslfh_2_bmapsz(struct msl_fhent *mfh)
{
	psc_assert(mfh->mfh_fcmh);
	psc_assert(mfh->mfh_fcmh->fcmh_fcoo);
	psc_assert(mfh->mfh_fcmh->fcmh_fcoo->fcoo_bmap_sz);
	return (mfh->mfh_fcmh->fcmh_fcoo->fcoo_bmap_sz);
}

static inline struct srt_fd_buf *
fcmh_2_fdb(struct fidc_membh *f)
{
	return (&f->fcmh_fcoo->fcoo_fdb);
}

SPLAY_PROTOTYPE(fhbmap_cache, msl_fbr, mfbr_tentry, fhbmap_cache_cmp);

static inline struct msl_fbr *
fhcache_bmap_lookup(struct msl_fhent *mfh, struct bmapc_memb *b)
{
	struct msl_fbr *r, lr;
	int locked;

	lr.mfbr_bmap = b;

	locked = reqlock(&mfh->mfh_lock);
	r = SPLAY_FIND(fhbmap_cache, &mfh->mfh_fhbmap_cache, &lr);
	ureqlock(&mfh->mfh_lock, locked);
	return (r);
}

#endif /* _MOUNT_SLASH_H_ */
