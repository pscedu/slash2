/* $Id$ */

#include <sys/types.h>
#include <stdarg.h>

#include "psc_types.h"
#include "psc_ds/tree.h"
#include "psc_mount/dhfh.h"
#include "psc_rpc/service.h"

#include "slconfig.h"
#include "fidcache.h"

struct fhent;
struct pscrpc_request;

#define MSTHRT_CTL	0	/* control interface */
#define MSTHRT_FS	1	/* fuse filesystem syscall handlers */
#define MSTHRT_RCM	2	/* service RPC reqs for client from MDS */
#define MSTHRT_LNETAC	3	/* lustre net accept thr */
#define MSTHRT_USKLNDPL	4	/* userland socket lustre net dev poll thr */
#define MSTHRT_EQPOLL	5	/* LNET event queue polling */
#define MSTHRT_TINTV	6	/* timer interval thread */
#define MSTHRT_TIOS	7	/* timer iostat updater */
#define MSTHRT_FUSE	8	/* fuse internal manager */

#define MSL_IO_CB_POINTER_SLOT 1
#define MSL_WRITE_CB_POINTER_SLOT 2

#define MSL_READ 0
#define MSL_WRITE 1

extern sl_ios_id_t prefIOS;

struct msrcm_thread {
	struct pscrpc_thread	 mrcm_prt;
};

struct msfs_thread {
};

/* msl_fbr (mount slash fhent bmap ref).
 *
 */
struct msl_fbr {
	struct bmapc_memb	*mfbr_bmap;    /* the bmap       */
	atomic_t		 mfbr_acnt;    /* access counter */
	SPLAY_ENTRY(msl_fbr)	 mfbr_tentry;
};

static inline void
msl_fbr_ref(struct msl_fbr *r, int rw)
{
	psc_assert(r->mfbr_bmap);

	if (rw & FHENT_READ)
                atomic_inc(&r->mfbr_bmap->bcm_rd_ref);
        if (rw & FHENT_WRITE)
                atomic_inc(&r->mfbr_bmap->bcm_wr_ref);
}

static inline struct msl_fbr *
msl_fbr_new(struct bmapc_memb *b, int rw)
{
	struct msl_fbr *r = PSCALLOC(sizeof(*r));

	r->mfbr_bmap = b;
	msl_fbr_ref(r, rw);

	return (r);
}

/* XXX this is never called */
static inline void
msl_fbr_free(struct msl_fbr *r, struct fhent *f)
{
	psc_assert(r->mfbr_bmap);

	if (f->fh_state & FHENT_READ)
		atomic_dec(&r->mfbr_bmap->bcm_rd_ref);
	if (f->fh_state & FHENT_WRITE)
		atomic_dec(&r->mfbr_bmap->bcm_wr_ref);

	PSCFREE(r);
}

static inline int
fhbmap_cache_cmp(const void *x, const void *y)
{
	const struct msl_fbr *rx = x, *ry = y;

	return (bmapc_cmp(rx->mfbr_bmap, ry->mfbr_bmap));
}

SPLAY_HEAD(fhbmap_cache, msl_fbr);
SPLAY_PROTOTYPE(fhbmap_cache, msl_fbr, mfbr_tentry, fhbmap_cache_cmp);

struct msl_fhent {
	psc_spinlock_t         mfh_lock;
	struct fidc_membh     *mfh_fcmh;
	struct fhbmap_cache    mfh_fhbmap_cache;
};

static inline u64
mslfh_2_cfd(struct msl_fhent *mfh)
{
	psc_assert(mfh->mfh_fcmh);
	psc_assert(mfh->mfh_fcmh->fcmh_fcoo);
	return (mfh->mfh_fcmh->fcmh_fcoo->fcoo_cfd);
}

static inline size_t
mslfh_2_bmapsz(struct msl_fhent *mfh)
{
	psc_assert(mfh->mfh_fcmh);
	psc_assert(mfh->mfh_fcmh->fcmh_fcoo);
	return (mfh->mfh_fcmh->fcmh_fcoo->fcoo_bmap_sz);
}

static inline u64
fcmh_2_cfd(struct fidc_membh *f)
{
	psc_assert(f->fcmh_fcoo);
	return (f->fcmh_fcoo->fcoo_cfd);
}

static inline int
msl_fuse_2_oflags(int fuse_flags)
{
	int oflag = -1;

	if (fuse_flags & O_RDONLY)
		oflag = FHENT_READ;

	else if (fuse_flags & O_WRONLY) {
		psc_assert(oflag == -1);
		oflag = FHENT_WRITE;

	} else if (fuse_flags & O_RDWR) {
		psc_assert(oflag == -1);
		oflag = FHENT_WRITE | FHENT_READ;

	} else
		psc_fatalx("Invalid fuse_flag %d", fuse_flags);

	return (oflag);
}

static inline struct msl_fbr *
fhcache_bmap_lookup(struct msl_fhent *mfh, struct bmapc_memb *b)
{
        struct msl_fbr *r=NULL, lr;
        int locked;

	lr.mfbr_bmap = b;
        locked = reqlock(&mfh->mfh_lock);
        r = SPLAY_FIND(fhbmap_cache, &mfh->mfh_fhbmap_cache, &lr);
	if (r)
		atomic_inc(&r->mfbr_acnt);
        ureqlock(&mfh->mfh_lock, locked);

        return (r);
}

struct io_server_conn {
	struct psclist_head		 isc_lentry;
	struct slashrpc_cservice	*isc_csvc;
};

void rpc_initsvc(void);
int msrmc_connect(const char *);
int msric_connect(const char *);
int msrcm_handler(struct pscrpc_request *);

void *msctlthr_begin(void *);

struct slashrpc_cservice *ion_get(void);

void msl_fdreg_cb(struct fhent *, int, void *[]);

#define msl_read(fh, buf, size, off)  msl_io(fh, buf, size, off, MSL_READ)
#define msl_write(fh, buf, size, off) msl_io(fh, buf, size, off, MSL_WRITE)

int msl_io(struct msl_fhent *, char *, size_t, off_t, int);
int msl_io_cb(struct pscrpc_request *, void *, int);
int msl_dio_cb(struct pscrpc_request *, void *, int);

void mseqpollthr_spawn(void);
void msctlthr_spawn(void);
void mstimerthr_spawn(void);

#define mds_import	(mds_csvc->csvc_import)

extern struct slashrpc_cservice *mds_csvc;
extern char ctlsockfn[];
