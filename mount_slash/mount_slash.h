/* $Id$ */

#include <sys/types.h>

#include <stdarg.h>

#include <fuse.h>

#include "psc_ds/tree.h"
#include "psc_mount/dhfh.h"

#include "psc_types.h"
#include "slconfig.h"
#include "fidcache.h"

struct fhent;
struct pscrpc_request;

#define MSTHRT_CTL	0
#define MSTHRT_FS	1
#define MSTHRT_RCM	1	/* service RPC reqs for client from MDS */
#define MSTHRT_LNET	2

#define MSL_IO_CB_POINTER_SLOT 1
#define MSL_WRITE_CB_POINTER_SLOT 2

#define MSL_READ 0
#define MSL_WRITE 1

extern sl_ios_id_t prefIOS;

struct msctl_thread {
	u32	mc_st_nclients;
	u32	mc_st_nsent;
	u32	mc_st_nrecv;
};

/* msl_fbr (mount slash fhent bmap ref).
 *
 */
struct msl_fbr {
	struct bmap_cache_memb *mfbr_bmap;    /* the bmap       */
	atomic_t                mfbr_acnt;    /* access counter */
	SPLAY_ENTRY(msl_fbr)    mfbr_tentry;
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
msl_fbr_new(struct bmap_cache_memb *b, int rw)
{
	struct msl_fbr *r = PSCALLOC(sizeof(*r));

	r->mfbr_bmap = b;
	msl_fbr_ref(r, rw);

	return (r);
}


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
	return (bmap_cache_cmp(((struct msl_fbr *)x)->mfbr_bmap, 
			       ((struct msl_fbr *)y)->mfbr_bmap));
}

SPLAY_HEAD(fhbmap_cache, msl_fbr);
SPLAY_PROTOTYPE(fhbmap_cache, msl_fbr, mfbr_tentry, fhbmap_cache_cmp);

struct msl_fhent {
	struct fidcache_memb_handle *mfh_fcmh;
	struct fhbmap_cache          mfh_fhbmap_cache;
};

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
fhcache_bmap_lookup(struct fhent *fh, struct bmap_cache_memb *b)
{
	struct msl_fhent *fhe=fh->fh_pri;
        struct msl_fbr *r=NULL, lr;
        int locked;

	lr.mfbr_bmap = b;
        locked = reqlock(&fh->fh_lock);
        r = SPLAY_FIND(fhbmap_cache, &fhe->mfh_fhbmap_cache, &lr);
	if (r)
		atomic_inc(&r->mfbr_acnt);
        ureqlock(&fh->fh_lock, locked);

        return (r);
}
 

struct msrcm_thread {
};

#define msctlthr(thr)	((struct msctl_thread *)(thr)->pscthr_private)

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

int msl_io(struct fhent *, char *, size_t, off_t, int);
int msl_io_cb(struct pscrpc_request *, void *, int);
int msl_dio_cb(struct pscrpc_request *, void *, int);

#define mds_import	(mds_csvc->csvc_import)

extern struct slashrpc_cservice *mds_csvc;
