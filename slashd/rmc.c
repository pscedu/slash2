/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

/*
 * Routines for handling RPC requests for MDS from CLIENT.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/fsuid.h>
#include <sys/statvfs.h>
#include <sys/vfs.h>

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <unistd.h>

#include "pfl/str.h"
#include "psc_rpc/rpc.h"
#include "psc_rpc/rsx.h"
#include "psc_rpc/rpclog.h"
#include "psc_rpc/service.h"
#include "psc_util/lock.h"

#include "bmap_mds.h"
#include "fdbuf.h"
#include "fidc_mds.h"
#include "fidcache.h"
#include "mdsio.h"
#include "mkfn.h"
#include "pathnames.h"
#include "repl_mds.h"
#include "rpc_mds.h"
#include "slashd.h"
#include "slashrpc.h"
#include "slerr.h"
#include "slutil.h"

/*
 * The following SLASHIDs are automatically assigned:
 *	0	not used
 *	1	-> /
 */
#define SLASHID_MIN	2

/*
 * TODO: SLASH ID should be logged on disk, so that it can be advanced
 *	continuously across reboots and crashes.
 */
uint64_t next_slash_id = SLASHID_MIN;

uint64_t
slm_get_next_slashid(void)
{
	static psc_spinlock_t lock = LOCK_INITIALIZER;
	static int init;
	char fn[PATH_MAX];
	uint64_t slid;
	FILE *fp;

	/* XXX XXX disgusting XXX XXX */
	spinlock(&lock);
	if (!init) {
		xmkfn(fn, "%s/%s", sl_datadir, SL_FN_HACK_FID);

		fp = fopen(fn, "a+");
		if (fp == NULL)
			psc_fatal("%s", fn);
		fchmod(fileno(fp), 0600);
		fscanf(fp, "%"PRId64, &next_slash_id);
		fclose(fp);

		next_slash_id += 1000;

		init = 1;
	}

	if (next_slash_id >= (UINT64_C(1) << SLASH_ID_FID_BITS))
		next_slash_id = SLASHID_MIN;
	slid = next_slash_id++;

#if 0
	/* XXX XXX disgusting XXX XXX */
	if ((next_slash_id % 1000) == 0) {
#endif
		xmkfn(fn, "%s/%s", sl_datadir, SL_FN_HACK_FID);

		fp = fopen(fn, "r+");
		if (fp == NULL)
			psc_fatal("%s", fn);
		fprintf(fp, "%"PRId64, next_slash_id);
		ftruncate(fileno(fp), ftell(fp));
		fclose(fp);
#if 0
	}

#endif
	freelock(&lock);
	return (slid | ((uint64_t)nodeResm->resm_site->site_id <<
	    SLASH_ID_FID_BITS));
}

int
slm_rmc_handle_connect(struct pscrpc_request *rq)
{
	struct pscrpc_export *e=rq->rq_export;
	struct srm_connect_req *mq;
	struct srm_generic_rep *mp;
	struct mexp_cli *mexp_cli;

	RSX_ALLOCREP(rq, mq, mp);
	if (mq->magic != SRMC_MAGIC || mq->version != SRMC_VERSION)
		mp->rc = -EINVAL;
	/* XXX this assert will crash the mds should the client try to 
	 *  reconnect on his own.  Zhihui's namespace tester is causing this.
	 */
	psc_assert(e->exp_private == NULL);
	mexp_cli = mexpcli_get(e);
	slm_getclcsvc(e);
	return (0);
}

int
slm_rmc_handle_getattr(struct pscrpc_request *rq)
{
	const struct srm_getattr_req *mq;
	struct srm_getattr_rep *mp;
	struct fidc_membh *fcmh;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;
	mp->attr = fcmh->fcmh_sstb;
 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_getbmap(struct pscrpc_request *rq)
{
	struct slash_bmap_cli_wire *cw;
	struct pscrpc_bulk_desc *desc;
	struct bmap_mds_info *bmdsi;
	struct srt_bmapdesc_buf bdb;
	const struct srm_bmap_req *mq;
	struct srm_bmap_rep *mp;
	struct bmapc_memb *bmap;
	struct fidc_membh *fcmh;
	struct iovec iov[5];
	int niov;

	bmap = NULL;
	RSX_ALLOCREP(rq, mq, mp);

	if (mq->rw != SL_READ && mq->rw != SL_WRITE)
		return (-EINVAL);

	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		return (mp->rc);

	bmap = NULL;
	mp->rc = mds_bmap_load_cli(fcmh, mq, rq->rq_export, &bmap, mp);
	if (mp->rc)
		return (mp->rc);

	bmdsi = bmap->bcm_pri;
	cw = (struct slash_bmap_cli_wire *)bmap->bcm_od->bh_crcstates;

	niov = 2;
	iov[0].iov_base = cw;
	iov[0].iov_len = sizeof(*cw);
	iov[1].iov_base = &bdb;
	iov[1].iov_len = sizeof(bdb);

	if (mq->getreptbl) {
		struct slash_inode_handle *ih;

		niov++;
		ih = fcmh_2_inoh(bmap->bcm_fcmh);
		mp->nrepls = ih->inoh_ino.ino_nrepls;

		iov[2].iov_base = ih->inoh_ino.ino_repls;
		iov[2].iov_len = sizeof(sl_replica_t) * INO_DEF_NREPLS;

		if (mp->nrepls > INO_DEF_NREPLS) {
			niov++;
			mds_inox_ensure_loaded(ih);
			iov[3].iov_base = ih->inoh_extras->inox_repls;
			iov[3].iov_len = sizeof(ih->inoh_extras->inox_repls);
		}
	}

	mp->nblks = 1;

	if (mq->rw == SL_WRITE) {
		psc_assert(bmdsi->bmdsi_wr_ion);
		mp->ios_nid = bmdsi->bmdsi_wr_ion->rmmi_resm->resm_nid;
		bdbuf_sign(&bdb, &mq->fg, &rq->rq_peer, mp->ios_nid, 
			   bmdsi->bmdsi_wr_ion->rmmi_resm->resm_res->res_id, 
			   bmap->bcm_blkno, mp->seq, mp->key);

	} else {
		mp->ios_nid = LNET_NID_ANY;
		bdbuf_sign(&bdb, &mq->fg, &rq->rq_peer, LNET_NID_ANY, 
			   IOS_ID_ANY, bmap->bcm_blkno, mp->seq, mp->key);
	}

	mp->rc = rsx_bulkserver(rq, &desc, BULK_PUT_SOURCE,
	    SRMC_BULK_PORTAL, iov, niov);
	if (desc)
		pscrpc_free_bulk(desc);

	return (0);
}

int
slm_rmc_handle_link(struct pscrpc_request *rq)
{
	struct fidc_membh *p, *c;
	struct srm_link_req *mq;
	struct srm_link_rep *mp;

	p = c = NULL;
	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->fg, &c);
	if (mp->rc)
		goto out;
	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	mp->fg.fg_fid = slm_get_next_slashid();

	mq->name[sizeof(mq->name) - 1] = '\0';
	mp->rc = mdsio_link(fcmh_2_mdsio_fid(c), fcmh_2_mdsio_fid(p),
	    mq->name, &mp->fg, &mq->creds, &mp->attr);
 out:
	if (c)
		fcmh_op_done_type(c, FCMH_OPCNT_LOOKUP_FIDC);
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_lookup(struct pscrpc_request *rq)
{
	struct srm_lookup_req *mq;
	struct srm_lookup_rep *mp;
	struct fidc_membh *p;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	mq->name[sizeof(mq->name) - 1] = '\0';
	if (fcmh_2_mdsio_fid(p) == SL_ROOT_INUM &&
	    strncmp(mq->name, SL_PATH_PREFIX,
	     strlen(SL_PATH_PREFIX)) == 0) {
		mp->rc = EINVAL;
		goto out;
	}
	mp->rc = mdsio_lookup(fcmh_2_mdsio_fid(p),
	    mq->name, &mp->fg, NULL, &rootcreds, &mp->attr);

 out:
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);
	return (0);
}

int
slm_rmc_handle_mkdir(struct pscrpc_request *rq)
{
	struct srm_mkdir_req *mq;
	struct srm_mkdir_rep *mp;
	struct fidc_membh *fcmh;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->pfg, &fcmh);
	if (mp->rc)
		goto out;

	mp->fg.fg_fid = slm_get_next_slashid();

	mq->name[sizeof(mq->name) - 1] = '\0';
	mp->rc = mdsio_mkdir(fcmh_2_mdsio_fid(fcmh), mq->name,
	    mq->mode, &mq->creds, &mp->attr, &mp->fg, NULL);
 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (0);
}

int
slm_rmc_handle_create(struct pscrpc_request *rq)
{
	struct srm_create_rep *mp;
	struct srm_create_req *mq;
	struct fidc_membh *p;
	void *mdsio_data;

	p = NULL;

	RSX_ALLOCREP(rq, mq, mp);
	mq->name[sizeof(mq->name) - 1] = '\0';

	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	/* note that we don't create a cache entry after the creation */
	mp->fg.fg_fid = slm_get_next_slashid();

	mp->rc = mdsio_opencreate(fcmh_2_mdsio_fid(p), &mq->creds,
	    O_CREAT | O_EXCL | O_RDWR, mq->mode, mq->name, &mp->fg,
	    NULL, &mp->attr, &mdsio_data);
	//XXX fix me.  Place an fcmh into the cache and don't close 
	// my zfs handle.
	if (mp->rc == 0)
		mdsio_release(&rootcreds, mdsio_data);

 out:
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	return (0);
}

int
slm_rmc_handle_readdir(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_readdir_req *mq;
	struct srm_readdir_rep *mp;
	struct fidc_membh *fcmh;
	struct iovec iov[2];
	size_t outsize;
	int niov;

	RSX_ALLOCREP(rq, mq, mp);

	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out2;

	if (mq->size > MAX_READDIR_BUFSIZ ||
	    mq->nstbpref > MAX_READDIR_NENTS) {
		mp->rc = EINVAL;
		goto out2;
	}

	iov[0].iov_base = PSCALLOC(mq->size);
	iov[0].iov_len = mq->size;

	niov = 1;
	if (mq->nstbpref) {
		niov++;
		iov[1].iov_len = mq->nstbpref * sizeof(struct srm_getattr_rep);
		iov[1].iov_base = PSCALLOC(iov[1].iov_len);
	} else {
		iov[1].iov_len = 0;
		iov[1].iov_base = NULL;
	}

	mp->rc = mdsio_readdir(&rootcreds, mq->size, mq->offset,
	    iov[0].iov_base, &outsize, iov[1].iov_base, mq->nstbpref,
	    fcmh_2_mdsio_data(fcmh));
	mp->size = outsize;

	psc_info("mdsio_readdir: rc=%d, data=%p", mp->rc,
	    fcmh_2_mdsio_data(fcmh));

	if (mp->rc)
		goto out1;

#if 0
	{	
		/* debugging only */
		unsigned int i;
		struct srm_getattr_rep *attr;
		attr = iov[1].iov_base;
		for (i = 0; i < mq->nstbpref; i++, attr++) {
			if (attr->rc || !attr->attr.sst_ino)
				break;
			psc_info("reply: i+g:%"PRIx64"+%"PRIx32", mode=0%o", 
				attr->attr.sst_ino, attr->attr.sst_gen, 
				attr->attr.sst_mode);
		}
	}
#endif

	mp->rc = rsx_bulkserver(rq, &desc,
	    BULK_PUT_SOURCE, SRMC_BULK_PORTAL, iov, niov);

	if (desc)
		pscrpc_free_bulk(desc);

 out1:
	PSCFREE(iov[0].iov_base);
	PSCFREE(iov[1].iov_base);
 out2:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);
	return (mp->rc);
}

int
slm_rmc_handle_readlink(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_readlink_req *mq;
	struct srm_readlink_rep *mp;
	struct fidc_membh *fcmh;
	struct iovec iov;
	char buf[PATH_MAX];

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;

	mp->rc = mdsio_readlink(fcmh_2_mdsio_fid(fcmh), buf, &rootcreds);
	if (mp->rc)
		goto out;

	iov.iov_base = buf;
	iov.iov_len = sizeof(buf);
	mp->rc = rsx_bulkserver(rq, &desc, BULK_PUT_SOURCE,
	    SRMC_BULK_PORTAL, &iov, 1);
	if (desc)
		pscrpc_free_bulk(desc);

 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (mp->rc);
}

int
slm_rmc_handle_rls_bmap(struct pscrpc_request *rq)
{
	struct srm_bmap_release_req *mq;
	struct srm_bmap_release_rep *mp;
	struct slash_fidgen fg;
	struct fidc_membh *f;
	struct bmapc_memb *b;
	struct srm_bmap_id *bid;
	uint32_t i;

	RSX_ALLOCREP(rq, mq, mp);

	for (i=0; i < mq->nbmaps; i++) {
		bid = &mq->bmaps[i];
		
		fg.fg_fid = bid->fid; 
		fg.fg_gen = FIDGEN_ANY;

		mp->bidrc[i] = slm_fcmh_get(&fg, &f);
		if (mp->bidrc[i])
			continue;
		
		DEBUG_FCMH(PLL_INFO, f, "rls bmap=%u", bid->bmapno);

		mp->bidrc[i] = bmap_lookup(f, bid->bmapno, &b);
		if (mp->bidrc[i])
                        continue;
		
		DEBUG_BMAP(PLL_INFO, b, "release %"PRId64, bid->seq);

		mp->bidrc[i] = mds_bmap_bml_release(b, bid->seq, bid->key);
		if (mp->bidrc[i])
                        continue;
	}
	return (0);
}

int
slm_rmc_handle_rename(struct pscrpc_request *rq)
{
	char from[NAME_MAX+1], to[NAME_MAX+1];
	struct pscrpc_bulk_desc *desc;
	struct fidc_membh *op, *np;
	struct srm_generic_rep *mp;
	struct srm_rename_req *mq;
	struct iovec iov[2];

	op = np = NULL;
	RSX_ALLOCREP(rq, mq, mp);
	if (mq->fromlen <= 1 ||
	    mq->tolen <= 1) {
		mp->rc = -ENOENT;
		return (0);
	}
	if (mq->fromlen > NAME_MAX ||
	    mq->tolen > NAME_MAX) {
		mp->rc = -EINVAL;
		return (0);
	}

	mp->rc = slm_fcmh_get(&mq->opfg, &op);
	if (mp->rc)
		goto out;

	mp->rc = slm_fcmh_get(&mq->npfg, &np);
	if (mp->rc)
		goto out;

	iov[0].iov_base = from;
	iov[0].iov_len = mq->fromlen;
	iov[1].iov_base = to;
	iov[1].iov_len = mq->tolen;

	mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRMC_BULK_PORTAL, iov, 2);
	if (mp->rc)
		goto out;
	pscrpc_free_bulk(desc);

	from[sizeof(from) - 1] = '\0';
	to[sizeof(to) - 1] = '\0';
	mp->rc = mdsio_rename(fcmh_2_mdsio_fid(op), from,
	    fcmh_2_mdsio_fid(np), to, &mq->creds);
 out:
	if (np)
		fcmh_op_done_type(np, FCMH_OPCNT_LOOKUP_FIDC);
	if (op)
		fcmh_op_done_type(op, FCMH_OPCNT_LOOKUP_FIDC);
	return (mp->rc);
}

int
slm_rmc_handle_setattr(struct pscrpc_request *rq)
{
	struct srm_setattr_req *mq;
	struct srm_setattr_rep *mp;
	struct fidc_membh *fcmh;
	int to_set;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;

	to_set = mq->to_set;
	if (to_set & SRM_SETATTRF_SIZE) {
		to_set &= ~SRM_SETATTRF_SIZE;
		to_set |= SRM_SETATTRF_FSIZE;
		if (mq->attr.sst_size == 0) {
			/* full truncate */
		} else {
			to_set |= SRM_SETATTRF_PTRUNCGEN;
			/* partial truncate */
			fcmh_2_ptruncgen(fcmh)++;
		}
	}
	/* If the file is open, mdsio_data will be valid and used.
	 * Otherwise, it will be NULL, and we'll use the mdsio_fid.
	 */
	mp->rc = mdsio_setattr(fcmh_2_mdsio_fid(fcmh), &mq->attr, to_set,
	    &rootcreds, &mp->attr, fcmh_2_mdsio_data(fcmh));

 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (0);
}

int
slm_rmc_handle_set_newreplpol(struct pscrpc_request *rq)
{
	struct srm_set_newreplpol_req *mq;
	struct slash_inode_handle *ih;
	struct srm_generic_rep *mp;
	struct fidc_membh *fcmh;

	RSX_ALLOCREP(rq, mq, mp);

	if (mq->pol < 0 || mq->pol >= NBRP) {
		mp->rc = EINVAL;
		return (0);
	}

	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;

	ih = fcmh_2_inoh(fcmh);
	mp->rc = mds_inox_ensure_loaded(ih);
	if (mp->rc == 0) {
		ih->inoh_extras->inox_newbmap_policy = mq->pol;
		ih->inoh_flags |= INOH_EXTRAS_DIRTY;
		mds_inode_sync(ih);
	}

 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (0);
}

int
slm_rmc_handle_set_bmapreplpol(struct pscrpc_request *rq)
{
	struct srm_set_bmapreplpol_req *mq;
	struct bmap_mds_info *bmdsi;
	struct srm_generic_rep *mp;
	struct fcmh_mds_info *fmi;
	struct fidc_membh *fcmh;
	struct bmapc_memb *bcm;

	fmi = NULL;

	RSX_ALLOCREP(rq, mq, mp);

	if (mq->pol < 0 || mq->pol >= NBRP) {
		mp->rc = EINVAL;
		return (0);
	}

	mp->rc = slm_fcmh_get(&mq->fg, &fcmh);
	if (mp->rc)
		goto out;

	fmi = fcmh_2_fmi(fcmh);

	if (!mds_bmap_exists(fcmh, mq->bmapno)) {
		mp->rc = SLERR_BMAP_INVALID;
		goto out;
	}
	mp->rc = mds_bmap_load(fcmh, mq->bmapno, &bcm);
	if (mp->rc)
		goto out;

	BMAP_LOCK(bcm);
	bmdsi = bmap_2_bmdsi(bcm);
	bcm->bcm_od->bh_repl_policy = mq->pol;
	bmdsi->bmdsi_flags |= BMIM_LOGCHG;
	mds_repl_bmap_rel(bcm);

 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (0);
}

int
slm_rmc_handle_statfs(struct pscrpc_request *rq)
{
	struct srm_statfs_req *mq;
	struct srm_statfs_rep *mp;
	struct statvfs sfb;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = mdsio_statfs(&sfb);
	sl_externalize_statfs(&sfb, &mp->ssfb);
	return (0);
}

int
slm_rmc_handle_symlink(struct pscrpc_request *rq)
{
	struct pscrpc_bulk_desc *desc;
	struct srm_symlink_req *mq;
	struct srm_symlink_rep *mp;
	struct fidc_membh *p;
	struct iovec iov;
	char linkname[PATH_MAX];

	p = NULL;

	RSX_ALLOCREP(rq, mq, mp);
	mq->name[sizeof(mq->name) - 1] = '\0';
	if (mq->linklen == 0) {
		mp->rc = ENOENT;
		goto out;
	}
	if (mq->linklen >= PATH_MAX) {
		mp->rc = ENAMETOOLONG;
		goto out;
	}

	mp->rc = slm_fcmh_get(&mq->pfg, &p);
	if (mp->rc)
		goto out;

	iov.iov_base = linkname;
	iov.iov_len = mq->linklen;
	mp->rc = rsx_bulkserver(rq, &desc, BULK_GET_SINK,
	    SRMC_BULK_PORTAL, &iov, 1);
	if (mp->rc)
		goto out;
	pscrpc_free_bulk(desc);

	mp->fg.fg_fid = slm_get_next_slashid();

	linkname[sizeof(linkname) - 1] = '\0';
	mp->rc = mdsio_symlink(linkname, fcmh_2_mdsio_fid(p), mq->name,
	    &mq->creds, &mp->attr, &mp->fg, NULL);
 out:
	if (p)
		fcmh_op_done_type(p, FCMH_OPCNT_LOOKUP_FIDC);

	return (mp->rc);
}

int
slm_rmc_handle_unlink(struct pscrpc_request *rq, int isfile)
{
	struct srm_unlink_req *mq;
	struct srm_unlink_rep *mp;
	struct fidc_membh *fcmh;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = slm_fcmh_get(&mq->pfg, &fcmh);
	if (mp->rc)
		goto out;

	mq->name[sizeof(mq->name) - 1] = '\0';
	if (isfile)
		mp->rc = mdsio_unlink(fcmh_2_mdsio_fid(fcmh),
		    mq->name, &rootcreds);
	else
		mp->rc = mdsio_rmdir(fcmh_2_mdsio_fid(fcmh),
		    mq->name, &rootcreds);

	psc_info("mdsio_unlink: name = %s, rc=%d, data=%p", mq->name, mp->rc,
	    fcmh_2_mdsio_data(fcmh));
 out:
	if (fcmh)
		fcmh_op_done_type(fcmh, FCMH_OPCNT_LOOKUP_FIDC);

	return (0);
}

int
slm_rmc_handle_addreplrq(struct pscrpc_request *rq)
{
	struct srm_generic_rep *mp;
	struct srm_replrq_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = mds_repl_addrq(&mq->fg,
	    mq->bmapno, mq->repls, mq->nrepls);
	return (0);
}

int
slm_rmc_handle_delreplrq(struct pscrpc_request *rq)
{
	struct srm_generic_rep *mp;
	struct srm_replrq_req *mq;

	RSX_ALLOCREP(rq, mq, mp);
	mp->rc = mds_repl_delrq(&mq->fg,
	    mq->bmapno, mq->repls, mq->nrepls);
	return (0);
}

int
slm_rmc_handle_getreplst(struct pscrpc_request *rq)
{
	struct srm_replst_master_req *mq;
	struct srm_replst_master_rep *mp;
	struct slm_replst_workreq *rsw;

	RSX_ALLOCREP(rq, mq, mp);

	rsw = PSCALLOC(sizeof(*rsw));
	rsw->rsw_fg = mq->fg;
	rsw->rsw_cid = mq->id;
	rsw->rsw_csvc = slm_getclcsvc(rq->rq_export);
	lc_add(&slm_replst_workq, rsw);
	return (0);
}

int
slm_rmc_handler(struct pscrpc_request *rq)
{
	int rc = 0;

	switch (rq->rq_reqmsg->opc) {
	case SRMT_REPL_ADDRQ:
		rc = slm_rmc_handle_addreplrq(rq);
		break;
	case SRMT_CONNECT:
		rc = slm_rmc_handle_connect(rq);
		break;
	case SRMT_CREATE:
		rc = slm_rmc_handle_create(rq);
		break;
	case SRMT_REPL_DELRQ:
		rc = slm_rmc_handle_delreplrq(rq);
		break;
	case SRMT_GETATTR:
		rc = slm_rmc_handle_getattr(rq);
		break;
	case SRMT_GETBMAP:
		rc = slm_rmc_handle_getbmap(rq);
		break;
	case SRMT_REPL_GETST:
		rc = slm_rmc_handle_getreplst(rq);
		break;
	case SRMT_LINK:
		rc = slm_rmc_handle_link(rq);
		break;
	case SRMT_MKDIR:
		rc = slm_rmc_handle_mkdir(rq);
		break;
	case SRMT_LOOKUP:
		rc = slm_rmc_handle_lookup(rq);
		break;
	case SRMT_READDIR:
		rc = slm_rmc_handle_readdir(rq);
		break;
	case SRMT_READLINK:
		rc = slm_rmc_handle_readlink(rq);
		break;
	case SRMT_RELEASEBMAP:
		rc = slm_rmc_handle_rls_bmap(rq);
		break;
	case SRMT_RENAME:
		rc = slm_rmc_handle_rename(rq);
		break;
	case SRMT_RMDIR:
		rc = slm_rmc_handle_unlink(rq, 0);
		break;
	case SRMT_SETATTR:
		rc = slm_rmc_handle_setattr(rq);
		break;
	case SRMT_SET_NEWREPLPOL:
		rc = slm_rmc_handle_set_newreplpol(rq);
		break;
	case SRMT_SET_BMAPREPLPOL:
		rc = slm_rmc_handle_set_bmapreplpol(rq);
		break;
	case SRMT_STATFS:
		rc = slm_rmc_handle_statfs(rq);
		break;
	case SRMT_SYMLINK:
		rc = slm_rmc_handle_symlink(rq);
		break;
	case SRMT_UNLINK:
		rc = slm_rmc_handle_unlink(rq, 1);
		break;
	default:
		psc_errorx("Unexpected opcode %d", rq->rq_reqmsg->opc);
		rq->rq_status = -ENOSYS;
		return (pscrpc_error(rq));
	}
	target_send_reply_msg(rq, rc, 0);
	return (rc);
}
