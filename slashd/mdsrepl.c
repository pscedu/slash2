/* $Id$ */

#include <errno.h>

#include "inode.h"
#include "inodeh.h"
#include "fid.h"
#include "fidcache.h"
#include "mdsexpc.h"


#if 0
__static int 
mds_repl_xattr_load_locked(struct slash_inode_handle *i)
{
	char fidfn[FID_MAX_PATH];
        size_t sz;
	int rc;

	DEBUG_INOH(PLL_INFO, i, "trying to load replica table");

	INOH_LOCK_ENSURE(i);
	psc_assert(i->inoh_ino.ino_nrepls);
	psc_assert(!i->inoh_replicas);
	psc_assert(!(i->inoh_flags & INOH_HAVE_REPS));

	fid_makepath(i->inoh_ino.ino_fg.fg_fid, fidfn);
	sz = (sizeof(sl_replica_t) * i->inoh_ino.ino_nrepls);

	if (fid_getxattr(fidfn, SFX_REPLICAS,  i->inoh_replicas, sz)) {
		psc_warnx("fid_getxattr failed to get %s", SFX_REPLICAS);
		rc = -errno;
		goto fail;

	} else if (mds_repl_crc_check(i)) {
		rc = -EIO;
		goto fail;

	} else {
		i->inoh_flags |= INOH_HAVE_REPS;		
		DEBUG_INOH(PLL_INFO, i, "replica table loaded");
	}
	return (0);
	
 fail:
	DEBUG_INOH(PLL_INFO, i, "replica table load failed");
	return (rc);
}


int
mds_repl_load_locked(struct slash_inode_handle *i)
{
	return (0);
}

int 
mds_repl_ios_lookup(struct slash_inode_handle *i, sl_ios_id_t ios, int add)
{
	u32 j;
	int rc=-ENOENT;

	INOH_LOCK(i);
	if (!i->inoh_ino.ino_nrepls)
		goto out;

	else if (!(i->inoh_flags & INOH_HAVE_REPS)) {
		if ((rc = mds_repl_load_locked(i)) != 0)
			goto out;
	}
	psc_assert(i->inoh_replicas);

	for (j=0; j < i->inoh_ino.ino_nrepls; j++) {
		if (i->inoh_replicas[j].bs_id == ios) {
			rc = j;
			goto out;
		}
	}

	if (rc == -ENOENT && add) {
		if (i->inoh_ino.ino_nrepls >= SL_MAX_REPLICAS) {
			DEBUG_INOH(PLL_WARN, i, "too many replicas");
			rc = -ENOSPC;

		} else {
			DEBUG_INOH(PLL_INFO, i, "add IOS(%u) to repls", ios);
			/* XXX journal write */
			i->inoh_ino.ino_nrepls++;
			i->inoh_replicas[j].bs_id = ios;
			/* Note that both the inode structure and replication
			 *  table must be synced.
			 */
			i->inoh_flags |= (INOH_REP_DIRTY | INOH_INO_DIRTY);
			rc = j;
		}
	}
 out:
	INOH_ULOCK(i);
	return (rc);
}

int
mds_repl_inv_except_locked(struct bmapc_memb *bmap, sl_ios_id_t ion)
{
	struct slash_bmap_od *bmapod=bmap_2_bmdsiod(bmap);
	int j, r, bumpgen=0, log=0;
	u8 mask, *b=bmapod->bh_repls;
	u32 pos, k;
	//struct fidc_mds_info *fmdsi=bmap->bcm_fcmh->fcmh_fcoo->fcoo_pri;
	//struct slash_inode_handle *inoh=&fmdsi->fmdsi_inodeh;

	BMAP_LOCK_ENSURE(bmap);
	/* Find our replica id else add ourselves.
         */
	j = mds_repl_ios_lookup(fcmh_2_inoh(bmap->bcm_fcmh), 
				sl_glid_to_resid(ion), 1);
	
        if (j < 0) 
		return (j);
	/* Iterate across the byte array.
	 */
	for (r=0, k=0; k < SL_REPLICA_NBYTES; k++, mask=0) {
		for (pos=0, mask=0; pos < NBBY; 
		     pos+=SL_BITS_PER_REPLICA, r++) {

			mask = (u8)(((2 << SL_BITS_PER_REPLICA)-1) << pos);
			
			if (r == j) {				
				b[r] |= mask & SL_REPL_ACTIVE;
				DEBUG_BMAP(PLL_INFO, bmap, 
					   "add repl for ion(%d)", ion);
			} else {
				switch (b[r] & mask) {
				case SL_REPL_INACTIVE:
				case SL_REPL_TOO_OLD:
					break;
				case SL_REPL_OLD:
					log++;
					b[r] |= mask & SL_REPL_TOO_OLD;
					break;
				case SL_REPL_ACTIVE:
					log++;
					bumpgen++;
					b[r] |= mask & SL_REPL_OLD;
					break;
				}
			}
		}
	}

	if (log) {
		if (bumpgen)
			bmapod->bh_gen.bl_gen++;
		mds_bmap_repl_log(bmap);
	}
	/* XXX Crc has to be rewritten too - this should be done at inode 
	 *  write time only.
	 */	
	return (0);
}

#else
__static int 
mds_repl_xattr_load_locked(__unusedx struct slash_inode_handle *i) { return (0); }

int
mds_repl_inv_except_locked(__unusedx struct bmapc_memb *bmap, __unusedx sl_ios_id_t ion) 
{ return 0; }
#endif

