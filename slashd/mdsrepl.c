/* $Id$ */

#include <errno.h>

#include "inode.h"
#include "inodeh.h"
#include "fid.h"
#include "fidcache.h"
#include "fidc_mds.h"
#include "mdsexpc.h"
#include "mdsio_zfs.h"
#include "mdslog.h"


static int
mds_repl_load_locked(struct slash_inode_handle *i)
{
	int rc;
	psc_crc_t crc;

	psc_assert(!(i->inoh_flags & INOH_HAVE_EXTRAS));

	i->inoh_flags |= INOH_LOAD_EXTRAS;	
	i->inoh_extras = PSCALLOC(sizeof(struct slash_inode_extras_od));

	if ((rc = mdsio_zfs_inode_extras_read(i)))
		return (rc);
	
	psc_crc_calc(&crc, i->inoh_extras, INOX_OD_CRCSZ);
	if (crc != i->inoh_extras->inox_crc) {
		DEBUG_INOH(PLL_WARN, i, "failed crc for extras");
		return (-EIO);
	}
	i->inoh_flags |= INOH_HAVE_EXTRAS;
	i->inoh_flags &= ~INOH_LOAD_EXTRAS;

	return (0);
}

int 
mds_repl_ios_lookup(struct slash_inode_handle *i, sl_ios_id_t ios, int add)
{
	uint32_t j=0, k;
	int rc = -ENOENT;
	sl_replica_t *repl;

	INOH_LOCK(i);
	if (!i->inoh_ino.ino_nrepls) {
		if (!add)
			goto out;
		else
			goto add_repl;
	}

	for (j=0, k=0, repl=i->inoh_ino.ino_repls; j < i->inoh_ino.ino_nrepls; 
	     j++, k++) {
		if (j >= INO_DEF_NREPLS) {
			/* The first few replicas are in the inode itself, 
			 *   the rest are in the extras block;
			 */
			if (!(i->inoh_flags & INOH_HAVE_EXTRAS))
                                if (!(rc = mds_repl_load_locked(i)))
					goto out;

			repl = i->inoh_extras->inox_repls;
			k = 0;
		}

		DEBUG_INOH(PLL_INFO, i, "rep%u[%u] == %u", 
			   k, repl[k].bs_id, ios);
		if (repl[k].bs_id == ios) {
			rc = j;
			goto out;
		}
	}
	/* It does not exist, add the replica to the inode if 'add' was
	 *   specified, else return.
	 */
	if (rc == -ENOENT && add) {
	add_repl:
		psc_assert(i->inoh_ino.ino_nrepls == j);

		if (i->inoh_ino.ino_nrepls >= SL_MAX_REPLICAS) {
			DEBUG_INOH(PLL_WARN, i, "too many replicas");
			rc = -ENOSPC;
			goto out;
		}
		
		if (j > INO_DEF_NREPLS) {
			/* Note that both the inode structure and replication
			 *  table must be synced.
			 */
			psc_assert(i->inoh_extras);
			i->inoh_flags |= (INOH_EXTRAS_DIRTY | INOH_INO_DIRTY);
			repl = i->inoh_extras->inox_repls;
			k = j - INO_DEF_NREPLS;
		} else {
			i->inoh_flags |= INOH_INO_DIRTY;
			repl = i->inoh_ino.ino_repls;
			k = j;
		}

		repl[k].bs_id = ios;
		i->inoh_ino.ino_nrepls++;
		
		DEBUG_INOH(PLL_INFO, i, "add IOS(%u) to repls, replica %d", 
			   ios, i->inoh_ino.ino_nrepls-1);
		
		mds_inode_addrepl_log(i, ios, j);

		rc = j;
	}
 out:
	INOH_ULOCK(i);
	return (rc);
}

int
mds_repl_inv_except_locked(struct bmapc_memb *bmap, sl_ios_id_t ion)
{
	struct slash_bmap_od *bmapod=bmap_2_bmdsiod(bmap);
	int r, j, bumpgen=0, log=0;
	uint8_t mask, *b=bmapod->bh_repls;
	uint32_t pos, k;

	BMAP_LOCK_ENSURE(bmap);
	/* Find our replica id else add ourselves.
         */
	j = mds_repl_ios_lookup(fcmh_2_inoh(bmap->bcm_fcmh), 
				sl_glid_to_resid(ion), 1);
	
        if (j < 0) 
		return (j);
	/* Iterate across the byte array.
	 */

	mds_bmapod_dump(bmap);

	for (r=0, k=0; k < SL_REPLICA_NBYTES; k++, mask=0)
		for (pos=0, mask=0; pos < NBBY; 
		     pos+=SL_BITS_PER_REPLICA, r++) {

			mask = (uint8_t)(SL_REPLICA_MASK << pos);	

			if (r == j) {				
				b[k] |= (mask & SL_REPL_ACTIVE);
				DEBUG_BMAP(PLL_INFO, bmap, 
					   "add repl for ion(%u)", ion);
			} else {
				switch (b[k] & mask) {
				case SL_REPL_INACTIVE:
				case SL_REPL_TOO_OLD:
					break;
				case SL_REPL_OLD:
					log++;
					b[k] |= (mask & SL_REPL_TOO_OLD);
					break;
				case SL_REPL_ACTIVE:
					log++;
					bumpgen++;
					b[k] |= (mask & SL_REPL_OLD);
					break;
				}
			}
		}

	if (log) {
		if (bumpgen)
			bmapod->bh_gen.bl_gen++;
		mds_bmap_repl_log(bmap);
	}

	return (0);
}


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
#endif
