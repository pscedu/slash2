#include "inode.h"
#include "fid.h"


__static int 
mds_repl_crc_check(sl_inodeh_t *i)
{
	psc_crc_t crc;

	PSC_CRC_CALC(crc, i->inoh_replicas, 
		     (sizeof(sl_replica_t) * i->inoh_ino.ino_nrepls));

	if (crc != i->inoh_ino.ino_rs_crc) {
		psc_warnx("Crc failure on replicas");
		return (-EIO);
	}		
	return (0);
}

__static int 
mds_repl_xattr_load_locked(sl_inodeh_t *i)
{
	char fidfn[FID_MAX_PATH];
        size_t sz;
	int rc;

	DEBUG_INOH(PLL_INFO, i, "trying to load replica table");

	INOH_LOCK_ENSURE(i);
	psc_assert(i->inoh_ino.ino_nrepls);
	psc_assert(!i->inoh_replicas);
	psc_assert(!(i->inoh_flags & INOH_HAVE_REPS));

	fid_makepath(fid, fidfn);
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
mds_repl_load_locked(sl_inodeh_t *i)
{
	int rc=0;

	INOH_LOCK_ENSURE(i);
	if (i->inoh_ino.ino_nrepls) {		
		if  (i->inoh_flags & INOH_HAVE_REPS)
			rc = mds_repl_crc_check(i);			
	        else 
			rc = mds_repl_xattr_load_locked(i);
	}	
	return (rc);
}

int 
mds_repl_ios_lookup(sl_inodeh_t *i, sl_iod_id_t ios, int add)
{
	int j, rc=-1;

	INOH_LOCK(i);
	if (!i->inoh_ino.ino_nrepls)
		goto out;

	else if (!(i->inoh_flags & INOH_HAVE_REPS)) {
		if (rc = mds_repl_load_locked(i))
			goto out;
	}
	psc_assert(i->inoh_replicas);

	for (j=0; j < i->inoh_ino.ino_nrepls; j++) {
		if (i->inoh_replicas[j].bs_id == ios) {
			rc = j;
			goto out;
		}
	}

	if (rc == -1 && add) {
		if (i->inoh_ino.ino_nrepls >= SL_MAX_REPLICAS)
			DEBUG_INOH(PLL_WARN, i, "too many replicas");
		else {
			DEBUG_INOH(PLL_INFO, i, "add IOS(%s) to repls", ios);
			/* XXX journal write */
			i->inoh_ino.ino_nrepls++;
			i->inoh_replicas[j].bs_id == ios;
			/* Note that both the inode structure and replication
			 *  table must be synced.
			 */
			i->inoh_flags |= (INOH_REP_DIRTY || INOH_INO_DIRTY);
			rc = j;
		}
	}
 out:
	INOH_ULOCK(i);
	return (rc);
}

int
mds_repl_inv_except_locked(struct bmapc_memb *b, sl_ios_id_t ion)
{
	//sl_inodeh_t *i=fcmh_2_inoh(b->bcm_fcmh);
	sl_blkh_t *bmapod=b->bcm_bmapih.bmapi_data
	int j, k, pos;
	u8 mask, *b=bmapod->bh_repls;

	BMAP_LOCK_ENSURE(b);
	/* Find our replica id else add ourselves.
         */
	j = mds_repl_ios_lookup(fcmh_2_inoh(b->bmap_fcmh), 
				sl_glid_to_resid(ion), 1);
        if (j < 0) 
		return (j);
	
	for (k=0, pos=0, mask=0, b=; k < SL_REPLICA_NBYTES; k++, mask=0) {
		
		mask = 3 << pos;
		
		pos += SL_BITS_PER_REPLICA;
		if (pos >= (NBBY / SL_BITS_PER_REPLICA))
			pos = 0;
	}
}
