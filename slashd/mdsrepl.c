#include "inode.h"
#include "fid.h"


__static int 
mds_repl_crc_check(sl_inodeh_t *i)
{
	psc_crc_t crc;
	size_t sz = (sizeof(sl_replica_t) * i->inoh_ino.ino_nrepls);

	PSC_CRC_CALC(crc, i->inoh_replicas, sz);
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
mds_repl_ios_lookup(sl_inodeh_t *i, sl_iod_id_t ios)
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
			break;
		}
	}
 out:
	INOH_ULOCK(i);
	return (rc);
}

