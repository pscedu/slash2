/* $Id$ */

#include <stdio.h>

#include "psc_ds/list.h"
#include "psc_ds/listcache.h"
#include "psc_ds/pool.h"
#include "psc_util/atomic.h"
#include "psc_util/cdefs.h"

#include "cache_params.h"
#include "fid.h"
#include "fidcache.h"
#include "fidc_common.h"

#if 0
__static int
fidc_xattr_load(slfid_t fid, sl_inodeh_t *inoh)
{
	char fidfn[FID_MAX_PATH];
	ssize_t sz=sizeof(sl_inode_t);
	psc_crc_t crc;
	int rc;

	fid_makepath(fid, fidfn);

	rc = fid_getxattr(fidfn, SFX_INODE,  &inoh->inoh_ino, sz);
	if (rc)
		return (rc);

	PSC_CRC_CALC(crc, &inoh->inoh_ino, sz);
	if (crc != inoh->inoh_ino.ino_crc) {
                psc_warnx("Crc failure on inode");
                errno = EIO;
                return -1;
        }
	/* XXX move me
	if (inoh->inoh_ino.ino_nrepls) {
                sz = sizeof(sl_replica_t) * inoh->inoh_ino.ino_nrepls;
                inoh->inoh_replicas = PSCALLOC(sz);
		rc = fid_getxattr(fidfn, SFX_INODE,  inoh->inoh_replicas, sz);

                PSC_CRC_CALC(&crc, inoh->inoh_replicas, sz);
                if (crc != inoh->inoh_ino.ino_rs_crc) {
                        psc_warnx("Crc failure on replicas");
                        errno = EIO;
                        return -1;
                }
        }
	*/
	return (0);
}
#endif


struct fidc_mds_info *
fidc_fid2fmdsi(slfid_t f, struct fidc_membh **fcmh)
{
	int l=0;
	//void *zfsdata;
	struct fidc_mds_info *fmdsi=NULL;
	
	*fcmh = fidc_lookup_inode(f);

	if (!*fcmh)
		return NULL;
	
	l = reqlock(&(*fcmh)->fcmh_lock);
	if (!(*fcmh)->fcmh_fcoo) {
		ureqlock(&(*fcmh)->fcmh_lock, l);
		return NULL;
	}

	if (fidc_fcoo_wait_locked((*fcmh), FCOO_NOSTART) < 0)
		return NULL;
	
	psc_assert((*fcmh)->fcmh_fcoo->fcoo_pri);       
	fmdsi = (*fcmh)->fcmh_fcoo->fcoo_pri;	
	ureqlock(&(*fcmh)->fcmh_lock, l);
	return (fmdsi);
}


struct fidc_mds_info *
fidc_fcmh2fmdsi(struct fidc_membh *fcmh)
{
	int l=0;
	struct fidc_mds_info *fmdsi;
	
	l = reqlock(&fcmh->fcmh_lock);
	if (!fcmh->fcmh_fcoo)
		goto out;

	if (fidc_fcoo_wait_locked(fcmh, FCOO_NOSTART) < 0)
		return NULL;

	psc_assert(fcmh->fcmh_fcoo->fcoo_pri);  
	fmdsi = fcmh->fcmh_fcoo->fcoo_pri;	
 out:
	ureqlock(&fcmh->fcmh_lock, l);
	return (fmdsi);
}
