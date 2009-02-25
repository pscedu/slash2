/* $Id$ */

#ifndef __FIDC_MDS_H__
#define __FIDC_MDS_H__ 1

#include "fid.h"
#include "fidcache.h"
#include "mdsexpc.h"

extern struct fidc_mds_info *
fidc_fid2fmdsi(slfid_t f, struct fidc_membh **fcmh);

extern struct fidc_mds_info *
fidc_fcmh2fmdsi(struct fidc_membh *fcmh);

#endif
