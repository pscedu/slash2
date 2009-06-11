/* $Id$ */

#ifndef _FIDC_MDS_H_
#define _FIDC_MDS_H_

#include "fid.h"
#include "fidcache.h"
#include "mdsexpc.h"

struct fidc_mds_info *fidc_fid2fmdsi(slfid_t, struct fidc_membh **);
struct fidc_mds_info *fidc_fcmh2fmdsi(struct fidc_membh *);

#endif /* _FIDC_MDS_H_ */
