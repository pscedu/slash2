/* $Id$ */

#ifndef _FIDC_IOD_H_
#define _FIDC_IOD_H_

#include "fid.h"
#include "fidcache.h"

struct fidc_iod_info {
	int fiodi_fd;
};

#define fcmh_2_fiodi(f) (struct fidc_iod_info *)((f)->fcmh_pri)

#endif
