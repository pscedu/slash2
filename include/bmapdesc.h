/* $Id$ */

#ifndef _SL_BMAPDESC_H_
#define _SL_BMAPDESC_H_

#include "psc_rpc/rpc.h"

#include "sltypes.h"
#include "slashrpc.h"

int	 bmapdesc_access_check(struct srt_bmapdesc *, enum rw,
	    sl_ios_id_t, lnet_nid_t);

#endif /* _SL_BMAPDESC_H_ */
