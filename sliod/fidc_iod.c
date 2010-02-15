/* $Id$ */

#include <sys/resource.h>

#include <stddef.h>

#include "psc_util/log.h"
#include "psc_util/rlimit.h"

#include "fidc_iod.h"
#include "fidcache.h"

int
sli_fcmh_grow(void)
{
	rlim_t soft, hard;
	int rc;

	/* increase #fd resource limit */
	spinlock(&psc_rlimit_lock);
	rc = psc_getrlimit(RLIMIT_NOFILE, &soft, &hard);
	if (rc == 0 && psc_setrlimit(RLIMIT_NOFILE,
	    soft + 1, hard + 1) == -1)
		psc_warn("setrlimit NOFILE %"PRId64, soft + 1);
	freelock(&psc_rlimit_lock);
	return (rc);
}

void
sli_fcmh_shrink(void)
{
	rlim_t soft, hard;
	int rc;

	/* decrease #fd resource limit */
	spinlock(&psc_rlimit_lock);
	rc = psc_getrlimit(RLIMIT_NOFILE, &soft, &hard);
	if (rc == 0 && psc_setrlimit(RLIMIT_NOFILE,
	    soft - 1, hard - 1) == -1)
		psc_warn("setrlimit NOFILE %"PRId64, soft - 1);
	freelock(&psc_rlimit_lock);
}

int
fcmh_ensure_has_fii(struct fidc_membh *fcmh)
{
	struct fcoo_iod_info *fii;
	int rc = 0, locked;

	locked = FCMH_RLOCK(fcmh);
	if (fcmh->fcmh_fcoo ||
	    (fcmh->fcmh_state & FCMH_FCOO_CLOSING)) {
		rc = fidc_fcoo_wait_locked(fcmh, FCOO_START);
		if (rc < 0)
			goto out;
	} else
		fidc_fcoo_start_locked(fcmh);
	if (fcmh->fcmh_fcoo->fcoo_pri == NULL)
		fcmh->fcmh_fcoo->fcoo_pri = PSCALLOC(sizeof(*fii));
 out:
	FCMH_URLOCK(fcmh, locked);
	return (rc);
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* getattr */	NULL,
/* grow */	sli_fcmh_grow,
/* shrink */	sli_fcmh_shrink
};
