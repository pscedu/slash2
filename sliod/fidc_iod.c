/* $Id$ */

#include <sys/resource.h>

#include <stddef.h>

#include "psc_util/log.h"
#include "psc_util/rlimit.h"

#include "fidcache.h"

int
sli_fcmh_init(__unusedx struct fidc_membh *fcmh)
{
	rlim_t soft, hard;
	int rc;

	/* increase #fd resource limit */
	spinlock(&psc_rlimit_lock);
	rc = psc_getrlimit(RLIMIT_NOFILE, &soft, &hard);
	if (rc == 0 && psc_setrlimit(RLIMIT_NOFILE,
	    soft + 1, hard + 1) == -1)
		psc_warn("setrlimit NOFILE %ld", soft + 1);
	freelock(&psc_rlimit_lock);
	return (rc);
}

void
sli_fcmh_dtor(__unusedx struct fidc_membh *fcmh)
{
	rlim_t soft, hard;
	int rc;

	/* decrease #fd resource limit */
	spinlock(&psc_rlimit_lock);
	rc = psc_getrlimit(RLIMIT_NOFILE, &soft, &hard);
	if (rc == 0 && psc_setrlimit(RLIMIT_NOFILE,
	    soft - 1, hard - 1) == -1)
		psc_warn("setrlimit NOFILE %ld", soft - 1);
	freelock(&psc_rlimit_lock);
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* getattr */	NULL,
/* init */	sli_fcmh_init,
/* dtor */	sli_fcmh_dtor
};
