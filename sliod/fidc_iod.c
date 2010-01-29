/* $Id$ */

#include <sys/resource.h>

#include <stddef.h>

#include "psc_util/log.h"
#include "psc_util/rlimit.h"

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
		psc_warn("setrlimit NOFILE %ld", soft + 1);
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
		psc_warn("setrlimit NOFILE %ld", soft - 1);
	freelock(&psc_rlimit_lock);
}

struct sl_fcmh_ops sl_fcmh_ops = {
/* getattr */	NULL,
/* grow */	sli_fcmh_grow,
/* shrink */	sli_fcmh_shrink
};
