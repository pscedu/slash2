/* $Id$ */

#include <stdio.h>

#include "pfl/cdefs.h"
#include "psc_ds/dynarray.h"
#include "psc_ds/list.h"
#include "psc_util/thread.h"

#include "slashd.h"
#include "slconfig.h"

__dead void *
slmsmthr_main(__unusedx void *arg)
{
	for (;;) {
	}
}

void
sitemons_spawn(void)
{
	struct slmsm_thread *smsmt;
	struct psc_thread *thr;
	struct sl_site *site;

	psclist_for_each_entry(site, &globalConfig.gconf_sites,
	    site_lentry) {
		thr = pscthr_init(SLMTHRT_SITEMON, 0, slmsmthr_main,
		    NULL, sizeof(*smsmt), "slmsmthr-%s",
		    site->site_name + strcspn(site->site_name, "@"));
		smsmt = slmsmthr(thr);
		smsmt->smsmt_site = site;
		dynarray_init(&smsmt->smsmt_replq);
		pscthr_setready(thr);
	}
}
