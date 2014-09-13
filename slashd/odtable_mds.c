/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2009-2014, Pittsburgh Supercomputing Center (PSC).
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or (at
 * your option) any later version.
 *
 * This program is distributed WITHOUT ANY WARRANTY; without even the
 * implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE.  See the GNU General Public License contained in the file
 * `COPYING-GPL' at the top of this distribution or at
 * https://www.gnu.org/licenses/gpl-2.0.html for more details.
 *
 * Pittsburgh Supercomputing Center	phone: 412.268.4960  fax: 412.268.5832
 * 300 S. Craig Street			e-mail: remarks@psc.edu
 * Pittsburgh, PA 15213			web: http://www.psc.edu/
 * -----------------------------------------------------------------------------
 * %PSC_END_COPYRIGHT%
 */

#include <sys/param.h>

#include <string.h>

#include "pfl/alloc.h"
#include "pfl/cdefs.h"
#include "pfl/ctlsvr.h"
#include "pfl/lock.h"
#include "pfl/lockedlist.h"
#include "pfl/log.h"
#include "pfl/odtable.h"
#include "pfl/str.h"
#include "pfl/types.h"

#include "mdsio.h"
#include "slashd.h"

#include "zfs-fuse/zfs_slashlib.h"

void			*pfl_odt_zerobuf;

void
_slm_odt_zerobuf_ensurelen(size_t len)
{
	static psc_spinlock_t zerobuf_lock = SPINLOCK_INIT;
	static size_t zerobuf_len;

	if (len <= zerobuf_size)
		return;

	spinlock(&zerobuf_lock);
	if (len > zerobuf_size) {
		pfl_odt_zerobuf = psc_realloc(pfl_odt_zerobuf, len);
		//
		zerobuf_size = len;
	}
	freelock(&zerobuf_lock);
}

int
slm_odt_write(struct pfl_odt *t, )
{
	pad = h->odth_slotsz - h->odth_objsz - sizeof(odtf);
	pfl_odt_zerobuf_ensurelen(pad);

	iov[0].iov_base = data;
	iov[0].iov_len = h->odth_objsz;

	iov[1].iov_base = pfl_odt_zerobuf;
	iov[1].iov_len = pad;

	iov[2].iov_base = &odtf;
	iov[2].iov_len = sizeof(odtf);

	rc = t->odt_ops.odtop_write(t,
	    current_vfsid, &rootcreds,
	    iov,
	    nitems(iov), &nb, odtr->odtr_elem * h->odth_slotsz +
	    h->odth_start, t->odt_handle, NULL, NULL);
	psc_assert(!rc && nb == h->odth_slotsz);
	if (nb != h->odth_slotsz && !rc)
		rc = EIO;
}

int
slm_odt_read(struct pfl_odt *t, )
{
	pad = h->odth_slotsz - h->odth_objsz - sizeof(f);
	pfl_odt_zerobuf_ensurelen(pad);

	iov[0].iov_base = data;
	iov[0].iov_len = h->odth_objsz;

	iov[1].iov_base = pfl_odt_zerobuf;
	iov[1].iov_len = pad;

	iov[2].iov_base = &f;
	iov[2].iov_len = sizeof(f);

	rc = mdsio_preadv(current_vfsid, &rootcreds, p, h->odth_slotsz,
	    &nb, h->odth_start + r->odtr_elem * h->odth_slotsz,
	    t->odt_handle);
	if (nb != h->odth_slotsz && !rc)
		rc = EIO;
}

void
slm_odt_sync(struct pfl_odt *t, size_t elem)
{
	mdsio_fsync(current_vfsid, &rootcreds, 0, t->odt_handle);
}

void
slm_odt_release(struct pfl_odt *t)
{
	mdsio_release(current_vfsid, &rootcreds, t->odt_handle);
}

void
slm_odt_resize(struct pfl_odt *t)
{
	/*
	 * XXX either trust the bitmap or initialize the footer
	 * of new items
	 */
	rc = mdsio_write(current_vfsid, &rootcreds, h,
	    sizeof(*h), &nb, 0,
	    t->odt_handle, NULL, NULL);
	psc_assert(!rc && nb == sizeof(*h));
}

void
pfl_odt_getfooter(const struct pfl_odt *t, size_t elem,
    struct pfl_odt_entftr *f)
{
	struct pfl_odt_hdr *h;
	size_t nb;
	int rc;

	h = t->odt_hdr;

	rc = mdsio_read(current_vfsid, &rootcreds, f, sizeof(*f),
	    &nb, h->odth_start + elem * h->odth_slotsz + h->odth_elemsz,
	    t->odt_handle);
	psc_assert(rc == 0 && nb == sizeof(*f));
}


slm_odt_open()
{
	rc = mdsio_lookup(current_vfsid,
	    mds_metadir_inum[current_vfsid], fn, &mf, &rootcreds, NULL);
	psc_assert(rc == 0);

	rc = mdsio_opencreate(current_vfsid, mf, &rootcreds, O_RDWR, 0,
	    NULL, NULL, NULL, &t->odt_handle, NULL, NULL, 0);
	if (rc || !t->odt_handle)
		psc_fatalx("failed to open odtable %s, rc=%d", fn, rc);

	rc = mdsio_read(current_vfsid, &rootcreds, h, sizeof(*h),
	    &nb, 0, t->odt_handle);
	t->odt_hdr = h;
	psc_assert(rc == 0 && nb == sizeof(*h));
}

struct pfl_odt_ops slm_odt_ops = {
};
