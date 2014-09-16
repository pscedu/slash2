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

void *slm_odt_zerobuf;

void
_slm_odt_zerobuf_ensurelen(size_t len)
{
	static psc_spinlock_t zerobuf_lock = SPINLOCK_INIT;
	static size_t zerobuf_len;

	if (len <= zerobuf_len)
		return;

	spinlock(&zerobuf_lock);
	if (len > zerobuf_len) {
		slm_odt_zerobuf = psc_realloc(slm_odt_zerobuf, len, 0);
		zerobuf_len = len;
	}
	freelock(&zerobuf_lock);
}

#define PACK_IOV(p, len)						\
	do {								\
		iov[nio].iov_base = (void *)(p);			\
		iov[nio].iov_len = (len);				\
		expect += (len);					\
		nio++;							\
	} while (0)

void
slm_odt_write(struct pfl_odt *t, const void *p,
    struct pfl_odt_entftr *f, size_t elem)
{
	size_t nb, expect = 0;
	struct pfl_odt_hdr *h;
	struct iovec iov[3];
	ssize_t rc, pad;
	int nio = 0;
	off_t off;

	memset(iov, 0, sizeof(iov));

	h = t->odt_hdr;
	pad = h->odth_slotsz - h->odth_objsz - sizeof(*f);
	_slm_odt_zerobuf_ensurelen(pad);

	off = elem * h->odth_slotsz + h->odth_start;

	if (p)
		PACK_IOV(p, h->odth_objsz);
	else
		off += h->odth_objsz;

	if (p && f)
		PACK_IOV(slm_odt_zerobuf, pad);
	else
		off += pad;

	if (f)
		PACK_IOV(f, sizeof(*f));

	rc = mdsio_pwritev(current_vfsid, &rootcreds, iov, nio, &nb,
	    off, t->odt_mfh, NULL, NULL);
	psc_assert(!rc && nb == expect);
}

void
slm_odt_read(struct pfl_odt *t, const struct pfl_odt_receipt *r,
    void *p, struct pfl_odt_entftr *f)
{
	size_t nb, expect = 0;
	struct pfl_odt_hdr *h;
	struct iovec iov[3];
	ssize_t rc, pad;
	int nio = 0;
	off_t off;

	memset(iov, 0, sizeof(iov));

	h = t->odt_hdr;
	pad = h->odth_slotsz - h->odth_objsz - sizeof(*f);
	_slm_odt_zerobuf_ensurelen(pad);

	off = h->odth_start + r->odtr_elem * h->odth_slotsz;

	if (p)
		PACK_IOV(p, h->odth_objsz);
	else
		off += h->odth_objsz;

	if (p && f)
		PACK_IOV(slm_odt_zerobuf, pad);
	else
		off += pad;

	if (f)
		PACK_IOV(f, sizeof(*f));

	rc = mdsio_preadv(current_vfsid, &rootcreds, iov, nio, &nb, off,
	    t->odt_mfh);
	psc_assert(!rc && nb == expect);
}

void
slm_odt_sync(struct pfl_odt *t, __unusedx size_t elem)
{
	mdsio_fsync(current_vfsid, &rootcreds, 0, t->odt_mfh);
}

void
slm_odt_close(struct pfl_odt *t)
{
	mdsio_release(current_vfsid, &rootcreds, t->odt_mfh);
}

void
slm_odt_resize(struct pfl_odt *t)
{
	struct pfl_odt_hdr *h;
	size_t nb;
	int rc;

	/*
	 * XXX either trust the bitmap or initialize the footer
	 * of new items
	 */
	h = t->odt_hdr;
	rc = mdsio_write(current_vfsid, &rootcreds, h, sizeof(*h), &nb,
	    0, t->odt_mfh, NULL, NULL);
	psc_assert(!rc && nb == sizeof(*h));
}

void
slm_odt_open(struct pfl_odt *t, const char *fn, __unusedx int oflg)
{
	struct pfl_odt_hdr *h;
	mdsio_fid_t mf;
	size_t nb;
	int rc;

	h = t->odt_hdr;
	rc = mdsio_lookup(current_vfsid,
	    mds_metadir_inum[current_vfsid], fn, &mf, &rootcreds, NULL);
	psc_assert(rc == 0);

	rc = mdsio_opencreate(current_vfsid, mf, &rootcreds, O_RDWR, 0,
	    NULL, NULL, NULL, &t->odt_mfh, NULL, NULL, 0);
	if (rc)
		psc_fatalx("failed to open odtable %s, rc=%d", fn, rc);

	rc = mdsio_read(current_vfsid, &rootcreds, h, sizeof(*h), &nb,
	    0, t->odt_mfh);
	psc_assert(rc == 0 && nb == sizeof(*h));
}

void
slm_odt_create(struct pfl_odt *t, const char *fn, __unusedx int overwrite)
{
	struct pfl_odt_hdr *h;
	mdsio_fid_t mf;
	size_t nb;
	int rc;

	h = t->odt_hdr;
	rc = mdsio_lookup(current_vfsid,
	    mds_metadir_inum[current_vfsid], fn, &mf, &rootcreds, NULL);
	psc_assert(rc == 0);

	rc = mdsio_opencreate(current_vfsid, mf, &rootcreds, O_RDWR, 0,
	    NULL, NULL, NULL, &t->odt_mfh, NULL, NULL, 0);
	if (rc)
		psc_fatalx("failed to open odtable %s, rc=%d", fn, rc);

	rc = mdsio_write(current_vfsid, &rootcreds, h, sizeof(*h), &nb,
	    0, t->odt_mfh, NULL, NULL);
	psc_assert(rc == 0 && nb == sizeof(*h));
}

struct pfl_odt_ops slm_odtops = {
	slm_odt_close,
	slm_odt_create,
	NULL,
	slm_odt_open,
	slm_odt_read,
	slm_odt_resize,
	slm_odt_sync,
	slm_odt_write
};
