/* $Id$ */
/*
 * %GPL_START_LICENSE%
 * ---------------------------------------------------------------------
 * Copyright 2015-2016, Google, Inc.
 * Copyright 2009-2016, Pittsburgh Supercomputing Center
 * All rights reserved.
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
 * ---------------------------------------------------------------------
 * %END_LICENSE%
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
#include "pathnames.h"

#include "zfs-fuse/zfs_slashlib.h"

static void *slm_odt_zerobuf;

static void
slm_odt_zerobuf_ensurelen(size_t len)
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
    struct pfl_odt_slotftr *f, int64_t item)
{
	size_t nb, expect = 0;
	struct pfl_odt_hdr *h;
	struct iovec iov[3];
	ssize_t rc, pad;
	int nio = 0;
	off_t off;

	memset(iov, 0, sizeof(iov));

	h = t->odt_hdr;

	pad = h->odth_slotsz - h->odth_itemsz - sizeof(*f);
	psc_assert(!pad);

	slm_odt_zerobuf_ensurelen(pad);

	off = item * h->odth_slotsz + h->odth_start;

	if (p)
		PACK_IOV(p, h->odth_itemsz);
	else
		off += h->odth_itemsz;

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
slm_odt_read(struct pfl_odt *t, int64_t item,
    void *p, struct pfl_odt_slotftr *f)
{
	size_t nb, expect = 0;
	struct pfl_odt_hdr *h;
	struct iovec iov[3];
	ssize_t rc, pad;
	int nio = 0;
	off_t off;

	memset(iov, 0, sizeof(iov));

	h = t->odt_hdr;
	pad = h->odth_slotsz - h->odth_itemsz - sizeof(*f);
	psc_assert(!pad);

	slm_odt_zerobuf_ensurelen(pad);

	off = h->odth_start + item * h->odth_slotsz;

	if (p)
		PACK_IOV(p, h->odth_itemsz);
	else
		off += h->odth_itemsz;

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
slm_odt_sync(struct pfl_odt *t, __unusedx int64_t item)
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

	h = t->odt_hdr;
	psc_crc64_calc(&h->odth_crc, h, sizeof(*h) - sizeof(h->odth_crc));
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
	if (rc == 0) {
		rc = mdsio_opencreate(current_vfsid, mf, &rootcreds,
		    O_RDWR, 0, NULL, NULL, NULL, &t->odt_mfh, NULL,
		    NULL, 0);
		if (rc)
			psc_fatalx("failed to open odtable %s: %s",
			    fn, sl_strerror(rc));

		rc = mdsio_read(current_vfsid, &rootcreds, h,
		    sizeof(*h), &nb, 0, t->odt_mfh);
		if (rc || nb != sizeof(*h))
			psc_fatalx("failed to read odtable %s, rc=%d",
			    fn, rc);
		return;
	}
	if (rc == 2 && strcmp(fn, SL_FN_BMAP_ODTAB) == 0)
		rc = t->odt_ops.odtop_new(t, fn, -1);

	if (rc)
		psc_fatalx("failed to lookup/create odtable %s, rc=%d", 
		    fn, rc);
}

/*
 * Create a default on-disk table if no one is found.
 */
int
slm_odt_new(struct pfl_odt *t, const char *fn, __unusedx int overwrite)
{
	int64_t item;
	struct pfl_odt_slotftr f;
	struct pfl_odt_hdr *h;
	size_t nb;
	int rc;

	rc = mdsio_opencreatef(current_vfsid,
	    mds_metadir_inum[current_vfsid], &rootcreds, O_RDWR|O_CREAT,
	    MDSIO_OPENCRF_NOLINK, 0600, fn, NULL, NULL, &t->odt_mfh,
	    NULL, NULL, 0);
	if (rc)
		psc_fatalx("failed to create odtable %s, rc=%d", fn,
		    rc);

	h = t->odt_hdr;
	h->odth_nitems = ODT_ITEM_COUNT;
	h->odth_itemsz = ODT_ITEM_SIZE;

	h->odth_options = ODTBL_OPT_CRC;
	h->odth_slotsz = ODT_ITEM_SIZE + sizeof(struct pfl_odt_slotftr);
	h->odth_start = ODT_ITEM_START;
	psc_crc64_calc(&h->odth_crc, h, sizeof(*h) - sizeof(h->odth_crc));

	rc = mdsio_write(current_vfsid, &rootcreds, h, sizeof(*h), &nb,
	    0, t->odt_mfh, NULL, NULL);
	if (rc || nb != sizeof(*h))
		psc_fatalx("failed to write odtable %s, rc=%d", fn, rc);

	for (item = 0; item < h->odth_nitems; item++) {
		f.odtf_flags = 0;
		f.odtf_slotno = item;

		/* CRC only cover the footer for an unused slot. */
		psc_crc64_init(&f.odtf_crc);
		psc_crc64_add(&f.odtf_crc, &f, sizeof(f) - sizeof(f.odtf_crc));
		psc_crc64_fini(&f.odtf_crc);

		t->odt_ops.odtop_write(t, NULL, &f, item);
	}
	mdsio_fsync(current_vfsid, &rootcreds, 0, t->odt_mfh);
	zfsslash2_wait_synced(0);
	psclog_max("On-disk table %s has been created successfully!", fn);
	return (0);
}

/* See also to pfl_odtops */
struct pfl_odt_ops slm_odtops = {
	slm_odt_new,		/* odtop_new() */
	slm_odt_open,		/* odtop_open() */
	slm_odt_read,		/* odtop_read() */
	slm_odt_write,		/* odtop_write() */
	slm_odt_resize,		/* odtop_resize() */
	slm_odt_close		/* odtop_close() */
};
