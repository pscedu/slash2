/* $Id$ */
/*
 * %PSCGPL_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2014, Pittsburgh Supercomputing Center (PSC).
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

/*
 * This file contains definitions related to the assignment and
 * orchestration of the identifers of files (FID) in a SLASH network.
 */

#ifndef _SLASH_FID_H_
#define _SLASH_FID_H_

#include <sys/types.h>

#include <inttypes.h>
#include <stdio.h>

#include "sltypes.h"

#define	SL_FIDBUF_LEN		(18 + 1 + 20 + 1)

struct sl_fidgen;

/*
 * SLASH file IDs (FID) consist of four parts: flag bits, site ID, cycle
 * bits, and a file sequence number.  FIDs are used always used for
 * external communication among other clients, I/O servers, and MDS to
 * identify files.
 *
 * Underlying backend MDS file system inode tracking is contained within
 * the mdsio layer and is only used internally.
 *
 * Cycle bits are used when we fail to recover a MDS to a previous
 * state.  The system adminstrator should decide the value to increase
 * to make sure that there is no outstanding duplicate fid.  For
 * example, if you hit two disasters in a row, we should bump the number
 * by two.
 *
 * Normally, we allow 42-bit worth of FIDs to be allocated.  When that
 * limit is reached, we could let system administrator to bump the cycle
 * number and zero the FID bits.
 */
#define	SLASH_FID_FLAG_BITS	4
#define	SLASH_FID_SITE_BITS	10
#define	SLASH_FID_CYCLE_BITS	8
#define	SLASH_FID_INUM_BITS	42

#define	SLASH_FID_FLAG_SHFT	(SLASH_FID_SITE_BITS + SLASH_FID_SITE_SHFT)
#define	SLASH_FID_SITE_SHFT	(SLASH_FID_CYCLE_BITS + SLASH_FID_CYCLE_SHFT)
#define	SLASH_FID_CYCLE_SHFT	(SLASH_FID_INUM_BITS)
#define	SLASH_FID_INUM_SHFT	0

#define SLFIDF_HIDE_DENTRY	(UINT64_C(1) << 0)	/* keep but hide an entry until its log arrives */
#define SLFIDF_LOCAL_DENTRY	(UINT64_C(1) << 1)	/* don't expose to external nodes */

/*
 * Looks like the links in our by-id namespace are all created as
 * regular files.  But some of them are really links to directories.  We
 * need a way to only allow them to be used as directories for remote
 * clients.
 */
#define SLFIDF_DIR_DENTRY	(UINT64_C(1) << 2)	/* a directory link */

struct sl_fidgen {
	slfid_t			fg_fid;
	slfgen_t		fg_gen;
};
#define slash_fidgen sl_fidgen

#define FID_ANY			UINT64_C(0xffffffffffffffff)

#define FID_MAX_INUM		MAXVALMASK(SLASH_FID_INUM_BITS)

/* temporary placeholder for the not-yet-known generation number */
#define FGEN_ANY		UINT64_C(0xffffffffffffffff)

/*
 * The following SLASHIDs are reserved:
 *	0	not used
 */
#define SLFID_ROOT		1	/* / */
#define SLFID_NS		2	/* /.slfidns */
#define SLFID_MIN		3	/* minimum usable */

#define SLPRI_FSID		"%#018"PRIx64
#define FSID_LEN		16

#define FID_PATH_DEPTH		4
#define FID_PATH_START		3

/* bits per hex char e.g. 0xffff=16 */
#define BPHXC			4

#define SLPRI_FID		"%#018"PRIx64
#define SLPRIxFID		PRIx64
#define SLPRI_FGEN		"%"PRIu64

#define SLPRI_FG		SLPRI_FID":"SLPRI_FGEN
#define SLPRI_FG_ARGS(fg)	(fg)->fg_fid, (fg)->fg_gen

#define _FID_SET_FIELD(fid, val, shft, nb)				\
	((fid) = ((fid) & ~(MAXVALMASK(nb) << (shft))) |		\
	 ((val) & MAXVALMASK(nb)) << (shft))

#define _FID_GET_FIELD(fid, shft, nb)					\
	(((fid) >> (shft)) & MAXVALMASK(nb))

#define FID_GET_FLAGS(fid)	_FID_GET_FIELD((fid), SLASH_FID_FLAG_SHFT, SLASH_FID_FLAG_BITS)
#define FID_GET_SITEID(fid)	_FID_GET_FIELD((fid), SLASH_FID_SITE_SHFT, SLASH_FID_SITE_BITS)
#define FID_GET_CYCLE(fid)	_FID_GET_FIELD((fid), SLASH_FID_CYCLE_SHFT, SLASH_FID_CYCLE_BITS)
#define FID_GET_INUM(fid)	_FID_GET_FIELD((fid), SLASH_FID_INUM_SHFT, SLASH_FID_INUM_BITS)

#define FID_SET_FLAGS(fid, fl)	_FID_SET_FIELD((fid), (fl), SLASH_FID_FLAG_SHFT, SLASH_FID_FLAG_BITS)
#define FID_SET_SITEID(fid, id)	_FID_SET_FIELD((fid), (id), SLASH_FID_SITE_SHFT, SLASH_FID_SITE_BITS)
#define FID_SET_CYCLE(fid, cy)	_FID_SET_FIELD((fid), (cy), SLASH_FID_CYCLE_SHFT, SLASH_FID_CYCLE_BITS)

#define SAMEFG(a, b)							\
	((a)->fg_fid == (b)->fg_fid && (a)->fg_gen == (b)->fg_gen)

#define COPYFG(dst, src)						\
	do {								\
		psc_assert(sizeof(*(dst)) ==				\
		    sizeof(struct slash_fidgen));			\
		psc_assert(sizeof(*(src)) ==				\
		    sizeof(struct slash_fidgen));			\
		memcpy((dst), (src), sizeof(*(dst)));			\
	} while (0)

static __inline int
sl_sprintf_fid(slfid_t fid, char *buf, size_t len)
{
	int rc;

	if (fid == FID_ANY)
		rc = snprintf(buf, len, "<FID_ANY>");
	else
		rc = snprintf(buf, len, SLPRI_FID, fid);
	return (rc);
}

static __inline int
sl_sprintf_fgen(slfgen_t fgen, char *buf, size_t len)
{
	int rc;

	if (fgen == FGEN_ANY)
		rc = snprintf(buf, len, "<FGEN_ANY>");
	else
		rc = snprintf(buf, len, SLPRI_FGEN, fgen);
	return (rc);
}

static __inline const char *
sl_sprinta_fid(slfid_t fid)
{
	char *buf;

	buf = pfl_tls_get(SL_TLSIDX_FIDBUF, SL_FIDBUF_LEN);
	sl_sprintf_fid(fid, buf, SL_FIDBUF_LEN);
	return (buf);
}

static __inline const char *
sl_sprinta_fgen(slfgen_t fgen)
{
	char *buf;

	buf = pfl_tls_get(SL_TLSIDX_FIDBUF, SL_FIDBUF_LEN);
	sl_sprintf_fgen(fgen, buf, SL_FIDBUF_LEN);
	return (buf);
}

static __inline const char *
sl_sprintfa_fg(struct slash_fidgen *fg)
{
	char *buf;
	int rc;

	buf = pfl_tls_get(SL_TLSIDX_FIDBUF, SL_FIDBUF_LEN);
	rc = sl_sprintf_fid(fg->fg_fid, buf, SL_FIDBUF_LEN);
	buf[rc] = ':';
	rc = sl_sprintf_fgen(fg->fg_gen, buf + rc, SL_FIDBUF_LEN - rc);
	return (buf);
}

#endif /* _SLASH_FID_H_ */
