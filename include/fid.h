/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
 *
 * Permission to use, copy, and modify this software and its documentation
 * without fee for personal use or non-commercial use within your organization
 * is hereby granted, provided that the above copyright notice is preserved in
 * all copies and that the copyright and this permission notice appear in
 * supporting documentation.  Permission to redistribute this software to other
 * organizations or individuals is not permitted without the written permission
 * of the Pittsburgh Supercomputing Center.  PSC makes no representations about
 * the suitability of this software for any purpose.  It is provided "as is"
 * without express or implied warranty.
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
#define FID_MAX_PATH		96

struct slash_fidgen;

/*
 * SLASH file IDs consist of three parts: flag bits, site ID, and a file
 * sequence number.  FIDs are used always used for external communication
 * among other clients, I/O servers, and MDS to identify files.
 *
 * Underlying backend MDS file system inode tracking is contained within the
 * mdsio layer and is only used internally.
 */
#define	SLASH_ID_FLAG_BITS	4
#define	SLASH_ID_SITE_BITS	10
#define	SLASH_ID_FID_BITS	50

#define SLFIDF_HIDE_DENTRY	(UINT64_C(1) << 0)	/* keep but hide an entry until its log arrives */
#define SLFIDF_LOCAL_DENTRY	(UINT64_C(1) << 1)	/* don't expose to external nodes */

/*
 * Looks like the links in our by-id namespace are all created as regular files.
 * But some of them are really links to directories. We need a way to only
 * allow them to be used as directories for remote clients.
 */
#define SLFIDF_DIR_DENTRY	(UINT64_C(1) << 2)	/* a directory link */

struct slash_fidgen {
	slfid_t			fg_fid;
	slfgen_t		fg_gen;
};

#define FID_ANY			UINT64_C(0xffffffffffffffff)

/* temporary placeholder for the not-yet-known generation number */
#define FGEN_ANY		UINT64_C(0xffffffffffffffff)

/*
 * The following SLASHIDs are reserved:
 *	0	not used
 *	1	-> /
 */
#define SLFID_ROOT		1
#define SLFID_MIN		2

#define SLPRI_FSID		"%#018"PRIx64
#define FSID_LEN		16

#define FID_PATH_DEPTH		3

/* bits per hex char e.g. 0xffff=16 */
#define BPHXC			4

#define SLPRI_FID		"%#018"PRIx64
#define SLPRIxFID		PRIx64
#define SLPRI_FGEN		PRIu64

#define SLPRI_FG		SLPRI_FID":%"SLPRI_FGEN
#define SLPRI_FG_ARGS(fg)	(fg)->fg_fid, (fg)->fg_gen

#define FID_GET_FLAGS(fid)	((fid) >> (SLASH_ID_SITE_BITS + SLASH_ID_FID_BITS))
#define FID_GET_SITEID(fid)	(((fid) >> SLASH_ID_FID_BITS) &		\
				    ~(~UINT64_C(0) << SLASH_ID_SITE_BITS))
#define FID_GET_INUM(fid)	((fid) & ~(~UINT64_C(0) << (SLASH_ID_FID_BITS)))

#define FID_SET_FLAGS(fid, fl)	((fid) |= ((fl) << (SLASH_ID_SITE_BITS + SLASH_ID_FID_BITS)))

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

#define IS_REMOTE_FID(fid)						\
	((fid) != SLFID_ROOT && nodeSite->site_id != FID_GET_SITEID(fid))

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
		rc = snprintf(buf, len, "%"SLPRI_FGEN, fgen);
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
