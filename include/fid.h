/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

#ifndef _SLASH_FID_H_
#define _SLASH_FID_H_

#include <sys/types.h>

#include <inttypes.h>

#define FID_MAX_PATH	96
#define IMNS_NAME_MAX	17
#define FID_FD_NOTOPEN	(-2)

struct slash_creds;
struct slash_fidgen;

typedef uint64_t slfid_t;	/* first 16 bits are the svr/fs id, rest are inum */
typedef uint64_t slfgen_t;

struct slash_fidgen {
	slfid_t		fg_fid;
	slfgen_t	fg_gen;
};

#define SLASH_ROOT_ID		UINT64_C(2)

#define FID_ANY			UINT64_C(0xffffffffffff)
#define FIDGEN_ANY		UINT64_C(0xffffffffffff)

/* 16 bit server/filesystem id */
#define FSID_FMT		"%016"PRIx64
#define FSID_LEN		16

#define FID_PATH_DEPTH		3
#define FID_PATH_LEN		1024
#define FID_PATH_NAME           ".slfidns"

#define BPHXC			4

#define FIDFMT			"%"PRId64":%"PRId64
#define FIDFMTARGS(fg)		(fg)->fg_fid, (fg)->fg_gen

#define FID_FSID(fid)		((uint32_t)((fid) >> 48))
#define FID_INUM(fid)		((uint64_t)((fid) & UINT64_C(0xffffffffffff)))

#define SAMEFID(a, b)							\
	(((a)->fg_fid == (b)->fg_fid) && ((a)->fg_gen == (b)->fg_gen))

#define COPYFID(d, s)		memcpy((d), (s), sizeof(*(d)))
#define COPYFG(d, s)		memcpy((d), (s), sizeof(*(d)))

void	fid_makepath(slfid_t, char *);
int	fid_link(slfid_t, const char *);
//int  fid_getxattr(const char *, const char *, void *, ssize_t);
int	fid_fileops(slfid_t, int);
int	fid_fileops_fg(struct slash_fidgen *, int, mode_t);

#define fid_open(f)	fid_fileops((f), O_RDWR)
#define fid_ocreat(f)	fid_fileops((f), O_RDWR | O_CREAT)

#endif /* _SLASH_FID_H_ */
