/* $Id$ */

#ifndef _SLASH_FID_H
#define _SLASH_FID_H

#include <string.h>

#include "psc_types.h"

typedef struct slash_fid {
	u64 fid_inum;  /* first 16 bits are the svr / fs id */
	u64 fid_gen;
} slash_fid_t;

#define FIDSZ  (sizeof(slash_fid_t))

/* 16 bit server / filesystem id */
#define FSID_FMT "%016"ZLPX64
#define FSID_LEN 16
#define FID_PATH_DEPTH 2
#define FID_PATH_LEN 1024

#define FIDFMT			"%"ZLPX64":%"ZLPX64
#define FIDFMT2			LPX64" "ZLPX64
#define FIDFMTARGS(fid)		(fid)->fid_inum, (fid)->fid_gen

#define SLASH_FID_FSID(fid) (u32)((fid)->fid_inum >> 48)
#define SLASH_FID_INUM(fid) (u64)((fid)->fid_inum & 0xffffffffffffULL)

#define COPYFID(dest,src) (memcpy((dest),(src),FIDSZ))

int  fid_makepath(const slash_fid_t *, char *);
int  fid_link(const slash_fid_t *, const char *);

#endif
