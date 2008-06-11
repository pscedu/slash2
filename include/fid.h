/* $Id$ */

#ifndef __FID_H__
#define __FID_H__

#include <string.h>

#include "psc_types.h"

typedef u64 slfid_t;	/* first 16 bits are the svr/fs id, rest are inum */

struct slash_fidgen {
	slfid_t	fg_fid;
	u64	fg_gen;
};

#define FID_ANY                 0xffffffffffffULL

/* 16 bit server/filesystem id */
#define FSID_FMT		"%016"_P_U64"x"
#define FSID_LEN		16
#define FID_PATH_DEPTH		2
#define FID_PATH_LEN		1024

#define FIDFMT			"%"_P_U64"x:%"_P_U64"x"
#define FIDFMTARGS(fg)		(fg)->fg_fid, (fg)->fg_gen

#define FID_FSID(fid)		((u32)((fid) >> 48))
#define FID_INUM(fid)		((u64)((fid) & U64CONST(0xffffffffffff)))

void fid_makepath(slfid_t, char *);
int  fid_link(slfid_t, const char *);
int  fid_get(slfid_t *, const char *, int);

int  translate_pathname(char *, int);
int  untranslate_pathname(char *);

#endif /* __FID_H__ */
