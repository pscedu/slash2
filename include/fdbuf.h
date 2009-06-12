/* $Id$ */

/*
 * fdbuf - file descriptor buffer routines.
 */

#ifndef _ZEST_FDBUF_H_
#define _ZEST_FDBUF_H_

#include <stdint.h>

#include "psc_rpc/rpc.h"

#include "fid.h"

struct stat;
struct srt_fd_buf;

void fdbuf_encrypt(struct srt_fd_buf *, const struct slash_fidgen *,
	lnet_process_id_t);
int  fdbuf_decrypt(struct srt_fd_buf *, uint64_t *,
	struct slash_fidgen *, lnet_process_id_t);
void fdbuf_readkeyfile(void);
void fdbuf_checkkeyfile(void);
void fdbuf_checkkey(const char *, struct stat *);
void fdbuf_createkeyfile(void);

#endif /* _ZEST_FDBUF_H_ */
