/* $Id$ */

/*
 * fdbuf - file descriptor buffer routines.
 */

#ifndef _ZEST_FDBUF_H_
#define _ZEST_FDBUF_H_

#include "slconfig.h"

struct stat;
struct srt_fd_buf;
struct srt_bmapdesc_buf;

void bdbuf_sign(struct srt_bmapdesc_buf *, const struct slash_fidgen *,
	lnet_process_id_t, lnet_nid_t, sl_ios_id_t, sl_blkno_t);
int  bdbuf_check(struct srt_bmapdesc_buf *, uint64_t *, struct slash_fidgen *,
	sl_blkno_t *, lnet_process_id_t, lnet_nid_t, sl_ios_id_t);

void fdbuf_sign(struct srt_fd_buf *, const struct slash_fidgen *,
	lnet_process_id_t);
int  fdbuf_check(struct srt_fd_buf *, uint64_t *,
	struct slash_fidgen *, lnet_process_id_t);
void fdbuf_readkeyfile(void);
void fdbuf_checkkeyfile(void);
void fdbuf_checkkey(const char *, struct stat *);
void fdbuf_createkeyfile(void);

#endif /* _ZEST_FDBUF_H_ */
