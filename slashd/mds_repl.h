/* $Id$ */

#ifndef _SL_MDS_REPL_H_
#define _SL_MDS_REPL_H_

int	mds_repl_addreq(struct slash_fidgen *, sl_blkno_t);
int	mds_repl_delreq(struct slash_fidgen *, sl_blkno_t);
int	mds_repl_inv_except_locked(struct bmapc_memb *, sl_ios_id_t);

#endif /* _SL_MDS_REPL_H_ */
