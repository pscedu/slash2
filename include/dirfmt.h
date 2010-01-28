/* $Id: dirfmt.h 9699 2010-01-07 18:36:46Z yanovich $ */
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

#ifndef _DIRFMT_H_
#define _DIRFMT_H_

struct slash_dirblk {
	int	db_free;		/* free space in the block */
	int	db_blkno;		/* where is the block */
	u32	db_minhash;		/* min hash value of names */
	u32	db_maxhash;		/* max hash vale of names */
};

#endif /* _DIRFMT_H_ */
