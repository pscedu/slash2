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

/*
 * Definitions for the directory entry format and the directory B+tree that lives
 * in a POSIX file.
 */

/* Directory block, each block contains pointers to lower level blocks or directory
   entries at the leaf level */
struct slash_dirblk {
	u32	db_magic;		/* magic number */
	u32	db_checksum;		/* CRC */
	int	db_level;		/* tree level */
	int	db_count;		/* number of records */
	u32	db_left;		/* my left neighbor */
	u32	db_right;		/* my right neighbor */
};

/* Directory record, each record describes a leaf directory block */
struct slash_dirrec {			
	int	dr_free;		/* free space in the block */
	int	dr_blkno;		/* where is the block */
	u32	dr_minhash;		/* min hash value of names */
	u32	dr_maxhash;		/* max hash value of names */
};

	
#endif /* _DIRFMT_H_ */
