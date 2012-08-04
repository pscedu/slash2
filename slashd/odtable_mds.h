/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2011-2012, Pittsburgh Supercomputing Center (PSC).
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
 * odtable: on-disk table for persistent storage of otherwise memory
 * resident data structures.
 *
 * This API is complementary to the vanilla odtable API provided by PFL
 * to write the same format through the ZFS backend so the odtables are
 * contained within the ZFS backend metadata file system.
 */

#ifndef _ODTABLE_MDS_H_
#define _ODTABLE_MDS_H_

void	 mds_odtable_load(struct odtable **, const char *, const char *, ...);
struct odtable_receipt *
	 mds_odtable_putitem(struct odtable *, void *, size_t);
int	 mds_odtable_getitem(struct odtable *, const struct odtable_receipt *, void *, size_t);
int	 mds_odtable_freeitem(struct odtable *, struct odtable_receipt *);
struct odtable_receipt *
	 mds_odtable_replaceitem(struct odtable *, struct odtable_receipt *, void *, size_t);
void	 mds_odtable_release(struct odtable *);
void	 mds_odtable_scan(struct odtable *, int (*)(void *, struct odtable_receipt *, void *), void *);

#endif /* _ODTABLE_MDS_H_ */
