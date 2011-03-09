/* $Id$ */
/* %PSC_COPYRIGHT% */

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
void	 mds_odtable_scan(struct odtable *, void (*)(void *, struct odtable_receipt *));

#endif /* _ODTABLE_MDS_H_ */
