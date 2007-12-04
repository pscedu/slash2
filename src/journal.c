/* $Id$ */

#define INUM_ALLOC_SZ 1024	/* allocate 1024 inums at a time */

void
slash_get_inum(struct slash_sb *sb)
{
	if (++sb->sb_inum % INUM_ALLOC_SZ == 0)
		slash_sb_update(sb);
	return (sb->sb_inum);
}
