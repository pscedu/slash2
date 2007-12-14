/* $Id$ */

#include <sys/types.h>

#include <stdlib.h>
#include <string.h>

#include "psc_util/journal.h"

#include "inode.h"
#include "sb.h"

#define SLASH_INUM_ALLOC_SZ	1024	/* allocate 1024 inums at a time */

#define SLASH_PJET_VOID		0
#define SLASH_PJET_INUM		1

struct slash_jent_inum {
	struct psc_journal_enthdr	sji_hdr;
	sl_inum_t			sji_inum;
};

sl_inum_t
slash_get_inum(struct slash_sb_mem *sbm)
{
	struct slash_jent_inum *sji;

	if (++sbm->sbm_inum % SLASH_INUM_ALLOC_SZ == 0) {
		sji = pjournal_alloclog(&sbm->sbm_pj);
		sji->sji_inum = sbm->sbm_inum;
		pjournal_logwrite(&sbm->sbm_pj, SLASH_PJET_INUM, sji);
		free(sji);
	}
	return (sbm->sbm_inum);
}

void
slash_journal_recover(struct slash_sb_mem *sbm)
{
	struct psc_journal_enthdr *pje;
	struct psc_journal_walker pjw;
	int rc, loslot;
	u32 logenid;

	logenid = -1;
	loslot = 0; /* gcc */
	memset(&pjw, 0, sizeof(pjw));
	pje = pjournal_alloclog(&sbm->sbm_pj);

	/* Locate the start of the lowest gen ID. */
	while ((rc = pjournal_walk(&sbm->sbm_pj, &pjw, pje)) == 0) {
		if ((int)(pje->pje_genid - logenid) < 0) {
			/*
			 * XXX if we wrapped, we should rescan from the
			 * beginning to find even earlier generations.
			 */
			loslot = pjw.pjw_pos;
			logenid = pje->pje_genid;
		}
	}
	if (rc == -1)
		psc_fatal("pjournal_walk");

	/* Walk starting from where the last unapplied entry was stored. */
	memset(&pjw, 0, sizeof(pjw));
	pjw.pjw_pos = pjw.pjw_stop = loslot;
	while ((rc = pjournal_walk(&sbm->sbm_pj, &pjw, pje)) == 0) {
	}
	if (rc == -1)
		psc_fatal("pjournal_walk");
	free(pje);
}
