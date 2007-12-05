/* $Id$ */

#include "psc_util/journal.h"


#define SLASH_INUM_ALLOC_SZ	1024	/* allocate 1024 inums at a time */

#define SLASH_PJET_VOID		0
#define SLASH_PJET_INUM		1

struct slash_jent_inum {
	struct psc_journal_enthdr	sji_hdr;
	slash_inum_t			sji_inum;
};

slash_inum_t
slash_get_inum(struct slash_sb_mem *sbm)
{
	struct slash_jent_inum *sji;

	if (++sbm->sbm_inum % INUM_ALLOC_SZ == 0) {
		sji = pjournal_alloclog(sbm->sbm_pj);
		sji->sgi_inum = sbm->sbm_inum;
		pjournal_writelog(sbm->sbm_pj, SLASH_PJET_INUM, sji);
		free(sji);
	}
	return (sbm->sbm_inum);
}

void
slash_journal_recover(struct slash_sb_mem *sbm)
{
	struct psc_journal_enthdr *pje;
	struct psc_journal_walker pjw;
	u32 logenid;
	int loslot;

	logenid = -1;
	loslot = 0; /* gcc */
	memset(&pjw, 0, sizeof(pjw));
	pje = pjournal_alloclog(pj);

	/* Locate the start of the lowest gen ID. */
	while ((rc = pjournal_walk(sbm->sbm_pj, &pjw, pje)) == 0) {
		if ((int)(pje->pje_genid - logenid) < 0) {
			/*
			 * XXX if we wrapped, we should rescan from the
			 * beginning to find even earlier generations.
			 */
			loslot = pjw->pjw_pos;
			logenid = pje->pje_genid;
		}
	}
	if (rc == -1)
		pfatal("pjournal_walk");

	/* Walk starting from where the last unapplied entry was stored. */
	memset(&pjw, 0, sizeof(pjw));
	pjw.pjw_pos = pjw.pjw_stop = loslot;
	while ((rc = pjournal_walk(sbm->sbm_pj, &pjw, pje)) == 0) {
	}
	if (rc == -1)
		pfatal("pjournal_walk");
	free(pje);
}
