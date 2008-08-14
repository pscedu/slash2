/* $Id$ */

#include <sys/param.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "psc_util/journal.h"


sl_inum_t
slmds_get_inum(void)
{
	struct slash_jent_inum *sji;

	if (++sbm.sbm_sbs->sbs_inum % SLASH_INUM_ALLOC_SZ == 0) {
		sji = pjournal_alloclog(&sbm.sbm_pj);
		sji->sji_inum = sbm.sbm_sbs->sbs_inum;
		pjournal_logwrite(&sbm.sbm_pj, SLASH_PJET_INUM, sji);
		free(sji);
	}
	return (sbm.sbm_sbs->sbs_inum);
}

void
slmds_journal_recover(void)
{
	struct psc_journal_enthdr *pje;
	struct psc_journal_walker pjw;
	int rc, loslot;
	u32 logenid;

	logenid = -1;
	loslot = 0; /* gcc */
	memset(&pjw, 0, sizeof(pjw));
	pje = pjournal_alloclog(&sbm.sbm_pj);

	/* Locate the start of the lowest gen ID. */
	while ((rc = pjournal_walk(&sbm.sbm_pj, &pjw, pje)) == 0) {
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
	while ((rc = pjournal_walk(&sbm.sbm_pj, &pjw, pje)) == 0) {
	}
	if (rc == -1)
		psc_fatal("pjournal_walk");
	free(pje);
}

void
sl_journal_init(struct psc_journal *pj, size_t nent, size_t entsz, int ra)
{
	char fn[PATH_MAX];
	int rc;

	rc = snprintf(fn, sizeof(fn), "%s/%s",
		      nodeInfo.node_res->res_fsroot, _PATH_SLJOURNAL);
	if (rc == -1)
		psc_fatal("snprintf");

	pjournal_init(pj, fn, 0, nent, entsz, ra);
}
