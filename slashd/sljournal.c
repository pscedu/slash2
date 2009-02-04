/* $Id$ */

#include <sys/param.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "psc_util/journal.h"

#include "pathnames.h"
#include "slconfig.h"
#include "sljournal.h"
#include "slashdthr.h"
#include "sb.h"

#if 0
sl_inum_t
slmds_get_inum(void)
{
	struct slmds_jent_inum *sji;

	if (++sbm.sbm_sbs->sbs_inum % SLMDS_INUM_ALLOC_SZ == 0) {
		sji = pjournal_alloclog(&sbm.sbm_pj);
		sji->sji_inum = sbm.sbm_sbs->sbs_inum;
		pjournal_logwrite(&sbm.sbm_pj, SLJ_MDS_PJET_INUM, sji);
		psc_freel(sji, PJ_PJESZ(&sbm.sbm_pj));
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

#ifdef AAA
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
#endif
	psc_freel(pje, PJ_PJESZ(&sbm.sbm_pj));
}

#endif
