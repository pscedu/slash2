/* $Id$ */

#include <sys/types.h>
#include <sys/time.h>

#include <unistd.h>

#include "psc_util/atomic.h"
#include "psc_util/cdefs.h"
#include "psc_util/iostats.h"
#include "psc_util/thread.h"
#include "psc_util/waitq.h"

#include "mount_slash.h"

struct psc_wait_queue	timerwtq;
struct psc_thread	mstintvthr;
struct psc_thread	mstiosthr;

void *
mstintvthr_main(__unusedx void *arg)
{
	for (;;) {
		sleep(1);
		psc_waitq_wakeall(&timerwtq);
		sched_yield();
	}
}

void *
mstiosthr_main(__unusedx void *arg)
{
	struct iostats *ist;
	struct timeval tv;
	unsigned long intv;

	for (;;) {
		psc_waitq_wait(&timerwtq, NULL);

		spinlock(&pscIostatsListLock);
		if (gettimeofday(&tv, NULL) == -1)
			psc_fatal("gettimeofday");
		psclist_for_each_entry(ist, &pscIostatsList, ist_lentry) {
			if (tv.tv_sec != ist->ist_lasttv.tv_sec) {
				timersub(&tv, &ist->ist_lasttv,
				    &ist->ist_intv);
				ist->ist_lasttv = tv;

				intv = 0;
				intv = atomic_xchg(&ist->ist_bytes_intv, intv);
				ist->ist_rate = intv /
				    ((ist->ist_intv.tv_sec * 1e6 +
				    ist->ist_intv.tv_usec) * 1e-6);
				ist->ist_bytes_total += intv;

				intv = 0;
				intv = atomic_xchg(&ist->ist_errors_intv, intv);
				ist->ist_erate = intv /
				    ((ist->ist_intv.tv_sec * 1e6 +
				    ist->ist_intv.tv_usec) * 1e-6);
				ist->ist_errors_total += intv;
			}
		}
		freelock(&pscIostatsListLock);
		sched_yield();
	}
}

void
mstimerthr_spawn(void)
{
	psc_waitq_init(&timerwtq);

	pscthr_init(&mstintvthr, MSTHRT_TINTV,
	    mstintvthr_main, NULL, "mstintvthr");
	pscthr_init(&mstiosthr, MSTHRT_TIOS,
	    mstiosthr_main, NULL, "mstiosthr");
}
