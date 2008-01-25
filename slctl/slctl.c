/* $Id$ */

#include <sys/param.h>
#include <sys/socket.h>
#include <sys/un.h>

#include <err.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "psc_ds/list.h"
#include "psc_util/log.h"
#include "../src/control.h"
#include "psc_util/subsys.h"
#include "psc_ds/vbitmap.h"
#include "psc_util/cdefs.h"

PSCLIST_HEAD(msgs);

struct msg {
	struct psclist_head	msg_link;
	size_t			msg_size;
	struct slctlmsghdr	msg_scmh;
};

int noheader;
int inhuman;
int nsubsys;
char *subsys_names;

int
lookupshow(const char *name)
{
	struct {
		const char	*s_name;
		int		 s_value;
	} showtab[] = {
		{ "log",		SCMT_GETLOGLEVEL },
		{ "stats",		SCMT_GETSTATS },
	};
	int n;

	if (strlen(name) == 0)
		return (-1);

	for (n = 0; n < NENTRIES(showtab); n++)
		if (strncasecmp(name, showtab[n].s_name,
		    strlen(name)) == 0)
			return (showtab[n].s_value);
	return (-1);
}

void *
pushmsg(int type, size_t siz)
{
	struct msg *msg;
	static int id;
	size_t tsiz;

	tsiz = siz + sizeof(*msg);
	if ((msg = malloc(tsiz)) == NULL)
		err(2, "malloc");
	memset(msg, 0, tsiz);
	psclist_add_tail(&msg->msg_link, &msgs);
	msg->msg_scmh.scmh_type = type;
	msg->msg_scmh.scmh_size = siz;
	msg->msg_scmh.scmh_id = id++;
	msg->msg_size = siz + sizeof(msg->msg_scmh);
	return (&msg->msg_scmh.scmh_data);
}

void
handlehashtable(const char *tblname)
{
	struct slctlmsg_hashtable *sht;

	sht = pushmsg(SCMT_GETHASHTABLE, sizeof(*sht));
	snprintf(sht->sht_name, sizeof(sht->sht_name), "%s", tblname);
}

void
parseshowspec(char *showspec)
{
	char *thrlist, *thr, *thrnext;
	struct slctlmsg_loglevel *sll;
	struct slctlmsg_subsys *sss;
	struct slctlmsg_stats *sst;
	int n, type;

	if ((thrlist = strchr(showspec, ':')) == NULL)
		thrlist = STHRNAME_EVERYONE;
	else
		*thrlist++ = '\0';

	if ((type = lookupshow(showspec)) == -1)
		errx(1, "invalid show parameter: %s", showspec);

	for (thr = thrlist; thr != NULL; thr = thrnext) {
		if ((thrnext = strchr(thr, ',')) != NULL)
			*thrnext++ = '\0';

		switch (type) {
		case SCMT_GETLOGLEVEL:
			sss = pushmsg(SCMT_GETSUBSYS, sizeof(*sss));

			sll = pushmsg(type, sizeof(*sll));
			n = snprintf(sll->sll_thrname,
			   sizeof(sll->sll_thrname), "%s", thr);
			if (n == -1)
				err(1, "snprintf");
			else if (n == 0 ||
			    n > (int)sizeof(sll->sll_thrname))
				err(1, "invalid thread name: %s", thr);
			break;
		case SCMT_GETSTATS:
			sst = pushmsg(type, sizeof(*sst));
			n = snprintf(sst->sst_thrname,
			   sizeof(sst->sst_thrname), "%s", thr);
			if (n == -1)
				err(1, "snprintf");
			else if (n == 0 ||
			    n > (int)sizeof(sst->sst_thrname))
				err(1, "invalid thread name: %s", thr);
			break;
		}
	}
}

void
parselc(char *lists)
{
	struct slctlmsg_lc *slc;
	char *list, *listnext;
	int n;

	for (list = lists; list != NULL; list = listnext) {
		if ((listnext = strchr(list, ',')) != NULL)
			*listnext++ = '\0';

		slc = pushmsg(SCMT_GETLC, sizeof(*slc));

		n = snprintf(slc->slc_name, sizeof(slc->slc_name),
		    "%s", list);
		if (n == -1)
			err(1, "snprintf");
		else if (n == 0 || n > (int)sizeof(slc->slc_name))
			errx(1, "invalid list: %s", list);
	}
}

void
parseparam(char *spec)
{
	char *thr, *field, *value;
	struct slctlmsg_param *sp;
	int n;

	if ((value = strchr(spec, '=')) != NULL)
		*value++ = '\0';

	if ((field = strchr(spec, '.')) == NULL) {
		thr = STHRNAME_EVERYONE;
		field = spec;
	} else {
		*field = '\0';

		if (strstr(spec, "thr") == NULL) {
			/* No thread specified; assume global or everyone. */
			thr = STHRNAME_EVERYONE;
			*field = '.';
			field = spec;
		} else {
			/* We saw "thr" at the first level; assume thread specification. */
			thr = spec;
			field++;
		}
	}

	sp = pushmsg(value ? SCMT_SETPARAM : SCMT_GETPARAM, sizeof(*sp));

	/* Set thread name. */
	n = snprintf(sp->sp_thrname, sizeof(sp->sp_thrname), "%s", thr);
	if (n == -1)
		err(1, "snprintf");
	else if (n == 0 || n > (int)sizeof(sp->sp_thrname))
		errx(1, "invalid thread name: %s", thr);

	/* Set parameter name. */
	n = snprintf(sp->sp_field,
	    sizeof(sp->sp_field), "%s", field);
	if (n == -1)
		err(1, "snprintf");
	else if (n == 0 || n > (int)sizeof(sp->sp_field))
		errx(1, "invalid parameter: %s", thr);

	/* Set parameter value (if applicable). */
	if (value) {
		n = snprintf(sp->sp_value,
		    sizeof(sp->sp_value), "%s", value);
		if (n == -1)
			err(1, "snprintf");
		else if (n == 0 || n > (int)sizeof(sp->sp_value))
			errx(1, "invalid parameter value: %s", thr);
	}
}

#if 0
void
parseiostat(char *iostats)
{
	struct slctlmsg_iostats *sist;
	char *iostat, *next;
	int n;

	for (iostat = iostats; iostat != NULL; iostat = next) {
		if ((next = strchr(iostat, ',')) != NULL)
			*next++ = '\0';

		sist = pushmsg(SCMT_GETIOSTAT, sizeof(*sist));

		/* Set iostat name. */
		n = snprintf(sist->sist_ist.ist_name,
		    sizeof(sist->sist_ist.ist_name), "%s", iostat);
		if (n == -1)
			err(1, "snprintf");
		else if (n == 0 || n > (int)sizeof(sist->sist_ist.ist_name))
			errx(1, "invalid iostat name: %s", iostat);

	}
}
#endif

__inline int
thread_namelen(void)
{
	return (12); /* "slrpcmdsthr%d" */
}

int
loglevel_namelen(int n)
{
	size_t maxlen;
	int j;

	maxlen = strlen(&subsys_names[n * SSS_NAME_MAX]);
	for (j = 0; j < PNLOGLEVELS; j++)
		maxlen = MAX(maxlen, strlen(psclog_name(j)));
	return (maxlen);
}

void
humanscale(char buf[8], double num)
{
	int mag;

	/*
	 * 1234567
	 * 1000.3K
	 */
	for (mag = 0; num > 1024.0; mag++)
		num /= 1024.0;
	if (mag > 6)
		snprintf(buf, sizeof(buf), "%.1e", num);
	else
		snprintf(buf, sizeof(buf), "%6.1f%c", num, "BKMGTPE"[mag]);
}

void
prscm(const struct slctlmsghdr *scmh, const void *scm)
{
	static int lastmsgtype = -1;
	static int last_thrtype = -1;

	const struct slctlmsg_hashtable *sht;
	const struct slctlmsg_loglevel *sll;
	const struct slctlmsg_errmsg *sem;
	const struct slctlmsg_subsys *sss;
	const struct slctlmsg_stats *sst;
	const struct slctlmsg_param *sp;
	const struct slctlmsg_lc *slc;
	int type, n, len;

	if (!noheader && lastmsgtype != scmh->scmh_type &&
	    lastmsgtype != -1)
		printf("\n");
	switch (scmh->scmh_type) {
	case SCMT_ERRMSG:
		sem = scm;
		if (scmh->scmh_size != sizeof(*sem))
			errx(2, "invalid msg size; type=%d; size=%zu; "
			    "expected=%zu", scmh->scmh_type,
			    scmh->scmh_size, sizeof(*sem));
		printf("error: %s\n", sem->sem_errmsg);
		break;
	case SCMT_GETSUBSYS:
		sss = scm;
		if (scmh->scmh_size == 0 ||
		    scmh->scmh_size % SSS_NAME_MAX)
			errx(2, "invalid msg size; type=%d; sizeof=%zu "
			    "minimal=%zu", scmh->scmh_type,
			    scmh->scmh_size, sizeof(*sss));
		nsubsys = scmh->scmh_size / SSS_NAME_MAX;
		subsys_names = PSCALLOC(scmh->scmh_size);
		memcpy(subsys_names, sss->sss_names, scmh->scmh_size);
		break;
	case SCMT_GETLOGLEVEL:
		sll = scm;
		if (scmh->scmh_size != sizeof(*sll))
			errx(2, "invalid msg size; type=%d; sizeof=%zu "
			    "expected=%zu", scmh->scmh_type,
			    scmh->scmh_size, sizeof(*sll));
		if (!noheader && lastmsgtype != SCMT_GETLOGLEVEL) {
			len = 0;
			printf("logging levels\n");
			len += printf(" %-*s ", thread_namelen(), "thread");
			for (n = 0; n < nsubsys; n++)
				len += printf(" %*s", loglevel_namelen(n),
				    &subsys_names[n * SSS_NAME_MAX]);
			printf("\n");
			for (n = 0; n < len; n++)
				putchar('=');
			printf("\n");
		}

		printf(" %-*s ", thread_namelen(), sll->sll_thrname);
		for (n = 0; n < nsubsys; n++)
			printf(" %*s", loglevel_namelen(n),
			    psclog_name(sll->sll_levels[n]));
		printf("\n");
		break;
	case SCMT_GETHASHTABLE:
		sht = scm;
		if (scmh->scmh_size != sizeof(*sht))
			errx(2, "invalid msg size; type=%d; size=%zu; "
			    "expected=%zu", scmh->scmh_type,
			    scmh->scmh_size, sizeof(*sht));
		if (!noheader && lastmsgtype != SCMT_GETHASHTABLE) {
			len = 0;
			printf("hash table statistics\n");
			len += printf("%12s %6s %6s %7s %6s %6s %6s",
			    "table", "total", "used", "%use", "ents",
			    "avglen", "maxlen");
			putchar('\n');
			for (n = 0; n < len; n++)
				putchar('=');
			putchar('\n');
		}
		printf("%12s %6d %6d %6.2f%% %6d %6.1f %6d\n",
		    sht->sht_name,
		    sht->sht_totalbucks, sht->sht_usedbucks,
		    sht->sht_usedbucks * 100.0 / sht->sht_totalbucks,
		    sht->sht_nents,
		    sht->sht_nents * 1.0 / sht->sht_totalbucks,
		    sht->sht_maxbucklen);
		break;
	case SCMT_GETSTATS:
		sst = scm;
		if (scmh->scmh_size < sizeof(*sst))
			errx(2, "invalid msg size");

		if (lastmsgtype != SCMT_GETSTATS) {
			printf("thread stats\n");
			last_thrtype = -1;
		}

		type = -1;
		switch (sst->sst_thrtype) {
		case SLTHRT_CTL:
		case SLTHRT_RPCMDS:
		case SLTHRT_RPCIO:
			type = sst->sst_thrtype;
			break;
		}

		/* print subheader for each thread type */
		if (last_thrtype != type) {
			if (lastmsgtype == SCMT_GETSTATS)
				printf("\n");
			len = 0;
			switch (type) {
			case SLTHRT_CTL:
				len += printf(" %-*s %8s", thread_namelen(),
				     "thread", "#clients");
				break;
			case SLTHRT_RPCMDS:
				len += printf(" %-*s %8s %8s %8s",
				     thread_namelen(), "thread",
				     "#open", "#close", "#stat");
				break;
			case SLTHRT_RPCIO:
				len += printf(" %-*s %8s", thread_namelen(),
				    "thread", "#write");
				break;
			default:
				len += printf(" %-*s %11s", thread_namelen(),
				    "thread", "");
				break;
			}
			putchar('\n');
			for (n = 0; n < len; n++)
				putchar('=');
			putchar('\n');
		}
		last_thrtype = type;

		/* print thread stats */
		switch (type) {
		case SLTHRT_CTL:
			len += printf(" %-*s %8u\n", thread_namelen(),
			    sst->sst_thrname, sst->sst_nclients);
			break;
		default:
			printf(" %-*s <no stats>\n", thread_namelen(),
			    sst->sst_thrname);
			break;
		}
		break;
	case SCMT_GETLC:
		slc = scm;
		if (scmh->scmh_size < sizeof(*slc))
			errx(2, "invalid msg size");
		if (!noheader && lastmsgtype != SCMT_GETLC) {
			len = 0;
			printf("list caches\n");
			len += printf(" %20s %8s %9s %8s",
			    "list", "size", "max", "#seen");
			putchar('\n');
			for (n = 0; n < len; n++)
				putchar('=');
			putchar('\n');
		}
		printf(" %20s %8zu ", slc->slc_name, slc->slc_size);
		if (slc->slc_max == (size_t)-1)
			printf(" %9s", "unlimited");
		else
			printf(" %9zu", slc->slc_max);
		printf("%8zu\n", slc->slc_nseen);
		break;
	case SCMT_GETPARAM:
		sp = scm;
		if (scmh->scmh_size < sizeof(*sp))
			errx(2, "invalid msg size");
		if (!noheader && lastmsgtype != SCMT_GETPARAM) {
			len = 0;
			printf("parameters\n");
			len += printf(" %-30s %s", "name", "value");
			putchar('\n');
			for (n = 0; n < len; n++)
				putchar('=');
			putchar('\n');
		}
		if (strcmp(sp->sp_thrname, STHRNAME_EVERYONE) == 0)
			printf(" %-30s %s\n", sp->sp_field, sp->sp_value);
		else
			printf(" %s.%s %s\n", sp->sp_thrname,
			    sp->sp_field, sp->sp_value);
		break;
#if 0
	case SCMT_GETIOSTAT:
		sist = scm;
		if (scmh->scmh_size < sizeof(*sist))
			errx(2, "invalid msg size");
		if (!noheader && lastmsgtype != SCMT_GETIOSTAT) {
			len = 0;
			printf("iostats\n");
			len += printf(" %-12s %9s %8s %8s %8s",
			    "name", "ratecur", "total", "erate", "toterr");
			putchar('\n');
			for (n = 0; n < len; n++)
				putchar('=');
			putchar('\n');
		}
		printf(" %-12s ", sist->sist_ist.ist_name);
		if (inhuman) {
			printf("%8.2f ", sist->sist_ist.ist_rate);
			printf("%8"_P_LP64"u ", sist->sist_ist.ist_bytes_total);
		} else {
			humanscale(buf, sist->sist_ist.ist_rate);
			printf("%7s/s ", buf);

			humanscale(buf, sist->sist_ist.ist_bytes_total);
			printf("%8s ", buf);
		}
		printf("%6.1f/s %8"_P_LP64"u\n",
		    sist->sist_ist.ist_erate,
		    sist->sist_ist.ist_errors_total);
		break;
#endif
	default:
		printf("Received unknown slctlmsg (type=%d)\n",
		    scmh->scmh_type);
		break;
	}
	lastmsgtype = scmh->scmh_type;
}

const char *progname;

__dead void
usage(void)
{
	fprintf(stderr,
	    "usage: %s [-HI] [-h table] [-i iostat] [-L listspec]\n"
	    "\t[-p param[=value]] [-S socket] [-s value]\n",
	    progname);
	exit(1);
}

int
main(int argc, char *argv[])
{
	struct psclist_head *ent, *nextent;
	struct sockaddr_un sun;
	struct slctlmsghdr scmh;
	const char *sockfn;
	struct msg *msg;
	size_t scmsiz;
	void *scm;
	ssize_t n;
	int c, s;

	sockfn = _PATH_SLCTLSOCK;
	progname = argv[0];
	while ((c = getopt(argc, argv, "Hh:Ii:L:M:m:p:S:s:")) != -1)
		switch (c) {
		case 'H':
			noheader = 1;
			break;
		case 'h':
			handlehashtable(optarg);
			break;
		case 'I':
			inhuman = 1;
			break;
//		case 'i':
//			parseiostat(optarg);
//			break;
		case 'L':
			parselc(optarg);
			break;
		case 'p':
			parseparam(optarg);
			break;
		case 'S':
			sockfn = optarg;
			break;
		case 's':
			parseshowspec(optarg);
			break;
		default:
			usage();
		}

	argc -= optind;
	if (psclist_empty(&msgs) || argc)
		usage();

	/* Connect to control socket. */
	if ((s = socket(AF_UNIX, SOCK_STREAM, 0)) == -1)
		err(2, "socket");

	bzero(&sun, sizeof(sun));
	sun.sun_family = AF_UNIX;
	snprintf(sun.sun_path, sizeof(sun.sun_path), "%s", sockfn);
	if (connect(s, (struct sockaddr *)&sun, sizeof(sun)) == -1)
		err(2, "connect: %s", sockfn);

	/* Send queued control messages. */
	psclist_for_each_safe(ent, nextent, &msgs) {
		msg = psclist_entry(ent, struct msg, msg_link);
		if (write(s, &msg->msg_scmh,
		    msg->msg_size) != (ssize_t)msg->msg_size)
			err(2, "write");
		free(msg);
	}
	if (shutdown(s, SHUT_WR) == -1)
		err(2, "shutdown");

	/* Read and print response messages. */
	scm = NULL;
	scmsiz = 0;
	while ((n = read(s, &scmh, sizeof(scmh))) != -1 && n != 0) {
		if (n != sizeof(scmh)) {
			warnx("short read");
			continue;
		}
		if (scmh.scmh_size == 0)
			errx(2, "received invalid message from slashd");
		if (scmh.scmh_size >= scmsiz) {
			scmsiz = scmh.scmh_size;
			if ((scm = realloc(scm, scmsiz)) == NULL)
				err(1, "realloc");
		}
		n = read(s, scm, scmh.scmh_size);
		if (n == -1)
			err(2, "read");
		else if (n == 0)
			errx(2, "received unexpected EOF from slashd");
		prscm(&scmh, scm);
	}
	if (n == -1)
		err(2, "read");
	free(scm);
	close(s);
	exit(0);
}
