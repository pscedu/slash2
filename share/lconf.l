/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2007-2011, Pittsburgh Supercomputing Center (PSC).
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

%{
#define YYSTYPE char *

#include <err.h>
#include <stdio.h>
#include <string.h>

#include "pfl/str.h"
#include "psc_util/alloc.h"
#include "psc_util/log.h"

#include "slconfig.h"
#include "yconf.h"

extern char	 cfg_filename[];
extern int	 cfg_lineno;

extern char	*yylval;

extern struct sl_site	  *currentSite;
extern struct sl_resource *currentRes;
extern struct sl_resm	  *currentResm;
%}

%option nounput

floatv		[0-9]*\.?[0-9]+
globpath	[/][a-zA-Z0-9|/.,_\[\]+?-]+
hexnum		0[Xx][0-9A-Fa-f]+
ipquad		[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*
lnetname	tcp[0-9]+|sdp[0-9]+
name		[A-Za-z][0-9A-Za-z_.-]+
num		[0-9]+
pathname	[/][a-zA-Z0-9/._-]+
quotedstr	\"[^"]*\"
sizeval		[0-9]+[KMGTBkmgtb]?

%%

include		{ return (INCLUDE); }
global		{ yywarn("`global' deprecated by `set'"); return (SET); }
set		{ return (SET); }
site		{ return (SITE_PROFILE); }
resource	{ return (RESOURCE_PROFILE); }
peers|ios	{ return (PEERS); }
nids		{ return (NIDS); }

yes|no {
	psclog_trace("BOOL %s", yytext);
	yylval = pfl_strdup(yytext);
	return (BOOL);
}

parallel_lfs|cluster_noshare_lfs|compute {
	psclog_trace("Resource type string %s", yytext);
	yylval = pfl_strdup(yytext);
	return (RESOURCE_TYPE);
}

parallell_lfs_compnt|standalone_fs|archival_fs|mds {
	psc_assert(!currentResm);
	currentResm = PSCALLOC(sizeof(struct sl_resm) + cfg_resm_pri_sz);
	slcfg_init_resm(currentResm);
	psc_dynarray_add(&currentRes->res_members, currentResm);
	currentResm->resm_res = currentRes;
	psclog_trace("Resource type string %s", yytext);
	yylval = pfl_strdup(yytext);
	return (RESOURCE_TYPE);
}

{ipquad} {
	psclog_trace("IPADRRESS %s", yytext);
	yylval = pfl_strdup(yytext);
	return (IPADDR);
}

{num} {
	psclog_trace("NUM %s", yytext);
	yylval = pfl_strdup(yytext);
	return (NUM);
}

{hexnum} {
	psclog_trace("HEXNUM %s", yytext);
	yylval = pfl_strdup(yytext);
	return (HEXNUM);
}

{lnetname} {
	psclog_trace("LNETNAME string %s", yytext);
	yylval = pfl_strdup(yytext);
	return (LNETNAME);
}

{name} {
	psclog_trace("NAME %s", yytext);
	yylval = pfl_strdup(yytext);
	return (NAME);
}

{pathname} {
	psclog_trace("PATHNAME %s", yytext);
	yylval = pfl_strdup(yytext);
	return (PATHNAME);
}

{globpath} {
	psclog_trace("GLOBPATH %s", yytext);
	yylval = pfl_strdup(yytext);
	return (GLOBPATH);
}

{floatv} {
	psclog_trace("FLOATVAL %s", yytext);
	yylval = pfl_strdup(yytext);
	return (FLOATVAL);
}

{sizeval} {
	psclog_trace("SIZEVAL %s", yytext);
	yylval = pfl_strdup(yytext);
	return (SIZEVAL);
}

{quotedstr} {
	size_t len;

	psclog_trace("Quoted string %s", yytext);
	len = strlen(yytext);
	psc_assert(len > 1);

	/* len = q["] + str + q["] */
	/* remove 2 for the quotes but add 1 for NUL; total: subtract 1 */
	len--;

	yylval = PSCALLOC(len);
	strlcpy(yylval, yytext + 1, len);
	return (QUOTEDS);
}

[ \t]*#.*	;
[ \t]+		;
[\n]		cfg_lineno++;

.		{ return yytext[0]; }

%%

int
yywrap(void)
{
	return (1);
}
