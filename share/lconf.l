/* $Id$ */
/*
 * %PSC_START_COPYRIGHT%
 * -----------------------------------------------------------------------------
 * Copyright (c) 2006-2010, Pittsburgh Supercomputing Center (PSC).
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

#include <stdio.h>
#include <string.h>

#include "psc_util/alloc.h"
#include "psc_util/log.h"
#include "psc_util/strlcpy.h"

#include "yconf.h"

extern int cfg_lineno;
extern char *yylval;

void yyerror(const char *, ...);
%}

%option nounput

num		[0-9]+
hexnum		0[Xx][0-9A-Fa-f]+
sitename	@[A-Za-z]+
char		[A-Za-z]
ipquad		[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}
name		[A-Za-z][0-9A-Za-z_.-]+
floatv		[0-9]*\.?[0-9]+
resname		[A-Za-z][0-9A-Za-z]+@[A-Z]+
lnettcp		tcp[0-9]+
sizeval		[0-9]+[KMGTBkmgtb]
pathname	[/][a-zA-Z0-9/._-]+
globpath	[/][a-zA-Z0-9|/.,_\[\]+?-]+
quotedstr	\"[^"]*\"

%%

";" {
	return END;
}

"," {
	psc_trace("Sep ','");
	return NSEP;
}

"=" {
	return EQ;
}

include {
	return INCLUDE;
}

global {
	psc_trace("GLOBAL %s", yytext);
	return GLOBAL;
}

site {
	psc_trace("SITETAG %s", yytext);
	return SITE_PROFILE;
}

resource {
	psc_trace("RESOURCE_PROFILE %s", yytext);
	return RESOURCE_PROFILE;
}

peers {
	psc_trace("PEERTAG %s", yytext);
	return PEERTAG;
}

ifs {
	psc_trace("INTERFACETAG %s", yytext);
	return INTERFACETAG;
}

"{" {
	psc_trace("SUBSECT_START TAG %s", yytext);
	return SUBSECT_START;
}

"}" {
	psc_trace("SUBSECT_END TAG %s", yytext);
	return SUBSECT_END;
}

yes {
	psc_trace("BOOL %s", yytext);
	yylval = strdup(yytext);
	return BOOL;
}

no {
	psc_trace("BOOL %s", yytext);
	yylval = strdup(yytext);
	return BOOL;
}

{num} {
	psc_trace("NUM %s", yytext);
	yylval = strdup(yytext);
	return NUM;
}

parallel_fs {
	psc_trace("Resource type string %s", yytext);
	yylval = strdup(yytext);
	return RESOURCE_TYPE;
}

archival_fs {
	psc_trace("Resource type string %s", yytext);
	yylval = strdup(yytext);
	return RESOURCE_TYPE;
}

mds {
	psc_trace("Resource type string %s", yytext);
	yylval = strdup(yytext);
	return RESOURCE_TYPE;
}

cluster_noshare_fs {
	psc_trace("Resource type string %s", yytext);
	yylval = strdup(yytext);
	return RESOURCE_TYPE;
}

compute {
	psc_trace("Resource type string %s", yytext);
	yylval = strdup(yytext);
	return RESOURCE_TYPE;
}

{hexnum} {
	psc_trace("HEXNUM %s", yytext);
	yylval = strdup(yytext);
	return HEXNUM;
}

{lnettcp} {
	psc_trace("Lnet tcp network string %s", yytext);
	yylval = strdup(yytext);
	return LNETTCP;
}

{name} {
	psc_trace("NAME %s", yytext);
	yylval = strdup(yytext);
	return NAME;
}

{pathname} {
	psc_trace("PATHNAME %s", yytext);
	yylval = strdup(yytext);
	return PATHNAME;
}

{globpath} {
	psc_trace("GLOBPATH %s", yytext);
	yylval = strdup(yytext);
	return GLOBPATH;
}

{floatv} {
	psc_trace("FLOATVAL %s", yytext);
	yylval = strdup(yytext);
	return FLOATVAL;
}

{sizeval} {
	psc_trace("SIZEVAL %s", yytext);
	yylval = strdup(yytext);
	return SIZEVAL;
}

{sitename} {
	psc_trace("SITE_NAME %s", yytext);
	yylval = strdup(yytext);
	return SITE_NAME;
}

{resname} {
	psc_trace("RESOURCE_NAME %s", yytext);
	yylval = strdup(yytext);
	return RESOURCE_NAME;
}

{ipquad} {
	psc_trace("IPADRRESS %s", yytext);
	yylval = strdup(yytext);
	return IPADDR;
}

{quotedstr} {
	size_t len;

	psc_trace("Quoted string %s", yytext);
	len = strlen(yytext);
	psc_assert(len > 1);

	/* len = q["] + str + q["] */
	/* remove 2 for the quotes but add 1 for NUL; total: subtract 1 */
	len--;

	yylval = PSCALLOC(len);
	strlcpy(yylval, yytext + 1, len);
	return QUOTEDS;
}

@ {
	return (ATSIGN);
}

[ \t]*#.*	;
[ \t]+		;
[\n]		cfg_lineno++;

.		yyerror("Unrecognized character: %s", yytext);

%%

int
yywrap(void)
{
	return (1);
}
