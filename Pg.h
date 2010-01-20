/*
   $Id$

   Copyright (c) 2000-2010 Greg Sabino Mullane and others: see the Changes file
   Copyright (c) 1997-2000 Edmund Mergl
   Portions Copyright (c) 1994-1997 Tim Bunce

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/

#include <math.h>
#include <wchar.h>

#ifdef WIN32
static int errno;
#include <string.h>
#define strcasecmp(s1,s2) stricmp((s1), (s2))
#ifndef snprintf
#define snprintf _snprintf
#endif
#else
#include <strings.h>
#endif

#define DBDPG_TRUE (bool)1
#define DBDPG_FALSE (bool)0
#define PG_ASYNC 1
#define PG_OLDQUERY_CANCEL 2
#define PG_OLDQUERY_WAIT 4

/* Force preprocessors to use this variable. Default to something valid yet noticeable */
#ifndef PGLIBVERSION
#define PGLIBVERSION 80009
#endif

#include "libpq-fe.h"

#ifndef INV_READ
#define INV_READ 0x00040000
#endif
#ifndef INV_WRITE
#define INV_WRITE 0x00020000
#endif

#ifdef BUFSIZ
#undef BUFSIZ
#endif
/* this should improve I/O performance for large objects */
#define BUFSIZ 32768

#define NEED_DBIXS_VERSION 93

#define PERL_NO_GET_CONTEXT

#include <DBIXS.h>      /* installed by the DBI module */

#include <dbivport.h>   /* DBI portability macros */

#include <dbd_xsh.h>    /* installed by the DBI module */

DBISTATE_DECLARE;

#include "types.h"
#include "dbdimp.h"
#include "quote.h"

#define TLEVEL	     (DBIS->debug & DBIc_TRACE_LEVEL_MASK)
#define TFLAGS	     (DBIS->debug & DBIc_TRACE_FLAGS_MASK)

#define TSQL	     (TFLAGS & 256) /* Defined in DBI */

#define FLAGS_LIBPQ    0x01000000
#define FLAGS_START    0x02000000
#define FLAGS_END      0x04000000
#define FLAGS_PREFIX   0x08000000
#define FLAGS_LOGIN    0x10000000

#define TFLIBPQ      (TFLAGS & FLAGS_LIBPQ)
#define TFSTART      (TFLAGS & FLAGS_START)
#define TFEND        (TFLAGS & FLAGS_END)
#define TFPREFIX     (TFLAGS & FLAGS_PREFIX)
#define TFLOGIN      (TFLAGS & FLAGS_LOGIN)

#define TRACE1       (TLEVEL >= 1) /* Avoid using directly: DBI only */
#define TRACE2       (TLEVEL >= 2) /* Avoid using directly: DBI only */
#define TRACE3       (TLEVEL >= 3) /* Basic debugging */
#define TRACE4       (TLEVEL >= 4) /* More detailed debugging */
#define TRACE5       (TLEVEL >= 5) /* Very detailed debugging */
#define TRACE6       (TLEVEL >= 6)
#define TRACE7       (TLEVEL >= 7)
#define TRACE8       (TLEVEL >= 8)

#define TLIBPQ       (TRACE5 || TFLIBPQ)
#define TSTART       (TRACE4 || TFSTART) /* Start of a major function */
#define TEND         (TRACE4 || TFEND)   /* End of a major function   */
#define TLOGIN       (TRACE5 || TFLOGIN) /* Connect and disconnect    */

#define TRACEWARN    (TRACE1) /* Non-fatal but serious problems */

/* Do we show a "dbdpg: " header? */
#define THEADER      (TFPREFIX) ? "dbdpg: " : ""

#define TRC (void)PerlIO_printf

/* Fancy stuff for tracing of commonly used libpq functions */
#define TRACE_XX                   if (TLIBPQ) TRC(DBILOGFP,
#define TRACE_PQBACKENDPID         TRACE_XX "%sPQbackendPID\n",          THEADER)
#define TRACE_PQCANCEL             TRACE_XX "%sPQcancel\n",              THEADER)
#define TRACE_PQCLEAR              TRACE_XX "%sPQclear\n",               THEADER)
#define TRACE_PQCMDSTATUS          TRACE_XX "%sPQcmdStatus\n",           THEADER)
#define TRACE_PQCMDTUPLES          TRACE_XX "%sPQcmdTuples\n",           THEADER)
#define TRACE_PQCONNECTDB          TRACE_XX "%sPQconnectdb\n",           THEADER)
#define TRACE_PQCONSUMEINPUT       TRACE_XX "%sPQconsumeInput\n",        THEADER)
#define TRACE_PQCONSUMEINPUT       TRACE_XX "%sPQconsumeInput\n",        THEADER)
#define TRACE_PQDB                 TRACE_XX "%sPQdb\n",                  THEADER)
#define TRACE_PQENDCOPY            TRACE_XX "%sPQendcopy\n",             THEADER)
#define TRACE_PQERRORMESSAGE       TRACE_XX "%sPQerrorMessage\n",        THEADER)
#define TRACE_PQEXEC               TRACE_XX "%sPQexec\n",                THEADER)
#define TRACE_PQEXECPARAMS         TRACE_XX "%sPQexecParams\n",          THEADER)
#define TRACE_PQEXECPREPARED       TRACE_XX "%sPQexecPrepared\n",        THEADER)
#define TRACE_PQFINISH             TRACE_XX "%sPQfinish\n",              THEADER)
#define TRACE_PQFMOD               TRACE_XX "%sPQfmod\n",                THEADER)
#define TRACE_PQFNAME              TRACE_XX "%sPQfname\n",               THEADER)
#define TRACE_PQFREECANCEL         TRACE_XX "%sPQfreeCancel\n",          THEADER)
#define TRACE_PQFREEMEM            TRACE_XX "%sPQfreemem\n",             THEADER)
#define TRACE_PQFREEMEM            TRACE_XX "%sPQfreemem\n",             THEADER)
#define TRACE_PQFSIZE              TRACE_XX "%sPQfsize\n",               THEADER)
#define TRACE_PQFTABLECOL          TRACE_XX "%sPQftableCol\n",           THEADER)
#define TRACE_PQFTABLE             TRACE_XX "%sPQftable\n",              THEADER)
#define TRACE_PQFTYPE              TRACE_XX "%sPQftype\n",               THEADER)
#define TRACE_PQGETCANCEL          TRACE_XX "%sPQgetCancel\n",           THEADER)
#define TRACE_PQGETCOPYDATA        TRACE_XX "%sPQgetCopyData\n",         THEADER)
#define TRACE_PQGETISNULL          TRACE_XX "%sPQgetisnull\n",           THEADER)
#define TRACE_PQGETRESULT          TRACE_XX "%sPQgetResult\n",           THEADER)
#define TRACE_PQGETVALUE           TRACE_XX "%sPQgetvalue\n",            THEADER)
#define TRACE_PQHOST               TRACE_XX "%sPQhost\n",                THEADER)
#define TRACE_PQISBUSY             TRACE_XX "%sPQisBusy\n",              THEADER)
#define TRACE_PQNFIELDS            TRACE_XX "%sPQnfields\n",             THEADER)
#define TRACE_PQNOTIFIES           TRACE_XX "%sPQnotifies\n",            THEADER)
#define TRACE_PQNTUPLES            TRACE_XX "%sPQntuples\n",             THEADER)
#define TRACE_PQOIDVALUE           TRACE_XX "%sPQoidValue\n",            THEADER)
#define TRACE_PQOPTIONS            TRACE_XX "%sPQoptions\n",             THEADER)
#define TRACE_PQPARAMETERSTATUS    TRACE_XX "%sPQparameterStatus\n",     THEADER)
#define TRACE_PQPASS               TRACE_XX "%sPQpass\n",                THEADER)
#define TRACE_PQPORT               TRACE_XX "%sPQport\n",                THEADER)
#define TRACE_PQPREPARE            TRACE_XX "%sPQprepare\n",             THEADER)
#define TRACE_PQPROTOCOLVERSION    TRACE_XX "%sPQprotocolVersion\n",     THEADER)
#define TRACE_PQPUTCOPYDATA        TRACE_XX "%sPQputCopyData\n",         THEADER)
#define TRACE_PQPUTCOPYEND         TRACE_XX "%sPQputCopyEnd\n",          THEADER)
#define TRACE_PQRESULTERRORFIELD   TRACE_XX "%sPQresultErrorField\n",    THEADER)
#define TRACE_PQRESULTSTATUS       TRACE_XX "%sPQresultStatus\n",        THEADER)
#define TRACE_PQSENDQUERY          TRACE_XX "%sPQsendQuery\n",           THEADER)
#define TRACE_PQSENDQUERYPARAMS    TRACE_XX "%sPQsendQueryParams\n",     THEADER)
#define TRACE_PQSENDQUERYPREPARED  TRACE_XX "%sPQsendQueryPrepared\n",   THEADER)
#define TRACE_PQSERVERVERSION      TRACE_XX "%sPQserverVersion\n",       THEADER)
#define TRACE_PQSETERRORVERBOSITY  TRACE_XX "%sPQsetErrorVerbosity\n",   THEADER)
#define TRACE_PQSETNOTICEPROCESSOR TRACE_XX "%sPQsetNoticeProcessor\n",  THEADER)
#define TRACE_PQSOCKET             TRACE_XX "%sPQsocket\n",              THEADER)
#define TRACE_PQSTATUS             TRACE_XX "%sPQstatus\n",              THEADER)
#define TRACE_PQTRACE              TRACE_XX "%sPQtrace\n",               THEADER)
#define TRACE_PQTRANSACTIONSTATUS  TRACE_XX "%sPQtransactionStatus\n",   THEADER)
#define TRACE_PQUNTRACE            TRACE_XX "%sPQuntrace\n",             THEADER)
#define TRACE_PQUSER               TRACE_XX "%sPQuser\n",                THEADER)

/* end of Pg.h */
