/*
   $Id$

   Copyright (c) 2000-2008 Greg Sabino Mullane and others: see the Changes file
   Copyright (c) 1997-2000 Edmund Mergl
   Portions Copyright (c) 1994-1997 Tim Bunce

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


#ifdef WIN32
static int errno;
#endif

#define DBDPG_TRUE (bool)1
#define DBDPG_FALSE (bool)0
#define PG_ASYNC 1
#define PG_OLDQUERY_CANCEL 2
#define PG_OLDQUERY_WAIT 4

#include "libpq-fe.h"

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

#include "dbdimp.h"
#include "quote.h"
#include "types.h"

/* defines for Driver.xst to let it know what functions to include */

#define dbd_st_rows dbd_st_rows
#define dbd_discon_all dbd_discon_all
#define dbd_st_fetchrow_hashref valid

/* end of Pg.h */
