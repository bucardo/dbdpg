/*
   $Id$

   Copyright (c) 2000-2006 PostgreSQL Global Development Group
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

#include "libpq-fe.h"

#ifdef NEVER
#include<sys/stat.h>
#include "libpq/libpq-fs.h"
#endif
#ifndef INV_READ
#define INV_READ 0x00040000
#endif
#ifndef INV_WRITE
#define INV_WRITE 0x00020000
#endif

/* For versions of DBI older than 1.41 */
#ifndef DBIcf_Executed
#define DBIcf_Executed 0x080000
#endif

#ifdef BUFSIZ
#undef BUFSIZ
#endif
/* this should improve I/O performance for large objects */
#define BUFSIZ 32768


#define NEED_DBIXS_VERSION 93

#include <DBIXS.h>		/* installed by the DBI module	*/

#include <dbd_xsh.h>		/* installed by the DBI module	*/

#include "dbdimp.h"
#include "quote.h"
#include "types.h"

/* defines for Driver.xst to let it know what functions to include */
#define dbd_st_rows dbd_st_rows
#define dbd_discon_all dbd_discon_all
#define dbd_st_fetchrow_hashref valid

/* end of Pg.h */
