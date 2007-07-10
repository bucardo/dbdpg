/*

  $Id$

  Copyright (c) 2002-2007 PostgreSQL Global Development Group
  Portions Copyright (c) 2002 Jeffrey W. Baker
  Portions Copyright (c) 1997-2000 Edmund Mergl
  Portions Copyright (c) 1994-1997 Tim Bunce
   
  You may distribute under the terms of either the GNU General Public
  License or the Artistic License, as specified in the Perl README file.

*/


#include "Pg.h"
#include <math.h>

/* Force preprocessors to use this variable. Default to something valid yet noticeable */
#ifndef PGLIBVERSION
#define PGLIBVERSION 80009
#endif

#ifdef WIN32
#define snprintf _snprintf
#define strcasecmp(s1,s2) lstrcmpiA((s1), (s2))
#endif

#define sword signed int
#define sb2 signed short
#define ub2 unsigned short

/* Someday, we can abandon pre-7.4 and life will be much easier... */
#if PGLIBVERSION < 70400
#define PG_DIAG_SQLSTATE 'C'
/* Better we do all this in one place here than put more ifdefs inside dbdimp.c */
typedef enum
{
	PQTRANS_IDLE,        /* connection idle */
	PQTRANS_ACTIVE,      /* command in progress */
	PQTRANS_INTRANS,     /* idle, within transaction block */
	PQTRANS_INERROR,     /* idle, within failed transaction */
	PQTRANS_UNKNOWN      /* cannot determine status */
} PGTransactionStatusType;
typedef enum
{
	PQERRORS_TERSE,      /* single-line error messages */
	PQERRORS_DEFAULT,    /* recommended style */
	PQERRORS_VERBOSE     /* all the facts, ma'am */
} PGVerbosity;
/* These are actually used to return default values */
int PQprotocolVersion(const PGconn *a);
int PQprotocolVersion(const PGconn *a) { return a ? 0 : 0; }

Oid PQftable(PGresult *a, int b);
Oid PQftable(PGresult *a, int b) { if (a||b) return InvalidOid; return InvalidOid; }

int PQftablecol(PGresult *a, int b);
int PQftablecol(PGresult *a, int b) { return a||b ? 0 : 0; }

int PQsetErrorVerbosity(PGconn *a, PGVerbosity b);
int PQsetErrorVerbosity(PGconn *a, PGVerbosity b) { return a||b ? 0 : 0; }

PGTransactionStatusType PQtransactionStatus(const PGconn *a);
PGTransactionStatusType PQtransactionStatus(const PGconn *a) { return a ? PQTRANS_UNKNOWN : PQTRANS_UNKNOWN; }

/* These should not be called, and will throw errors if they are */
PGresult *PQexecPrepared(PGconn *a,const char *b,int c,const char *const *d,const int *e,const int *f,int g);
PGresult *PQexecPrepared(PGconn *a,const char *b,int c,const char *const *d,const int *e,const int *f,int g) {
	if (a||b||c||d||e||f||g) g=0;
	croak ("Called wrong PQexecPrepared\n");
}
PGresult *PQexecParams(PGconn *a,const char *b,int c,const Oid *d,const char *const *e,const int *f,const int *g,int h);
PGresult *PQexecParams(PGconn *a,const char *b,int c,const Oid *d,const char *const *e,const int *f,const int *g,int h) {
	if (a||b||c||d||e||f||g||h) h=0;
	croak("Called wrong PQexecParams\n");
}

#endif

#if PGLIBVERSION < 80000

/* Should not be called, throw errors: */
PGresult *PQprepare(PGconn *a, const char *b, const char *c, int d, const Oid *e);
PGresult *PQprepare(PGconn *a, const char *b, const char *c, int d, const Oid *e) {
	if (a||b||c||d||e) d=0;
	croak ("Called wrong PQprepare");
}

int PQserverVersion(const PGconn *a);
int PQserverVersion(const PGconn *a) { if (!a) return 0; croak ("Called wrong PQserverVersion"); }

#endif

#ifndef PGErrorVerbosity
typedef enum
{
	PGERROR_TERSE,				/* single-line error messages */
	PGERROR_DEFAULT,			/* recommended style */
	PGERROR_VERBOSE				/* all the facts, ma'am */
} PGErrorVerbosity;
#endif

#define IS_DBI_HANDLE(h) \
  (SvROK(h) && SvTYPE(SvRV(h)) == SVt_PVHV && \
  SvRMAGICAL(SvRV(h)) && (SvMAGIC(SvRV(h)))->mg_type == 'P')

static ExecStatusType _result(imp_dbh_t *imp_dbh, const char *sql);
static ExecStatusType _sqlstate(imp_dbh_t *imp_dbh, PGresult *result);
static void pg_error(SV *h, ExecStatusType error_num, char *error_msg);
static int dbd_db_rollback_commit (SV *dbh, imp_dbh_t *imp_dbh, char * action);
static void dbd_st_split_statement (imp_sth_t *imp_sth, int version, char *statement);
static int dbd_st_prepare_statement (SV *sth, imp_sth_t *imp_sth);
static int is_high_bit_set(char *val);
static int dbd_st_deallocate_statement(SV *sth, imp_sth_t *imp_sth);
static PGTransactionStatusType dbd_db_txn_status (imp_dbh_t *imp_dbh);
static int pg_db_start_txn (SV *dbh, imp_dbh_t *imp_dbh);

DBISTATE_DECLARE;

/* ================================================================== */
/* Quick command executor used throughout this file */
static ExecStatusType _result(imp_dbh, sql)
		 imp_dbh_t *imp_dbh;
		 const char *sql;
{
	PGresult *result;
	ExecStatusType status;

	if (dbis->debug >= 4) (void)PerlIO_printf(DBILOGFP, "dbdpg: _result (%s)\n", sql);

	result = PQexec(imp_dbh->conn, sql);

	status = _sqlstate(imp_dbh, result);

	if (dbis->debug >= 4) (void)PerlIO_printf(DBILOGFP, "dbdpg: Set status to (%d)\n", status);

	PQclear(result);

	return status;

} /* end of _result */


/* ================================================================== */
/* Set the SQLSTATE based on a result, returns the status */
static ExecStatusType _sqlstate(imp_dbh, result)
		 imp_dbh_t *imp_dbh;
		 PGresult *result;
{
	ExecStatusType status = PGRES_FATAL_ERROR; /* until proven otherwise */
	bool stateset = DBDPG_FALSE;

	if (dbis->debug >= 4) (void)PerlIO_printf(DBILOGFP, "dbdpg: _sqlstate\n");

	if (result)
		status = PQresultStatus(result);

	if (dbis->debug >= 6) (void)PerlIO_printf(DBILOGFP, "dbdpg: Status is (%d)\n", status);

#if PGLIBVERSION >= 70400
	/*
	  Because PQresultErrorField may not work completely when an error occurs, and 
	  we are connecting over TCP/IP, only set it here if non-null, and fall through 
	  to a better default value below.
    */
	if (result && imp_dbh->pg_server_version >= 70400
	  && NULL != PQresultErrorField(result,PG_DIAG_SQLSTATE)) {
		strncpy(imp_dbh->sqlstate, PQresultErrorField(result,PG_DIAG_SQLSTATE), 5);
		imp_dbh->sqlstate[5] = '\0';
		stateset = DBDPG_TRUE;
	}
#endif

	if (!stateset) {
		/* Do our best to map the status result to a sqlstate code */
		switch (status) {
		case PGRES_EMPTY_QUERY:
		case PGRES_COMMAND_OK:
		case PGRES_TUPLES_OK:
		case PGRES_COPY_OUT:
		case PGRES_COPY_IN:
			strncpy(imp_dbh->sqlstate, "00000\0", 6); /* Successful completion */
			break;
		case PGRES_BAD_RESPONSE:
		case PGRES_NONFATAL_ERROR:
			strncpy(imp_dbh->sqlstate, "01000\0", 6); /* Warning */
			break;
		case PGRES_FATAL_ERROR:
		default:
			strncpy(imp_dbh->sqlstate, "S8006\0", 6); /* Connection failure */
			break;
		}
	}

	if (dbis->debug >= 6) (void)PerlIO_printf(DBILOGFP, "dbdpg: Set sqlstate to (%s)\n", imp_dbh->sqlstate);

	return status;

} /* end of _sqlstate */


/* ================================================================== */
/* Turn database notices into perl warnings for proper handling. */
static void pg_warn (arg, message)
		 void * arg;
		 const char *message;
{
	D_imp_dbh( sv_2mortal(newRV((SV*)arg)) );
	
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_warn (%s) DBIc_WARN=%d\n",
												message, DBIc_WARN(imp_dbh) ? 1 : 0);

	if (DBIc_WARN(imp_dbh) && DBIc_is(imp_dbh, DBIcf_PrintWarn))
		warn(message);
}


/* ================================================================== */
/* Database specific error handling. */
static void pg_error (h, error_num, error_msg)
		 SV *h;
		 ExecStatusType error_num;
		 char *error_msg;
{
	D_imp_xxh(h);
	char *err;
	imp_dbh_t	*imp_dbh = (imp_dbh_t *)(DBIc_TYPE(imp_xxh) == DBIt_ST ? DBIc_PARENT_COM(imp_xxh) : imp_xxh);
	
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_error (%s) number=%d\n",
												error_msg, error_num);

	New(0, err, strlen(error_msg)+1, char); /* freed below */
	strcpy(err, error_msg);
	/* Strip final newline so line number appears for warn/die */
	if (err[strlen(err)] == 10)
	  err[strlen(err)] = '\0';

	sv_setiv(DBIc_ERR(imp_xxh), (IV)error_num);		 /* set err early */
	sv_setpv(DBIc_ERRSTR(imp_xxh), (char*)err);
	sv_setpvn(DBIc_STATE(imp_xxh), (char*)imp_dbh->sqlstate, 5);
	if (dbis->debug >= 3) {
		(void)PerlIO_printf
			(DBILOGFP, "dbdpg: %s error %d recorded: %s\n",
			 err, error_num, SvPV_nolen(DBIc_ERRSTR(imp_xxh)));
	}
	Safefree(err);

} /* end of pg_error */


/* ================================================================== */
void dbd_init (dbistate_t *dbistate)
{
	DBIS = dbistate;
}


/* ================================================================== */
int dbd_db_login (SV * dbh, imp_dbh_t * imp_dbh, char * dbname, char * uid, char * pwd)
{

	char *         conn_str;
	char *         dest;
	bool           inquote = DBDPG_FALSE;
	STRLEN         connect_string_size;
	ExecStatusType status;

	if (dbis->debug >= 4)
	(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_login\n");
  
	/* DBD::Pg syntax: 'dbname=dbname;host=host;port=port', 'User', 'Pass' */
	/* libpq syntax: 'dbname=dbname host=host port=port user=uid password=pwd' */

	/* Figure out how large our connection string is going to be */
	connect_string_size = strlen(dbname);
	if (strlen(uid))
		connect_string_size += strlen("user='' ") + 2*strlen(uid);
	if (strlen(pwd))
		connect_string_size += strlen("password='' ") + 2*strlen(pwd);
	Newx(conn_str, connect_string_size+1, char); /* freed below */

	/* Change all semi-colons in dbname to a space, unless single-quoted */
	dest = conn_str;
	while (*dbname != '\0') {
		if (';' == *dbname && !inquote)
			*dest++ = ' ';
		else {
			if ('\'' == *dbname)
				inquote = !inquote;
			*dest++ = *dbname;
		}
		dbname++;
	}
	*dest = '\0';

	/* Add in the user and/or password if they exist, escaping single quotes and backslashes */
	if (strlen(uid)) {
		strcat(conn_str, " user='");
		dest = conn_str;
		while(*dest != '\0')
			dest++;
		while(*uid != '\0') {
			if ('\''==*uid || '\\'==*uid)
				*(dest++)='\\';
			*(dest++)=*(uid++);
		}
		*dest = '\0';
		strcat(conn_str, "'");
	}
	if (strlen(pwd)) {
		strcat(conn_str, " password='");
		dest = conn_str;
		while(*dest != '\0')
			dest++;
		while(*pwd != '\0') {
			if ('\''==*pwd || '\\'==*pwd)
				*(dest++)='\\';
			*(dest++)=*(pwd++);
		}
		*dest = '\0';
		strcat(conn_str, "'");
	}

	/* Close any old connection and free memory, just in case */
	if (imp_dbh->conn)
		PQfinish(imp_dbh->conn);
	
	/* Remove any stored savepoint information */
	if (imp_dbh->savepoints) {
		av_undef(imp_dbh->savepoints);
		sv_free((SV *)imp_dbh->savepoints);
	}
	imp_dbh->savepoints = newAV();

	/* Attempt the connection to the database */
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: login connection string: (%s)\n", conn_str);
	imp_dbh->conn = PQconnectdb(conn_str);
	if (dbis->debug >= 6)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: successful connection\n");
	Safefree(conn_str);
	
	/* Set the initial sqlstate */
	Renew(imp_dbh->sqlstate, 6, char); /* freed in dbd_db_destroy */
	strncpy(imp_dbh->sqlstate, "25P01", 6); /* "NO ACTIVE SQL TRANSACTION" */

	/* Check to see that the backend connection was successfully made */
	status = PQstatus(imp_dbh->conn);
	if (CONNECTION_OK != status) {
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		strncpy(imp_dbh->sqlstate, "08006", 6); /* "CONNECTION FAILURE" */
		PQfinish(imp_dbh->conn);
		return 0;
	}
	
	/* Call the pg_warn function anytime this connection raises a notice */
	(void)PQsetNoticeProcessor(imp_dbh->conn, pg_warn, (void *)SvRV(dbh));
	
	/* Figure out what protocol this server is using (most likely 3) */
	imp_dbh->pg_protocol = PQprotocolVersion(imp_dbh->conn);

	/* Figure out this particular backend's version */
	imp_dbh->pg_server_version = -1;
#if PGLIBVERSION >= 80000
	imp_dbh->pg_server_version = PQserverVersion(imp_dbh->conn);
#endif
	if (imp_dbh->pg_server_version <= 0) {
		PGresult *result;
		int	cnt, vmaj, vmin, vrev;
	
		result = PQexec(imp_dbh->conn, "SELECT version(), 'DBD::Pg'");
		status = _sqlstate(imp_dbh, result);
	
		if (!result || PGRES_TUPLES_OK != status || (0==PQntuples(result))) {
			if (dbis->debug >= 1)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Could not get version from the server, status was %d\n", status);
		}
		else {
			cnt = sscanf(PQgetvalue(result,0,0), "PostgreSQL %d.%d.%d", &vmaj, &vmin, &vrev);
			if (cnt >= 2) {
				if (cnt == 2) /* Account for devel version e.g. 8.3beta1 */
					vrev = 0;
				imp_dbh->pg_server_version = (100 * vmaj + vmin) * 100 + vrev;
			}
			else if (dbis->debug >= 1)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Unable to parse version from \"%s\"\n", PQgetvalue(result,0,0));
		}
		if (result)
			PQclear(result);
	}

	imp_dbh->pg_bool_tf     = DBDPG_FALSE;
	imp_dbh->pg_enable_utf8 = DBDPG_FALSE;
	imp_dbh->prepare_now    = DBDPG_FALSE;
	imp_dbh->done_begin     = DBDPG_FALSE;
	imp_dbh->dollaronly     = DBDPG_FALSE;
	imp_dbh->pid_number     = getpid();
	imp_dbh->prepare_number = 1;
	imp_dbh->copystate      = 0;
	imp_dbh->pg_errorlevel  = 1; /* Default */
	imp_dbh->async_status   = 0;
	imp_dbh->async_sth      = NULL;
  
	/* If the server can handle it, we default to "smart", otherwise "off" */
	imp_dbh->server_prepare = imp_dbh->pg_protocol >= 3 ? 
		/* If using 3.0 protocol but not yet version 8, switch to "smart" */
		PGLIBVERSION >= 80000 ? 1 : 2 : 0;
  
	/* Tell DBI that imp_dbh is all ready to go */
	DBIc_IMPSET_on(imp_dbh);
	DBIc_ACTIVE_on(imp_dbh);
  
	return 1;

} /* end of dbd_db_login */


/* ================================================================== */
int dbd_db_ping (dbh)
		 SV *dbh;
{
	D_imp_dbh(dbh);
	PGTransactionStatusType tstatus;
	int status;

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_ping\n"); }

	if (NULL == imp_dbh->conn) {
		return -1;
	}

#if PGLIBVERSION < 70400
	tstatus = 0;
#else
	tstatus = dbd_db_txn_status(imp_dbh);
#endif

	if (dbis->debug >= 6)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: tstatus: (%d)\n", tstatus);

	if (tstatus >= 4) { /* Unknown, so we err on the side of "bad" */
		return -2;
	}

	if (tstatus != 0) { /* 2=active, 3=intrans, 4=inerror */
		return 1+tstatus ;
	}

	/* Even though it may be reported as normal, we have to make sure by issuing a command */

	status = _result(imp_dbh, "SELECT 'DBD::Pg ping test'");

	if (PGRES_TUPLES_OK == status) {
		return 1;
	}

	return -3;

} /* end of dbd_db_ping */


/* ================================================================== */
static PGTransactionStatusType dbd_db_txn_status (imp_dbh)
		 imp_dbh_t *imp_dbh;
{

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_txn_status%s\n",
												PGLIBVERSION < 70400 ? " (ALWAYS 4)" : "");

	/* Pre 7.3 *compiled* servers (our PG library) always return unknown */

	/* Since 7.3 has a possible autocommit issue, we simply have it return unknown as well */
#if PGLIBVERSION < 70400
	return 4;
#else
	return PQtransactionStatus(imp_dbh->conn);
#endif

} /* end of dbd_db_txn_status */


/* rollback and commit share so much code they get one function: */

/* ================================================================== */
static int dbd_db_rollback_commit (dbh, imp_dbh, action)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char * action;
{

	PGTransactionStatusType tstatus;
	int status;

	if (dbis->debug >= 4) {
		(void)PerlIO_printf
			(DBILOGFP, "dbdpg: dbd_db_%s (AutoCommit is %d) (BegunWork is %d)\n", action,
			 DBIc_is(imp_dbh, DBIcf_AutoCommit) ? 1 : 0,
			 DBIc_is(imp_dbh, DBIcf_BegunWork) ? 1 : 0);
	}
	
	/* no action if AutoCommit = on or the connection is invalid */
	if ((NULL == imp_dbh->conn) || (DBIc_has(imp_dbh, DBIcf_AutoCommit)))
		return 0;

	/* We only perform these actions if we need to. For newer servers, we 
		 ask it for the status directly and double-check things */

#if PGLIBVERSION < 70400
	tstatus = 0;
#else
	tstatus = dbd_db_txn_status(imp_dbh);
	if (PQTRANS_IDLE == tstatus) { /* Not in a transaction */
		if (imp_dbh->done_begin) {
			/* We think we ARE in a transaction but we really are not */
			if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: Warning: invalid done_begin turned off\n"); }
			imp_dbh->done_begin = DBDPG_FALSE;
		}
	}
	else if (PQTRANS_ACTIVE == tstatus) { /* Still active - probably in a COPY */
		if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: Command in progress, so no done_begin checking!\n"); }
	}
	else if (PQTRANS_INTRANS == tstatus || PQTRANS_INERROR == tstatus) { /* In a (possibly failed) transaction */
		if (!imp_dbh->done_begin) {
			/* We think we are NOT in a transaction but we really are */
			if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: Warning: invalid done_begin turned on\n"); }
			imp_dbh->done_begin = DBDPG_TRUE;
		}
	}
	else { /* Something is wrong: transaction status unknown */
		if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: Warning: cannot determine transaction status\n"); }
	}
#endif

	/* If begin_work has been called, turn AutoCommit back on and BegunWork off */
	if (DBIc_has(imp_dbh, DBIcf_BegunWork)!=0) {
		DBIc_set(imp_dbh, DBIcf_AutoCommit, 1);
		DBIc_set(imp_dbh, DBIcf_BegunWork, 0);
	}

	if (!imp_dbh->done_begin)
		return 1;

	status = _result(imp_dbh, action);
		
	/* Set this early, for scripts that continue despite the error below */
	imp_dbh->done_begin = DBDPG_FALSE;

	if (PGRES_COMMAND_OK != status) {
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		return 0;
	}

	av_undef(imp_dbh->savepoints);

	/* If we just did a rollback or a commit, we can no longer be in a PGRES_COPY state */
	imp_dbh->copystate=0;

	return 1;

} /* end of dbd_db_rollback_commit */

/* ================================================================== */
int dbd_db_commit (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	return dbd_db_rollback_commit(dbh, imp_dbh, "commit");
}

/* ================================================================== */
int dbd_db_rollback (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	return dbd_db_rollback_commit(dbh, imp_dbh, "rollback");
}


/* ================================================================== */
int dbd_db_disconnect (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_disconnect\n"); }

	/* We assume that disconnect will always work	
		 since most errors imply already disconnected. */

	DBIc_ACTIVE_off(imp_dbh);
	
	if (NULL != imp_dbh->conn) {
		/* Rollback if needed */
		if (0!=dbd_db_rollback(dbh, imp_dbh) && dbis->debug >= 4)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_disconnect: AutoCommit=off -> rollback\n");
		
		PQfinish(imp_dbh->conn);
		
		imp_dbh->conn = NULL;
	}

	/* We don't free imp_dbh since a reference still exists	*/
	/* The DESTROY method is the only one to 'free' memory.	*/
	/* Note that statement objects may still exists for this dbh!	*/

	return 1;

} /* end of dbd_db_disconnect */


/* ================================================================== */
void dbd_db_destroy (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_destroy\n"); }

	av_undef(imp_dbh->savepoints);
	sv_free((SV *)imp_dbh->savepoints);
	Safefree(imp_dbh->sqlstate);

	if (DBIc_ACTIVE(imp_dbh)!=0)
		(void)dbd_db_disconnect(dbh, imp_dbh);

	DBIc_IMPSET_off(imp_dbh);

} /* end of dbd_db_destroy */


/* ================================================================== */
int dbd_db_STORE_attrib (dbh, imp_dbh, keysv, valuesv)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 SV *keysv;
		 SV *valuesv;
{
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	int oldval;
	int newval = SvTRUE(valuesv);

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_STORE (%s) (%d)\n", key, newval); }
	
	if (10==kl && strEQ(key, "AutoCommit")) {
		oldval = DBIc_has(imp_dbh, DBIcf_AutoCommit);
		if (oldval == newval)
			return 1;
		if (newval!=0) { /* It was off but is now on, so do a final commit */
			if (0!=dbd_db_commit(dbh, imp_dbh) && dbis->debug >= 5)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Setting AutoCommit on forced a commit\n");
		}
		DBIc_set(imp_dbh, DBIcf_AutoCommit, newval);
	}
	else if (10==kl && strEQ(key, "pg_bool_tf")) {
		imp_dbh->pg_bool_tf = newval!=0 ? DBDPG_TRUE : DBDPG_FALSE;
	}
#ifdef is_utf8_string
	else if (14==kl && strEQ(key, "pg_enable_utf8")) {
		imp_dbh->pg_enable_utf8 = newval!=0 ? DBDPG_TRUE : DBDPG_FALSE;
	}
#endif
	else if (13==kl && strEQ(key, "pg_errorlevel")) {
		/* Introduced in 7.4 servers */
		if (imp_dbh->pg_protocol >= 3) {
			newval = SvIV(valuesv);
			/* Default to "1" if an invalid value is passed in */
			imp_dbh->pg_errorlevel = 0==newval ? 0 : 2==newval ? 2 : 1;
			(void)PQsetErrorVerbosity(imp_dbh->conn, imp_dbh->pg_errorlevel); /* pre-7.4 does nothing */
			if (dbis->debug >= 5)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Reset error verbosity to %d\n", imp_dbh->pg_errorlevel);
		}
	}
	else if (17==kl && strEQ(key, "pg_server_prepare")) {
		/* No point changing this if the server does not support it */
		if (imp_dbh->pg_protocol >= 3) {
			newval = SvIV(valuesv);
			/* Default to "2" if an invalid value is passed in */
			imp_dbh->server_prepare = 0==newval ? 0 : 1==newval ? 1 : 2;
		}
	}
	else if (14==kl && strEQ(key, "pg_prepare_now")) {
		if (imp_dbh->pg_protocol >= 3) {
			imp_dbh->prepare_now = newval ? DBDPG_TRUE : DBDPG_FALSE;
		}
	}
	else if (25==kl && strEQ(key, "pg_placeholder_dollaronly")) {
	  imp_dbh->dollaronly = newval ? DBDPG_TRUE : DBDPG_FALSE;
	}
	else {
		return 0;
	}
	return 1;

} /* end of dbd_db_STORE_attrib */


/* ================================================================== */
SV * dbd_db_FETCH_attrib (dbh, imp_dbh, keysv)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 SV *keysv;
{
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	SV *retsv = Nullsv;
	char *host = NULL;
	
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_FETCH (%s) dbh=%d\n", key, dbh); }
	
	if (10==kl && strEQ(key, "AutoCommit")) {
		retsv = boolSV(DBIc_has(imp_dbh, DBIcf_AutoCommit));
	} else if (10==kl && strEQ(key, "pg_bool_tf")) {
		retsv = newSViv((IV)imp_dbh->pg_bool_tf);
	} else if (10==kl && strEQ(key, "pg_pid_number")) {
		retsv = newSViv((IV)imp_dbh->pid_number);
	} else if (13==kl && strEQ(key, "pg_errorlevel")) {
		retsv = newSViv((IV)imp_dbh->pg_errorlevel);
#ifdef is_utf8_string
	} else if (14==kl && strEQ(key, "pg_enable_utf8")) {
		retsv = newSViv((IV)imp_dbh->pg_enable_utf8);
#endif
	} else if (11==kl && strEQ(key, "pg_INV_READ")) {
		retsv = newSViv((IV)INV_READ);
	} else if (12==kl && strEQ(key, "pg_INV_WRITE")) {
		retsv = newSViv((IV)INV_WRITE);
	} else if (11==kl && strEQ(key, "pg_protocol")) {
		retsv = newSViv((IV)imp_dbh->pg_protocol);
	} else if (17==kl && strEQ(key, "pg_server_prepare")) {
		retsv = newSViv((IV)imp_dbh->server_prepare);
	} else if (14==kl && strEQ(key, "pg_prepare_now")) {
		retsv = newSViv((IV)imp_dbh->prepare_now);
	} else if (25==kl && strEQ(key, "pg_placeholder_dollaronly")) {
		retsv = newSViv((IV)imp_dbh->dollaronly);
	} else if (14==kl && strEQ(key, "pg_lib_version")) {
		retsv = newSViv((IV) PGLIBVERSION );
	} else if (17==kl && strEQ(key, "pg_server_version")) {
		retsv = newSViv((IV)imp_dbh->pg_server_version);
	}
	else if (5==kl && strEQ(key, "pg_db")) {
		retsv = newSVpv(PQdb(imp_dbh->conn),0);
	} else if (7==kl && strEQ(key, "pg_user")) {
		retsv = newSVpv(PQuser(imp_dbh->conn),0);
	} else if (7==kl && strEQ(key, "pg_pass")) {
		retsv = newSVpv(PQpass(imp_dbh->conn),0);
	} else if (7==kl && strEQ(key, "pg_host")) {
		host = PQhost(imp_dbh->conn); /* May return null */
		if (NULL==host)
			return Nullsv;
		retsv = newSVpv(host,0);
	} else if (7==kl && strEQ(key, "pg_port")) {
		retsv = newSVpv(PQport(imp_dbh->conn),0);
	} else if (15==kl && strEQ(key, "pg_default_port")) {
		retsv = newSViv((IV) PGDEFPORT );
	} else if (10==kl && strEQ(key, "pg_options")) {
		retsv = newSVpv(PQoptions(imp_dbh->conn),0);
	} else if (9==kl && strEQ(key, "pg_socket")) {
		retsv = newSViv((IV)PQsocket(imp_dbh->conn));
	} else if (6==kl && strEQ(key, "pg_pid")) {
		retsv = newSViv((IV)PQbackendPID(imp_dbh->conn));
	}
	
	if (!retsv)
		return Nullsv;
	
	if (retsv == &sv_yes || retsv == &sv_no) {
		return retsv; /* no need to mortalize yes or no */
	}
	return sv_2mortal(retsv);

} /* end of dbd_db_FETCH_attrib */


/* ================================================================== */
int dbd_discon_all (drh, imp_drh)
		 SV *drh;
		 imp_drh_t *imp_drh;
{
	
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_discon_all drh=%d\n", drh); }
	
	/* The disconnect_all concept is flawed and needs more work */
	if (!PL_dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
		sv_setiv(DBIc_ERR(imp_drh), (IV)1);
		sv_setpv(DBIc_ERRSTR(imp_drh), "disconnect_all not implemented");
	}
	return DBDPG_FALSE;

} /* end of dbd_discon_all */


/* ================================================================== */
int dbd_db_getfd (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_getfd dbh=%d\n", dbh); }
	
	return PQsocket(imp_dbh->conn);

} /* end of dbd_db_getfd */


/* ================================================================== */
SV * dbd_db_pg_notifies (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	PGnotify *notify;
	AV *ret;
	SV *retsv;
	int status;
	
	if (dbis->debug >= 3) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_pg_notifies\n"); }
	
	status = PQconsumeInput(imp_dbh->conn);
	if (0 == status) { 
		status = PQstatus(imp_dbh->conn);
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		return &sv_undef;
	}
	
	notify = PQnotifies(imp_dbh->conn);
	
	if (!notify)
		return &sv_undef; 
	
	ret=newAV();
	
	av_push(ret, newSVpv(notify->relname,0) );
	av_push(ret, newSViv(notify->be_pid) );
	
#if PGLIBVERSION >= 70400
 	PQfreemem(notify);
#else
	Safefree(notify);
#endif

	retsv = newRV(sv_2mortal((SV*)ret));
	
	return sv_2mortal(retsv);

} /* end of dbd_db_pg_notifies */


/* ================================================================== */
int dbd_st_prepare (sth, imp_sth, statement, attribs)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 char *statement;
		 SV *attribs; /* hashref of arguments passed to prepare */
{

	D_imp_dbh_from_sth;
	STRLEN mypos=0, wordstart, newsize; /* Used to find and set firstword */
	SV **svp; /* To help parse the arguments */

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_prepare (%s)\n", statement); }

	/* Set default values for this statement handle */
	imp_sth->is_dml = DBDPG_FALSE; /* Not preparable DML until proved otherwise */
	imp_sth->prepared_by_us = DBDPG_FALSE; /* Set to 1 when actually done preparing */
	imp_sth->has_binary = DBDPG_FALSE; /* Are any of the params binary? */
	imp_sth->has_default = DBDPG_FALSE; /* Are any of the params DEFAULT? */
	imp_sth->has_current = DBDPG_FALSE; /* Are any of the params DEFAULT? */
	imp_sth->onetime = DBDPG_FALSE; /* Allow internal shortcut */
	imp_sth->result	= NULL;
	imp_sth->cur_tuple = 0;
	imp_sth->placeholder_type = 0;
	imp_sth->rows = -1;
	imp_sth->totalsize = 0;
	imp_sth->numsegs = imp_sth->numphs = imp_sth->numbound = 0;
	imp_sth->direct = DBDPG_FALSE;
	imp_sth->prepare_name = NULL;
	imp_sth->seg = NULL;
	imp_sth->ph = NULL;
	imp_sth->type_info = NULL;

	/* We inherit some preferences from the database handle */
	imp_sth->server_prepare = imp_dbh->server_prepare;
	imp_sth->prepare_now = imp_dbh->prepare_now;
	imp_sth->dollaronly = imp_dbh->dollaronly;

	/* Parse and set any attributes passed in */
	if (attribs) {
		if ((svp = hv_fetch((HV*)SvRV(attribs),"pg_server_prepare", 17, 0)) != NULL) {
			if (imp_dbh->pg_protocol >= 3) {
				int newval = SvIV(*svp);
				/* Default to "2" if an invalid value is passed in */
				imp_sth->server_prepare = 0==newval ? 0 : 1==newval ? 1 : 2;
			}
		}
		if ((svp = hv_fetch((HV*)SvRV(attribs),"pg_direct", 9, 0)) != NULL)
			imp_sth->direct = 0==SvIV(*svp) ? DBDPG_FALSE : DBDPG_TRUE;
		else if ((svp = hv_fetch((HV*)SvRV(attribs),"pg_prepare_now", 14, 0)) != NULL) {
			if (imp_dbh->pg_protocol >= 3) {
				imp_sth->prepare_now = 0==SvIV(*svp) ? DBDPG_FALSE : DBDPG_TRUE;
			}
		}
		if ((svp = hv_fetch((HV*)SvRV(attribs),"pg_placeholder_dollaronly", 25, 0)) != NULL) {
			imp_sth->dollaronly = SvTRUE(*svp) ? DBDPG_TRUE : DBDPG_FALSE;
		}
	}

	/* Figure out the first word in the statement */
	while (*statement && isSPACE(*statement)) {
		mypos++;
		statement++;
	}
	if ((*statement=='\0') || !isALPHA(*statement)) {
		imp_sth->firstword = NULL;
	}
	else {
		wordstart = mypos;
		while((*statement!='\0') && isALPHA(*statement)) {
			mypos++;
			statement++;
		}
		newsize = mypos-wordstart;
		New(0, imp_sth->firstword, newsize+1, char); /* freed in dbd_st_destroy, and above */
		Copy(statement-newsize,imp_sth->firstword,newsize,char);
		imp_sth->firstword[newsize] = '\0';
		/* Try to prevent transaction commands unless "pg_direct" is set */
		if (0==strcasecmp(imp_sth->firstword, "END") ||
				0==strcasecmp(imp_sth->firstword, "BEGIN") ||
				0==strcasecmp(imp_sth->firstword, "ABORT") ||
				0==strcasecmp(imp_sth->firstword, "COMMIT") ||
				0==strcasecmp(imp_sth->firstword, "ROLLBACK") ||
				0==strcasecmp(imp_sth->firstword, "RELEASE") ||
				0==strcasecmp(imp_sth->firstword, "SAVEPOINT")
				) {
			if (!imp_sth->direct)
				croak ("Please use DBI functions for transaction handling");
			imp_sth->is_dml = DBDPG_TRUE; /* Close enough for our purposes */
		}
		/* Note whether this is preparable DML */
		if (0==strcasecmp(imp_sth->firstword, "SELECT") ||
				0==strcasecmp(imp_sth->firstword, "INSERT") ||
				0==strcasecmp(imp_sth->firstword, "UPDATE") ||
				0==strcasecmp(imp_sth->firstword, "DELETE")
				) {
			imp_sth->is_dml = DBDPG_TRUE;
		}
	}
	statement -= mypos; /* Rewind statement */

	/* Break the statement into segments by placeholder */
	dbd_st_split_statement(imp_sth, imp_dbh->pg_server_version, statement);

	/*
		We prepare it right away if:
		1. The statement is DML
		2. The attribute "direct" is false
		3. The backend can handle server-side prepares
		4. The attribute "pg_server_prepare" is not 0
		5. The attribute "pg_prepare_now" is true
		6. We are compiled on a 8 or greater server
	*/
	if (dbis->debug >= 6)
		(void)PerlIO_printf
			(DBILOGFP,
			 "dbdpg: Immediate prepare decision: dml=%d direct=%d protocol=%d server_prepare=%d prepare_now=%d PGLIBVERSION=%d\n",
			 imp_sth->is_dml, imp_sth->direct, imp_dbh->pg_protocol, imp_sth->server_prepare, imp_sth->prepare_now, PGLIBVERSION
			 );

	if (imp_sth->is_dml
		&& !imp_sth->direct
		&& imp_dbh->pg_protocol >= 3
		&& 0 != imp_sth->server_prepare
		&& imp_sth->prepare_now
		&& PGLIBVERSION >= 80000
		) {
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Running an immediate prepare\n");

		if (dbd_st_prepare_statement(sth, imp_sth)!=0) {
			croak (PQerrorMessage(imp_dbh->conn));
		}
	}

	DBIc_IMPSET_on(imp_sth);

	return imp_sth->numphs;

} /* end of dbd_st_prepare */


/* ================================================================== */
static void dbd_st_split_statement (imp_sth, version, statement)
		 imp_sth_t *imp_sth;
		 int version;
		 char *statement;
{

	/* Builds the "segment" and "placeholder" structures for a statement handle */

	STRLEN currpos; /* Where we currently are in the statement string */

	STRLEN sectionstart, sectionstop; /* Borders of current section */

	STRLEN sectionsize; /* Size of an allocated segment */

	STRLEN backslashes; /* Counts backslashes, only used in quote section */

	STRLEN dollarsize; /* Size of dollarstring */

	int topdollar; /* Used to enforce sequential $1 arguments */

	int placeholder_type; /* Which type we are in: one of 0,1,2,3 (none,?,$,:) */

 	char ch; /* The current character being checked */

	char quote; /* Current quote or comment character: used only in those two blocks */

	bool found; /* Simple boolean */

	bool inside_dollar; /* Inside a dollar quoted value */

	char * dollarstring = NULL; /* Dynamic string between $$ in dollar quoting */

	STRLEN xlen; /* Because "x" is too hard to search for */

	int xint;

	seg_t *newseg, *currseg = NULL; /* Segment structures to help build linked lists */

	ph_t *newph, *thisph, *currph = NULL; /* Placeholder structures to help build ll */

	if (dbis->debug >= 4) {
		if (dbis->debug >= 10) {
			(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_split_statement (%s)\n", statement);
		}
		else {
			(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_split_statement\n");
		}
	}

	/*
		If the pg_direct flag is set (or the string has no length), we do not split at all,
		but simply put everything verbatim into a single segment and return.
	*/
	if (imp_sth->direct || '\0' == *statement) {
		if (dbis->debug >= 4) {
			(void)PerlIO_printf(DBILOGFP, "dbdpg: not splitting due to %s\n",
													imp_sth->direct ? "pg_direct" : "empty string");
		}
		imp_sth->numsegs = 1;
		imp_sth->numphs = 0;
		New(0, imp_sth->seg, 1, seg_t); /* freed in dbd_st_destroy */
		imp_sth->seg->nextseg = NULL;
		imp_sth->seg->placeholder = 0;
		imp_sth->seg->ph = NULL;
		imp_sth->totalsize = strlen(statement);
		if (imp_sth->totalsize > 0) {
			New(0, imp_sth->seg->segment, imp_sth->totalsize+1, char); /* freed in dbd_st_destroy */
			Copy(statement, imp_sth->seg->segment, imp_sth->totalsize+1, char);
		}
		else {
			imp_sth->seg->segment = NULL;
		}
		if (dbis->debug >= 10) {
			(void)PerlIO_printf(DBILOGFP, "dbdpg: direct split = (%s) length=(%d)\n",
													imp_sth->seg->segment, imp_sth->totalsize);
		}
		return;
	}

	/* Start everyone at the start of the string */
	currpos = sectionstart = 0;

	ch = 1;

	while (1) {

		/* Are we done processing this string? */
		if (ch < 1) {
			break;
		}

		/* Put the current letter into ch, and advance statement to the next character */
		ch = *statement++;

		/* Remember: currpos matches *statement, not ch */
		currpos++;

		/* Quick short-circuit for uninteresting characters */
		if (
				(ch < 34 && ch != 0) || (ch > 63 && ch != 91) ||
				 (ch!=34 && ch!=39 &&    /* simple quoting */
					ch!=45 && ch!=47 &&    /* comment */
					ch!=36 &&              /* dollar quoting or placeholder */
					ch!=58 && ch!=63 &&    /* placeholder */
					ch!=91 &&              /* array slice */
					ch!=0                  /* end of the string (create segment) */
					)
				) {
			continue;
		}

		/* 1: A traditionally quoted section */
		if ('\'' == ch || '"' == ch) {
			quote = ch;
			backslashes = 0;
			/* Go until ending quote character (unescaped) or end of string */
			while (quote && ++currpos && (ch = *statement++)) {
				/* 1.1 : single quotes have no meaning in double-quoted sections and vice-versa */
				/* 1.2 : backslashed quotes do not end the section */
				if (ch == quote && (0==(backslashes&1))) {
					quote = 0;
				}
				else if ('\\' == ch) 
					backslashes++;
				else
					backslashes = 0;
			}
			/* 1.3 Quote ended normally, not the end of the string */
			if (ch != 0)
				continue;
			/* 1.4 String ended, but the quote did not */
			if (0 != quote) {
				/* Let the backend handle this */
			}

			/* 1.5: End quote was the last character in the string */
		} /* end quote section */

		/* 2: A comment block: */
		if (('-' == ch && '-' == *statement) ||
				('/' == ch && '/' == *statement) ||
				('/' == ch && '*' == *statement)
				) {
			quote = *statement;
			/* Go until end of comment (may be newline) or end of the string */
			while (quote && ++currpos && (ch = *statement++)) {
				/* 2.1: dashdash and slashslash only terminate at newline */
				if (('-' == quote || '/' == quote) && '\n' == ch) {
					quote=0;
				}
				/* 2.2: slashstar ends with a matching starslash */
				else if ('*' == quote && '*' == ch && '/' == *statement) {
					/* Slurp up the slash */
					ch = *statement++;
					currpos++;
					quote=0;
				}
			}

			/* 2.3 Comment ended normally, not the end of the string */
			if (ch != 0)
				continue;

			/* 2.4 String ended, but the comment did not - do nothing special */
			/* 2.5: End quote was the last character in the string */
		} /* end comment section */

		/* 3: advanced dollar quoting - only if the backend is version 8 or higher */
		if (version >= 80000 && '$' == ch && (*statement == '$' || *statement >= 'A')) {
			/* Unlike PG, we allow a little more latitude in legal characters - anything >= 65 can be used */
			sectionsize = 0; /* How far from the first dollar sign are we? */
		  found = 0; /* Have we found the end of the dollarquote? */

			/* Scan forward until we hit the matching dollarsign */
			while ((ch = *statement++)) {

				sectionsize++;
				/* If we hit an invalid character, bail out */
				if (ch <= 32 || (ch >= '0' && ch <= '9')) {
					break;
				}
				if ('$' == ch) {
					found = DBDPG_TRUE;
					break;
				}
			} /* end first scan */

			/* Not found? Move to the next letter after the dollarsign and move on */
			if (!found) {
				statement -= sectionsize;
				if (!ch) {
					ch = 1; /* So the top loop still works */
					statement--;
				}
				continue;
			}

			/* We only need to create a dollarstring if something was between the two dollar signs */
			if (sectionsize >= 1) {
				New(0, dollarstring, sectionsize, char); /* note: a true array, not a null-terminated string */
				strncpy(dollarstring, statement-sectionsize, sectionsize);
			}

			/* Move on and see if the quote is ever closed */

			inside_dollar=0; /* Are we evaluating the dollar sign for the end? */
			dollarsize = sectionsize;
			xlen=0; /* The current character we are tracing */
			found=0;
			while ((ch = *statement++)) {
				sectionsize++;
				if (inside_dollar) {
					/* Special case of $$ */
					if (dollarsize < 1) {
						found = DBDPG_TRUE;
						break;
					}
					if (ch == dollarstring[xlen++]) {
						/* Got a total match? */
						if (xlen >= dollarsize) {
							found = DBDPG_TRUE;
							statement++;
							sectionsize--;
							break;
						}
					}
					else { /* False dollar string: reset */
						inside_dollar=0;
						xlen=0;
						/* Fall through in case this is a dollar sign */
					}
				}
				if ('$' == ch) {
					inside_dollar = DBDPG_TRUE;
				}
			}

			/* Once here, we are either rewinding, or are done parsing the string */

			/* If end of string, rewind one character */
			if (0==ch) {
				sectionsize--;
			}

			if (dollarstring)
				Safefree(dollarstring);

			/* Advance our cursor to the current position */
			currpos += sectionsize+1;

			statement--; /* Rewind statement by one */

			/* If not found, might be end of string, so set ch */
			if (!found) {
				ch = 1;
			}

			/* Regardless if found or not, we send it back */
			continue;

		} /* end dollar quoting */
		
		/* All we care about at this point is placeholder characters and end of string */
		if ('?' != ch && '$' != ch && ':' != ch && 0!=ch) {
			continue;
		}

		/* We might slurp in a placeholder, so mark the character before the current one */
		/* In other words, inside of "ABC?", set sectionstop to point to "C" */
		sectionstop=currpos-1;

		/* Figure out if we have a placeholder */
		placeholder_type = 0;

		/* Dollar sign placeholder style */
		if ('$' == ch && isDIGIT(*statement)) {
			if ('0' == *statement)
				croak("Invalid placeholder value");
			while(isDIGIT(*statement)) {
				++statement;
				++currpos;
			}
			placeholder_type = 2;
		}
		else if (! imp_sth->dollaronly) {
			/* Question mark style */
			if ('?' == ch) {
				placeholder_type = 1;
			}
			/* Colon style, but skip two colons in a row (e.g. myval::float) */
			else if (':' == ch) {
				if (':' == *statement) {
					/* Might as well skip _all_ consecutive colons */
					while(':' == *statement) {
						++statement;
						++currpos;
					}
					continue;
				}
				if (isALNUM(*statement)) {
					while(isALNUM(*statement)) {
						++statement;
						++currpos;
					}
					placeholder_type = 3;
				}
			}
		}

		/* Check for conflicting placeholder types */
		if (placeholder_type!=0) {
			if (imp_sth->placeholder_type && placeholder_type != imp_sth->placeholder_type)
				croak("Cannot mix placeholder styles \"%s\" and \"%s\"",
							1==imp_sth->placeholder_type ? "?" : 2==imp_sth->placeholder_type ? "$1" : ":foo",
							1==placeholder_type ? "?" : 2==placeholder_type ? "$1" : ":foo");
		}
		
		/* Move on to the next letter unless we found a placeholder, or we are at the end of the string */
		if (0==placeholder_type && ch)
			continue;

		/* If we got here, we have a segment that needs to be saved */
		New(0, newseg, 1, seg_t); /* freed in dbd_st_destroy */
		newseg->nextseg = NULL;
		newseg->placeholder = 0;
		newseg->ph = NULL;

		if (1==placeholder_type) {
			newseg->placeholder = ++imp_sth->numphs;
		}
		else if (2==placeholder_type) {
			newseg->placeholder = atoi(statement-(currpos-sectionstop-1));
		}
		else if (3==placeholder_type) {
			sectionsize = currpos-sectionstop;
			/* Have we seen this placeholder yet? */
			for (xint=1,thisph=imp_sth->ph; NULL != thisph; thisph=thisph->nextph,xint++) {
				if (0==strncmp(thisph->fooname, statement-sectionsize, sectionsize)) {
					newseg->placeholder = xint;
					newseg->ph = thisph;
					break;
				}
			}
			if (0==newseg->placeholder) {
				imp_sth->numphs++;
				newseg->placeholder = imp_sth->numphs;
				New(0, newph, 1, ph_t); /* freed in dbd_st_destroy */
				newseg->ph = newph;
				newph->nextph = NULL;
				newph->bind_type = NULL;
				newph->value = NULL;
				newph->quoted = NULL;
				newph->referenced = DBDPG_FALSE;
				newph->defaultval = DBDPG_TRUE;
				newph->isdefault = DBDPG_FALSE;
				newph->iscurrent = DBDPG_FALSE;
				New(0, newph->fooname, sectionsize+1, char); /* freed in dbd_st_destroy */
				Copy(statement-sectionsize, newph->fooname, sectionsize, char);
				newph->fooname[sectionsize] = '\0';
				if (NULL==currph) {
					imp_sth->ph = newph;
				}
				else {
					currph->nextph = newph;
				}
				currph = newph;
			}
		} /* end if placeholder_type */

		sectionsize = sectionstop-sectionstart; /* 4-0 for "ABCD" */
		if (sectionsize>0) {
			New(0, newseg->segment, sectionsize+1, char); /* freed in dbd_st_destroy */
			Copy(statement-(currpos-sectionstart), newseg->segment, sectionsize, char);
			newseg->segment[sectionsize] = '\0';
			imp_sth->totalsize += sectionsize;
		}
		else {
			newseg->segment = NULL;
		}
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Created segment (%s)\n", newseg->segment);
		
		/* Tie it in to the previous one */
		if (NULL==currseg) {
			imp_sth->seg = newseg;
		}
		else {
			currseg->nextseg = newseg;
		}
		currseg = newseg;
		sectionstart = currpos;
		imp_sth->numsegs++;

		if (placeholder_type > 0)
			imp_sth->placeholder_type = placeholder_type;

		/* If this segment also, ended the string, set ch so we bail out early */
		if ('\0' == *statement)
			break;

	} /* end large while(1) loop: statement parsing */

	/* For dollar sign placeholders, ensure that the rules are followed */
	if (2==imp_sth->placeholder_type) {
		/* 
			 We follow the Pg rules: must start with $1, repeats are allowed, 
			 numbers must be sequential. We change numphs if repeats found
		*/
		topdollar=0;
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (currseg->placeholder > topdollar)
				topdollar = currseg->placeholder;
		}
		/* Make sure every placeholder from 1 to topdollar is used at least once */
		for (xint=1; xint <= topdollar; xint++) {
			for (found=0, currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				if (currseg->placeholder==xint) {
					found = DBDPG_TRUE;
					break;
				}
			}
			if (!found)
				croak("Invalid placeholders: must start at $1 and increment one at a time (expected: $%d)\n", xint);
		}
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Set number of placeholders to %d\n", topdollar);
		imp_sth->numphs = topdollar;
	}

	/* Create sequential placeholders */
	if (3 != imp_sth->placeholder_type) {
		for (xint=1; xint <= imp_sth->numphs; xint++) {
			New(0, newph, 1, ph_t); /* freed in dbd_st_destroy */
			newph->nextph = NULL;
			newph->bind_type = NULL;
			newph->value = NULL;
			newph->quoted = NULL;
			newph->referenced = DBDPG_FALSE;
			newph->defaultval = DBDPG_TRUE;
			newph->isdefault = DBDPG_FALSE;
			newph->iscurrent = DBDPG_FALSE;
			newph->fooname = NULL;
			/* Let the correct segment(s) point to it */
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				if (currseg->placeholder==xint) {
					currseg->ph = newph;
				}
			}
			if (NULL==currph) {
				imp_sth->ph = newph;
			}
			else {
				currph->nextph = newph;
			}
			currph = newph;
		}
	}

	if (dbis->debug >= 10) {
		(void)PerlIO_printf
			(DBILOGFP, "dbdpg: Placeholder type: %d numsegs: %d numphs: %d\n",
			 imp_sth->placeholder_type, imp_sth->numsegs, imp_sth->numphs);
		(void)PerlIO_printf(DBILOGFP, "dbdpg: Placeholder numbers, ph id, and segments:\n");
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			(void)PerlIO_printf(DBILOGFP, "dbdpg: PH: (%d) ID: (%d) SEG: (%s)\n", currseg->placeholder, NULL==currseg->ph ? 0 : currseg->ph, currseg->segment);
		}
		(void)PerlIO_printf(DBILOGFP, "dbdpg: Placeholder number, fooname, id:\n");
		for (xlen=1,currph=imp_sth->ph; NULL != currph; currph=currph->nextph,xlen++) {
			(void)PerlIO_printf(DBILOGFP, "dbdpg: #%d FOONAME: (%s) ID: (%d)\n", xlen, currph->fooname, currph);
		}
	}

	DBIc_NUM_PARAMS(imp_sth) = imp_sth->numphs;

} /* end dbd_st_split_statement */



/* ================================================================== */
static int dbd_st_prepare_statement (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{

	D_imp_dbh_from_sth;
	char *statement;
	unsigned int x;
	STRLEN execsize;
	PGresult *result;
	int status = -1;
	seg_t *currseg;
	bool oldprepare = DBDPG_TRUE;
	int params = 0;
	Oid *paramTypes = NULL;
	ph_t *currph;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_prepare_statement\n");

#if PGLIBVERSION >= 80000
	oldprepare = DBDPG_FALSE;
#endif

	Renew(imp_sth->prepare_name, 25, char); /* freed in dbd_st_destroy (and above) */

	/* Name is simply "dbdpg_PID_#" */
	sprintf(imp_sth->prepare_name,"dbdpg_%d_%d", imp_dbh->pid_number, imp_dbh->prepare_number);

	if (dbis->debug >= 5)
		(void)PerlIO_printf
			(DBILOGFP, "dbdpg: New statement name (%s), oldprepare is %d\n",
			 imp_sth->prepare_name, oldprepare);

	/* PQprepare was not added until 8.0 */

	execsize = imp_sth->totalsize;
	if (oldprepare)
	  execsize += strlen("PREPARE  AS ") + strlen(imp_sth->prepare_name); /* Two spaces! */

	if (imp_sth->numphs!=0) {
	  if (oldprepare) {
			execsize += strlen("()");
			execsize += imp_sth->numphs-1; /* for the commas */
		}
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (0==currseg->placeholder)
				continue;
			/* The parameter itself: dollar sign plus digit(s) */
			for (x=1; x<7; x++) {
				if (currseg->placeholder < pow((double)10,(double)x))
					break;
			}
			if (x>=7)
				croak("Too many placeholders!");
			execsize += x+1;
			if (oldprepare) {
				/* The parameter type, only once per number please */
				if (!currseg->ph->referenced)
					execsize += strlen(currseg->ph->bind_type->type_name);
				currseg->ph->referenced = DBDPG_TRUE;
			}
		}
	}

	New(0, statement, execsize+1, char); /* freed below */

	if (oldprepare) {
		sprintf(statement, "PREPARE %s", imp_sth->prepare_name);
		if (imp_sth->numphs!=0) {
			strcat(statement, "(");
			for (x=0, currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				if (currseg->placeholder && currseg->ph->referenced) {
					if (x!=0)
						strcat(statement, ",");
					strcat(statement, currseg->ph->bind_type->type_name);
					x=1;
					currseg->ph->referenced = DBDPG_FALSE;
				}
			}
			strcat(statement, ")");
		}
		strcat(statement, " AS ");
	}
	else {
		statement[0] = '\0';
	}
	/* Construct the statement, with proper placeholders */
	for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
		strcat(statement, currseg->segment);
		if (currseg->placeholder) {
			sprintf(strchr(statement, '\0'), "$%d", currseg->placeholder);
		}
	}

	statement[execsize] = '\0';

	if (dbis->debug >= 6)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: Prepared statement (%s)\n", statement);

	if (oldprepare) {
		status = _result(imp_dbh, statement);
	}
	else {
		if (imp_sth->numbound!=0) {
			params = imp_sth->numphs;
			Newz(0, paramTypes, (unsigned)imp_sth->numphs, Oid);
			for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph) {
				paramTypes[x++] = (currph->defaultval) ? 0 : (Oid)currph->bind_type->type_id;
			}
		}
		result = PQprepare(imp_dbh->conn, imp_sth->prepare_name, statement, params, paramTypes);
		Safefree(paramTypes);
		if (result)
			status = PQresultStatus(result);
		PQclear(result);
		if (dbis->debug >= 6)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Using PQprepare: %s\n", statement);
	}
	Safefree(statement);
	if (PGRES_COMMAND_OK != status) {
		pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
		return -2;
	}

	imp_sth->prepared_by_us = DBDPG_TRUE; /* Done here so deallocate is not called spuriously */
	imp_dbh->prepare_number++; /* We do this at the end so we don't increment if we fail above */

	return 0;
	
} /* end of dbd_st_prepare_statement */



/* ================================================================== */
int dbd_bind_ph (sth, imp_sth, ph_name, newvalue, sql_type, attribs, is_inout, maxlen)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 SV *ph_name;
		 SV *newvalue;
		 IV sql_type;
		 SV *attribs;
		 int is_inout;
		 IV maxlen;
{

	char *name = Nullch;
	STRLEN name_len;
	ph_t *currph = NULL;
	int x, phnum;
	SV **svp;
	bool reprepare = DBDPG_FALSE;
	int pg_type = 0;
	char *value_string = NULL;
	maxlen = 0; /* not used */

	if (dbis->debug >= 4) {
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_bind_ph ph_name: (%s) newvalue: %s(%lu)\n",
												neatsvpv(ph_name,0), neatsvpv(newvalue,0), SvOK(newvalue));
	}

	if (is_inout!=0)
		croak("bind_inout not supported by this driver");

	if (0==imp_sth->numphs) {
		croak("Statement has no placeholders to bind");
	}

	/* Check the placeholder name and transform to a standard form */
	if (SvGMAGICAL(ph_name)) {
		(void)mg_get(ph_name);
	}
	name = SvPV(ph_name, name_len);
	if (3==imp_sth->placeholder_type) {
		if (':' != *name) {
			croak("Placeholders must begin with ':' when using the \":foo\" style");
		}
	}
	else {
		for (x=0; *(name+x); x++) {
			if (!isDIGIT(*(name+x)) && (x!=0 || '$'!=*(name+x))) {
				croak("Placeholder should be in the format \"$1\"\n");
			}
		}
	}

	/* Find the placeholder in question */

	if (3==imp_sth->placeholder_type) {
		for (x=0,currph=imp_sth->ph; NULL != currph; currph = currph->nextph) {
			if (0==strcmp(currph->fooname, name)) {
				x=1;
				break;
			}
		}
		if (0==x)
			croak("Cannot bind unknown placeholder '%s'", name);
	}
	else { /* We have a number */	
		if ('$' == *name)
			*name++;
		phnum = atoi(name);
		if (phnum < 1 || phnum > imp_sth->numphs)
			croak("Cannot bind unknown placeholder %d (%s)", phnum, neatsvpv(ph_name,0));
		for (x=1,currph=imp_sth->ph; NULL != currph; currph = currph->nextph,x++) {
			if (x==phnum)
				break;
		}
	}

	/* Check the value */
	if (SvTYPE(newvalue) > SVt_PVLV) { /* hook for later array logic	*/
		croak("Cannot bind a non-scalar value (%s)", neatsvpv(newvalue,0));
	}
	/* dbi handle allowed for cursor variables */
	if ((SvROK(newvalue) &&!IS_DBI_HANDLE(newvalue) &&!SvAMAGIC(newvalue))) {
		if (strnEQ("DBD::Pg::DefaultValue", neatsvpv(newvalue,0), 21)
				|| strnEQ("DBI::DefaultValue", neatsvpv(newvalue,0), 17)) {
			/* This is a special type */
			Safefree(currph->value);
			currph->value = NULL;
			currph->valuelen = 0;
			currph->isdefault = DBDPG_TRUE;
			imp_sth->has_default = DBDPG_TRUE;
		}
		else if (strnEQ("DBD::Pg::Current", neatsvpv(newvalue,0), 16)) {
			/* This is a special type */
			Safefree(currph->value);
			currph->value = NULL;
			currph->valuelen = 0;
			currph->iscurrent = DBDPG_TRUE;
			imp_sth->has_current = DBDPG_TRUE;
		}
		else {
			croak("Cannot bind a reference (%s) (%s) (%d) type=%d %d %d %d", neatsvpv(newvalue,0), SvAMAGIC(newvalue),
						SvTYPE(SvRV(newvalue)) == SVt_PVAV ? 1 : 0, SvTYPE(newvalue), SVt_PVAV, SVt_PV, 0);
		}
	}
	if (dbis->debug >= 5) {
		(void)PerlIO_printf(DBILOGFP, "dbdpg: Bind (%s) <== (%s) (type=%ld)\n", name, neatsvpv(newvalue,0), (long)sql_type);
		if (attribs) {
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Bind attribs (%s)", neatsvpv(attribs,0));
		}
	}
	
	/* Check for a pg_type argument (sql_type already handled) */
	if (attribs) {
		if((svp = hv_fetch((HV*)SvRV(attribs),"pg_type", 7, 0)) != NULL)
			pg_type = SvIV(*svp);
	}
	
	if (sql_type && pg_type)
		croak ("Cannot specify both sql_type and pg_type");

	if (NULL == currph->bind_type && (sql_type || pg_type))
		imp_sth->numbound++;
	
	if (pg_type) {
		if ((currph->bind_type = pg_type_data(pg_type))) {
			if (!currph->bind_type->bind_ok) { /* Re-evaluate with new prepare */
				croak("Cannot bind %s, sql_type %s not supported by DBD::Pg",
							name, currph->bind_type->type_name);
			}
		}
		else {
			croak("Cannot bind %s unknown pg_type %" IVdf, name, pg_type);
		}
	}
	else if (sql_type) {
		/* always bind as pg_type, because we know we are 
			 inserting into a pg database... It would make no 
			 sense to quote something to sql semantics and break
			 the insert.
		*/
		if (!(currph->bind_type = sql_type_data((int)sql_type))) {
			croak("Cannot bind param %s: unknown sql_type %d", name, sql_type);
		}
		if (!(currph->bind_type = pg_type_data(currph->bind_type->type.pg))) {
			croak("Cannot find a pg_type for %" IVdf, sql_type);
		}
 	}
	else if (NULL == currph->bind_type) { /* "sticky" data type */
		/* This is the default type, but we will honor defaultval if we can */
				currph->bind_type = pg_type_data(UNKNOWNOID);
		if (!currph->bind_type)
			croak("Default type is bad!!!!???");
	}

	if (pg_type || sql_type) {
		currph->defaultval = DBDPG_FALSE;
		/* Possible re-prepare, depending on whether the type name also changes */
		if (imp_sth->prepared_by_us && NULL != imp_sth->prepare_name)
			reprepare = DBDPG_TRUE;
		/* Mark this statement as having binary if the type is bytea */
		if (BYTEAOID==currph->bind_type->type_id)
			imp_sth->has_binary = DBDPG_TRUE;
	}

	if (currph->isdefault || currph->iscurrent)
		return 1;

	/* convert to a string ASAP */
	if (!SvPOK(newvalue) && SvOK(newvalue)) {
		(void)sv_2pv(newvalue, &na);
	}

	/* upgrade to at least string */
	(void)SvUPGRADE(newvalue, SVt_PV);

	if (SvOK(newvalue)) {
		value_string = SvPV(newvalue, currph->valuelen);
		Renew(currph->value, currph->valuelen+1, char); /* freed in dbd_st_destroy (and above) */
		Copy(value_string, currph->value, currph->valuelen, char);
		currph->value[currph->valuelen] = '\0';
	}
	else {
		Safefree(currph->value);
		currph->value = NULL;
		currph->valuelen = 0;
	}

	if (reprepare) {
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Binding has forced a re-prepare\n");
		/* Deallocate sets the prepare_name to NULL */
		if (dbd_st_deallocate_statement(sth, imp_sth)!=0) {
			/* Deallocation failed. Let's mark it and move on */
			Safefree(imp_sth->prepare_name);
			imp_sth->prepare_name = NULL;
			if (dbis->debug >= 4)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Failed to deallocate!\n");
		}
	}

	if (dbis->debug >= 10)
		(void)PerlIO_printf
			(DBILOGFP, "dbdpg: Placeholder (%s) bound as type (%s) (type_id=%d), length %d, value of (%s)\n",
			 name, currph->bind_type->type_name, currph->bind_type->type_id, currph->valuelen,
			 BYTEAOID==currph->bind_type->type_id ? "(binary, not shown)" : value_string);

	return 1;

} /* end of dbd_bind_ph */


/* ================================================================== */
int pg_quickexec (dbh, sql)
		 SV *          dbh;
		 const char *  sql;
{
	D_imp_dbh(dbh);
	PGresult *result;
	ExecStatusType status = PGRES_FATAL_ERROR; /* Assume the worst */
	char *cmdStatus = NULL;
	int rows = 0;

	if (dbis->debug >= 4) (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_quickexec (%s)\n", sql);

	if (NULL == imp_dbh->conn)
		croak("execute on disconnected handle");

	/* Abort if we are in the middle of a copy */
	if (imp_dbh->copystate!=0)
		croak("Must call pg_endcopy before issuing more commands");

	/* If not autocommit, start a new transaction */
	if (!imp_dbh->done_begin && !DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
		status = _result(imp_dbh, "begin");
		if (PGRES_COMMAND_OK != status) {
			pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
			return -2;
		}
		imp_dbh->done_begin = DBDPG_TRUE;
	}

	result = PQexec(imp_dbh->conn, sql);
	status = _sqlstate(imp_dbh, result);

	imp_dbh->copystate = 0; /* Assume not in copy mode until told otherwise */

	switch (status) {
	case PGRES_TUPLES_OK:
		rows = PQntuples(result);
		break;
	case PGRES_COMMAND_OK:
		/* non-select statement */
		cmdStatus = PQcmdStatus(result);
		if ((0==strncmp(cmdStatus, "DELETE", 6)) || (0==strncmp(cmdStatus, "INSERT", 6)) || 
				(0==strncmp(cmdStatus, "UPDATE", 6))) {
			rows = atoi(PQcmdTuples(result));
		}
		break;
	case PGRES_COPY_OUT:
	case PGRES_COPY_IN:
		/* Copy Out/In data transfer in progress */
		imp_dbh->copystate = status;
		rows = -1;
		break;
	case PGRES_EMPTY_QUERY:
	case PGRES_BAD_RESPONSE:
	case PGRES_NONFATAL_ERROR:
		rows = -2;
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		break;
	case PGRES_FATAL_ERROR:
	default:
		rows = -2;
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		break;
	}

	if (result)
		PQclear(result);
	else
		return -2;

	return rows;

} /* end of pg_quickexec */


/* ================================================================== */
int dbd_st_execute (sth, imp_sth) /* <= -2:error, >=0:ok row count, (-1=unknown count) */
		 SV *sth;
		 imp_sth_t *imp_sth;
{

	D_imp_dbh_from_sth;
	ph_t *currph;
	int status = -1;
	STRLEN execsize, x;
	const char **paramValues = NULL;
	int *paramLengths = NULL, *paramFormats = NULL;
	Oid *paramTypes = NULL;
	seg_t *currseg;
	char *statement = NULL, *cmdStatus = NULL;
	int num_fields, ret = -2;
	
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_execute\n"); }
	
	if (NULL == imp_dbh->conn)
		croak("execute on disconnected handle");

	/* Abort if we are in the middle of a copy */
	if (imp_dbh->copystate!=0)
		croak("Must call pg_endcopy before issuing more commands");

	/* Ensure that all the placeholders have been bound */
	if (imp_sth->numphs!=0) {
		for (currph=imp_sth->ph; NULL != currph; currph=currph->nextph) {
			if (NULL == currph->bind_type) {
				pg_error(sth, PGRES_FATAL_ERROR, "execute called with an unbound placeholder");
				return -2;
			}
		}
	}

	/* If not autocommit, start a new transaction */
	if (!imp_dbh->done_begin && !DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
		status = _result(imp_dbh, "begin");
		if (PGRES_COMMAND_OK != status) {
			pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
			return -2;
		}
		imp_dbh->done_begin = DBDPG_TRUE;
	}

	/* clear old result (if any) */
	if (imp_sth->result != NULL)
		PQclear(imp_sth->result);

	/*
		Now, we need to build the statement to send to the backend
		 We are using one of PQexec, PQexecPrepared, or PQexecParams
		 First, we figure out the size of the statement...
	*/

	execsize = imp_sth->totalsize; /* Total of all segments */

	/* If using plain old PQexec, we need to quote each value ourselves */
	if (!imp_sth->is_dml
		|| imp_dbh->pg_protocol < 3
		|| imp_sth->has_default
		|| imp_sth->has_current
		|| (1 != imp_sth->server_prepare
			&& imp_sth->numbound != imp_sth->numphs)
		) {
		for (currph=imp_sth->ph; NULL != currph; currph=currph->nextph) {
			if (currph->isdefault) {
				Renew(currph->quoted, 8, char); /* freed in dbd_st_destroy */
				strncpy(currph->quoted, "DEFAULT\0", 8);
				currph->quotedlen = 7;
			}
			else if (currph->iscurrent) {
				Renew(currph->quoted, 8, char); /* freed in dbd_st_destroy */
				strncpy(currph->quoted, "CURRENT_TIMESTAMP\0", 18);
				currph->quotedlen = 17;
			}
			else if (NULL == currph->value) {
				Renew(currph->quoted, 5, char); /* freed in dbd_st_destroy */
				strncpy(currph->quoted, "NULL\0", 5);
				currph->quotedlen = 4;
			}
			else {
				if (currph->quoted)
					Safefree(currph->quoted);
				currph->quoted = currph->bind_type->quote
					(currph->value, currph->valuelen, &currph->quotedlen); /* freed in dbd_st_destroy */
			}
		}
		/* Set the size of each actual in-place placeholder */
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (currseg->placeholder!=0)
				execsize += currseg->ph->quotedlen;
		}
	}
	else { /* We are using a server that can handle PQexecParams/PQexecPrepared */
		/* Put all values into an array to pass to PQexecPrepared */
		Newz(0, paramValues, (unsigned)imp_sth->numphs, const char *); /* freed below */
		for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph) {
			paramValues[x++] = currph->value;
		}

		/* Binary or regular? */

		if (imp_sth->has_binary) {
			Newz(0, paramLengths, (unsigned)imp_sth->numphs, int); /* freed below */
			Newz(0, paramFormats, (unsigned)imp_sth->numphs, int); /* freed below */
			for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph,x++) {
				if (BYTEAOID==currph->bind_type->type_id) {
					paramLengths[x] = (int)currph->valuelen;
					paramFormats[x] = 1;
				}
				else {
					paramLengths[x] = 0;
					paramFormats[x] = 0;
				}
			}
		}
	}
	
	/* We use the new server_side prepare style if:
		1. The statement is DML
		2. The attribute "pg_direct" is false
		3. We can handle server-side prepares
		4. The attribute "pg_server_prepare" is not 0
		5. There is one or more placeholders
		6. There are no DEFAULT values
		7a. The attribute "pg_server_prepare" is 1
		OR
		7b. All placeholders are bound (and "pg_server_prepare" is 2)
	*/
	if (dbis->debug >= 7) {
		(void)PerlIO_printf
			(DBILOGFP, "dbdpg: PQexec* decision: dml=%d direct=%d protocol=%d server_prepare=%d numbound=%d numphs=%d default=%d\n",
			 imp_sth->is_dml, imp_sth->direct, imp_dbh->pg_protocol, imp_sth->server_prepare, imp_sth->numbound, imp_sth->numphs, imp_sth->has_default);
	}
	if (imp_sth->is_dml
		&& !imp_sth->direct
		&& imp_dbh->pg_protocol >= 3
		&& 0 != imp_sth->server_prepare
		&& !imp_sth->has_default
		&& !imp_sth->has_current
		&& (1 <= imp_sth->numphs && !imp_sth->onetime)
		&& (1 == imp_sth->server_prepare
			|| (imp_sth->numbound == imp_sth->numphs))
		 ){
	
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: PQexecPrepared\n");

		/* Prepare if it has not already been prepared (or it needs repreparing) */
		if (NULL == imp_sth->prepare_name) {
			if (imp_sth->prepared_by_us) {
				if (dbis->debug >= 5)
					(void)PerlIO_printf(DBILOGFP, "dbdpg: Re-preparing statement\n");
			}
			if (dbd_st_prepare_statement(sth, imp_sth)!=0) {
				Safefree(paramValues);
				Safefree(paramLengths);
				Safefree(paramFormats);
				return -2;
			}
		}
		else {
			if (dbis->debug >= 5)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Using previously prepared statement (%s)\n", imp_sth->prepare_name);
		}
		
		if (dbis->debug >= 10) {
			for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph,x++) {
				(void)PerlIO_printf(DBILOGFP, "dbdpg: PQexecPrepared item #%d\n", x);
				(void)PerlIO_printf(DBILOGFP, "dbdpg: -> Value: (%s)\n", paramValues[x]);
				(void)PerlIO_printf(DBILOGFP, "dbdpg: -> Length: (%d)\n", paramLengths ? paramLengths[x] : 0);
				(void)PerlIO_printf(DBILOGFP, "dbdpg: -> Format: (%d)\n", paramFormats ? paramFormats[x] : 0);
			}
		}
		
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Running PQexecPrepared with (%s)\n", imp_sth->prepare_name);
		imp_sth->result = PQexecPrepared
			(imp_dbh->conn, imp_sth->prepare_name, imp_sth->numphs, paramValues, paramLengths, paramFormats, 0);

	} /* end new-style prepare */
	else {
		
		/* prepare via PQexec or PQexecParams */


		/* PQexecParams */

		if (imp_sth->is_dml
			&& imp_dbh->pg_protocol >= 3
			&& imp_sth->numphs
			&& !imp_sth->has_default
			&& !imp_sth->has_current
			&& (1 == imp_sth->server_prepare || imp_sth->numbound == imp_sth->numphs)
			) {
		  
			if (dbis->debug >= 5)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: PQexecParams\n");

			/* Figure out how big the statement plus placeholders will be */
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				if (0==currseg->placeholder)
					continue;
				/* The parameter itself: dollar sign plus digit(s) */
				for (x=1; x<7; x++) {
					if (currseg->placeholder < pow((double)10,(double)x))
						break;
				}
				if (x>=7)
					croak("Too many placeholders!");
				execsize += x+1;
			}

			/* Create the statement */
			New(0, statement, execsize+1, char); /* freed below */
			statement[0] = '\0';
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				strcat(statement, currseg->segment);
				if (currseg->placeholder!=0)
					sprintf(strchr(statement, '\0'), "$%d", currseg->placeholder);
			}
			statement[execsize] = '\0';
			
			/* Populate paramTypes */
			Newz(0, paramTypes, (unsigned)imp_sth->numphs, Oid);
			for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph) {
				paramTypes[x++] = (currph->defaultval) ? 0 : (Oid)currph->bind_type->type_id;
			}
		
			if (dbis->debug >= 10) {
				for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph,x++) {
					(void)PerlIO_printf(DBILOGFP, "dbdpg: PQexecParams item #%d\n", x);
					(void)PerlIO_printf(DBILOGFP, "dbdpg: -> Type: (%d)\n", paramTypes[x]);
					(void)PerlIO_printf(DBILOGFP, "dbdpg: -> Value: (%s)\n", paramValues[x]);
					(void)PerlIO_printf(DBILOGFP, "dbdpg: -> Length: (%d)\n", paramLengths ? paramLengths[x] : 0);
					(void)PerlIO_printf(DBILOGFP, "dbdpg: -> Format: (%d)\n", paramFormats ? paramFormats[x] : 0);
				}
			}

			if (dbis->debug >= 5)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Running PQexecParams with (%s)\n", statement);
			imp_sth->result = PQexecParams
				(imp_dbh->conn, statement, imp_sth->numphs, paramTypes, paramValues, paramLengths, paramFormats, 0);
			Safefree(paramTypes);
		}
		
		/* PQexec */

		else {

			if (dbis->debug >= 5)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: PQexec\n");

			/* Go through and quote each value, then turn into a giant statement */
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				if (currseg->placeholder!=0)
					execsize += currseg->ph->quotedlen;
			}

			New(0, statement, execsize+1, char); /* freed below */
			statement[0] = '\0';
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			  strcat(statement, currseg->segment);
			  if (currseg->placeholder!=0)
				strcat(statement, currseg->ph->quoted);
			}
			statement[execsize] = '\0';

			if (dbis->debug >= 5)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Running PQexec with (%s)\n", statement);
			
			imp_sth->result = PQexec(imp_dbh->conn, statement);

		} /* end PQexec */

		Safefree(statement);

	} /* end non-prepared exec */

	/* Some form of PQexec has been run at this point */

	status = _sqlstate(imp_dbh, imp_sth->result);

	Safefree(paramValues);
	Safefree(paramLengths);
	Safefree(paramFormats);			

	imp_dbh->copystate = 0; /* Assume not in copy mode until told otherwise */
	if (PGRES_TUPLES_OK == status) {
		num_fields = PQnfields(imp_sth->result);
		imp_sth->cur_tuple = 0;
		DBIc_NUM_FIELDS(imp_sth) = num_fields;
		DBIc_ACTIVE_on(imp_sth);
		ret = PQntuples(imp_sth->result);
		if (dbis->debug >= 5)
			(void)PerlIO_printf
				(DBILOGFP, "dbdpg: Status was PGRES_TUPLES_OK, fields=%d, tuples=%d\n",
				 num_fields, ret);
	}
	else if (PGRES_COMMAND_OK == status) {
		/* non-select statement */
		if (imp_sth->result) {
			cmdStatus = PQcmdStatus(imp_sth->result);
		}
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Status was PGRES_COMMAND_OK\n");
		if ((0==strncmp(cmdStatus, "DELETE", 6)) || (0==strncmp(cmdStatus, "INSERT", 6)) || 
				(0==strncmp(cmdStatus, "UPDATE", 6))) {
			ret = atoi(PQcmdTuples(imp_sth->result));
		}
		else {
			/* We assume that no rows are affected for successful commands (e.g. ALTER TABLE) */
			return 0;
		}
	}
	else if (PGRES_COPY_OUT == status || PGRES_COPY_IN == status) {
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Status was PGRES_COPY_%s\n",
													PGRES_COPY_OUT == status ? "OUT" : "IN");
		/* Copy Out/In data transfer in progress */
		imp_dbh->copystate = status;
		return -1;
	}
	else {
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Invalid status returned (%d)\n", status);
		pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
		return -2;
	}
	
	/* store the number of affected rows */
	
	imp_sth->rows = ret;

	return ret;

} /* end of dbd_st_execute */


/* ================================================================== */
static int is_high_bit_set(val)
		 char *val;
{
	while (*val)
		if (*val++ & 0x80) return 1;
	return 0;
}


/* ================================================================== */
AV * dbd_st_fetch (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	sql_type_info_t *type_info;
	int num_fields;
	char *value;
	char *p;
	int i, chopblanks;
	STRLEN value_len = 0;
	STRLEN len;
	AV *av;
	D_imp_dbh_from_sth;
	
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_fetch\n"); }

	/* Check that execute() was executed successfully */
	if ( !DBIc_ACTIVE(imp_sth) ) {
		pg_error(sth, PGRES_NONFATAL_ERROR, "no statement executing\n");	
		return Nullav;
	}
	
	if (imp_sth->cur_tuple == PQntuples(imp_sth->result) ) {
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Fetched the last tuple (%d)\n", imp_sth->cur_tuple);
		imp_sth->cur_tuple = 0;
		DBIc_ACTIVE_off(imp_sth);
		return Nullav; /* we reached the last tuple */
	}
	
	av = DBIS->get_fbav(imp_sth);
	num_fields = AvFILL(av)+1;
	
	chopblanks = DBIc_has(imp_sth, DBIcf_ChopBlanks);

	/* Set up the type_info array if we have not seen it yet */
	if (NULL == imp_sth->type_info) {
		Newz(0, imp_sth->type_info, (unsigned)num_fields, sql_type_info_t*); /* freed in dbd_st_destroy */
		for (i = 0; i < num_fields; ++i) {
			imp_sth->type_info[i] = pg_type_data((int)PQftype(imp_sth->result, i));
		}
	}
	
	for (i = 0; i < num_fields; ++i) {
		SV *sv;

		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Fetching a field\n");

		sv = AvARRAY(av)[i];
		if (PQgetisnull(imp_sth->result, imp_sth->cur_tuple, i)!=0) {
			SvROK(sv) ? (void)sv_unref(sv) : (void)SvOK_off(sv);
		}
		else {
			value = (char*)PQgetvalue(imp_sth->result, imp_sth->cur_tuple, i); 
			type_info = imp_sth->type_info[i];

			if (type_info) {
				type_info->dequote(value, &value_len); /* dequote in place */
				if (BOOLOID == type_info->type_id && imp_dbh->pg_bool_tf)
					*value = ('1' == *value) ? 't' : 'f';
			}
			else
				value_len = strlen(value);
			
			sv_setpvn(sv, value, value_len);
			
			if (type_info && (BPCHAROID == type_info->type_id) && chopblanks)
				{
					p = SvEND(sv);
					len = SvCUR(sv);
					while(len && ' ' == *--p)
						--len;
					if (len != SvCUR(sv)) {
						SvCUR_set(sv, len);
						*SvEND(sv) = '\0';
					}
				}
			
#ifdef is_utf8_string
			if (imp_dbh->pg_enable_utf8 && type_info) {
				SvUTF8_off(sv);
				switch (type_info->type_id) {
					case CHAROID:
					case TEXTOID:
					case BPCHAROID:
					case VARCHAROID:
						if (is_high_bit_set(value) && is_utf8_string((unsigned char*)value, value_len)) {
							SvUTF8_on(sv);
						}
						break;
					default:
						break;
				}
			}
#endif
		}
	}
	
	imp_sth->cur_tuple += 1;
	
	return av;

} /* end of dbd_st_fetch */


/* ================================================================== */
int dbd_st_rows (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_rows sth=%d\n", sth); }
	
	return imp_sth->rows;

} /* end of dbd_st_rows */


/* ================================================================== */
int dbd_st_finish (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_finish sth=%d\n", sth); }
	
	if (DBIc_ACTIVE(imp_sth) && imp_sth->result) {
		PQclear(imp_sth->result);
		imp_sth->result = NULL;
		imp_sth->rows = 0;
	}
	
	DBIc_ACTIVE_off(imp_sth);
	return 1;

} /* end of sbs_st_finish */


/* ================================================================== */
static int dbd_st_deallocate_statement (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	char tempsqlstate[6];
	char *stmt;
	int status;
	PGTransactionStatusType tstatus;
	D_imp_dbh_from_sth;
	
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_deallocate_statement\n"); }

	if (NULL == imp_dbh->conn || NULL == imp_sth->prepare_name)
		return 0;
	
	tempsqlstate[0] = '\0';

	/* What is our status? */
	tstatus = dbd_db_txn_status(imp_dbh);
	if (dbis->debug >= 5)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: Transaction status is %d\n", tstatus);

	/* If we are in a failed transaction, rollback before deallocating */
	if (PQTRANS_INERROR == tstatus) {
		if (dbis->debug >= 4)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Issuing rollback before deallocate\n");
		{
			/* If a savepoint has been set, rollback to the last savepoint instead of the entire transaction */
			I32	alen = av_len(imp_dbh->savepoints);
			if (alen > -1) {
				SV		*sp = Nullsv;
				char	*cmd;
				sp = *av_fetch(imp_dbh->savepoints, alen, 0);
				New(0, cmd, SvLEN(sp) + 13, char); /* Freed below */
				if (dbis->debug >= 4)
					(void)PerlIO_printf(DBILOGFP, "dbdpg: Rolling back to savepoint %s\n", SvPV_nolen(sp));
				sprintf(cmd,"rollback to %s",SvPV_nolen(sp));
				strncpy(tempsqlstate, imp_dbh->sqlstate, strlen(imp_dbh->sqlstate)+1);
				status = _result(imp_dbh, cmd);
				Safefree(cmd);
			}
			else {
				status = _result(imp_dbh, "ROLLBACK");
				imp_dbh->done_begin = DBDPG_FALSE;
			}
		}
		if (PGRES_COMMAND_OK != status) {
			/* This is not fatal, it just means we cannot deallocate */
			if (dbis->debug >= 4)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Rollback failed, so no deallocate\n");
			return 1;
		}
	}

	New(0, stmt, strlen("DEALLOCATE ") + strlen(imp_sth->prepare_name) + 1, char); /* freed below */

	sprintf(stmt, "DEALLOCATE %s", imp_sth->prepare_name);

	if (dbis->debug >= 5)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: Deallocating (%s)\n", imp_sth->prepare_name);

	status = _result(imp_dbh, stmt);
	Safefree(stmt);
	if (PGRES_COMMAND_OK != status) {
		pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
		return 2;
	}

	Safefree(imp_sth->prepare_name);
	imp_sth->prepare_name = NULL;
	if (tempsqlstate[0]) {
		strncpy(imp_dbh->sqlstate, tempsqlstate, strlen(tempsqlstate)+1);
	}

	return 0;

} /* end of dbd_st_deallocate_statement */


/* ================================================================== */
void dbd_st_destroy (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{

	seg_t *currseg, *nextseg;
	ph_t *currph, *nextph;
	D_imp_dbh_from_sth;

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_destroy\n"); }

	if (NULL == imp_sth->seg) /* Already been destroyed! */
		croak("dbd_st_destroy called twice!");

	/* If the InactiveDestroy flag has been set, we go no further */
	if (DBIc_IADESTROY(imp_dbh)) {
		if (dbis->debug >= 4) {
			(void)PerlIO_printf(DBILOGFP, "dbdpg: skipping sth destroy due to InactiveDestroy\n");
		}
		DBIc_IMPSET_off(imp_sth); /* let DBI know we've done it */
		return;
	}

	/* Deallocate only if we named this statement ourselves and we still have a good connection */
	/* On rare occasions, dbd_db_destroy is called first and we can no longer rely on imp_dbh */
	if (imp_sth->prepared_by_us && DBIc_ACTIVE(imp_dbh)) {
		if (dbd_st_deallocate_statement(sth, imp_sth)!=0) {
			if (dbis->debug >= 4)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Could not deallocate\n");
		}
	}

	Safefree(imp_sth->prepare_name);
	Safefree(imp_sth->type_info);
	Safefree(imp_sth->firstword);

	if (NULL != imp_sth->result) {
		PQclear(imp_sth->result);
		imp_sth->result = NULL;
	}

	/* Free all the segments */
	currseg = imp_sth->seg;
	while (NULL != currseg) {
		Safefree(currseg->segment);
		currseg->ph = NULL;
		nextseg = currseg->nextseg;
		Safefree(currseg);
		currseg = nextseg;
	}
	imp_sth->seg = NULL;

	/* Free all the placeholders */
	currph = imp_sth->ph;
	while (NULL != currph) {
		Safefree(currph->fooname);
		Safefree(currph->value);
		Safefree(currph->quoted);
 		currph->bind_type = NULL;
		nextph = currph->nextph;
		Safefree(currph);
		currph = nextph;
	}
	imp_sth->ph = NULL;

	DBIc_IMPSET_off(imp_sth); /* let DBI know we've done it */

} /* end of dbd_st_destroy */


/* ================================================================== */
int dbd_st_STORE_attrib (sth, imp_sth, keysv, valuesv)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 SV *keysv;
		 SV *valuesv;
{
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	STRLEN vl;
	char *value = SvPV(valuesv,vl);

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_STORE (%s) (%s) sth=%d\n", key, value, sth); }
	
	if (17==kl && strEQ(key, "pg_server_prepare")) {
		imp_sth->server_prepare = strEQ(value,"0") ? DBDPG_FALSE : DBDPG_TRUE;
	}
	else if (14==kl && strEQ(key, "pg_prepare_now")) {
		imp_sth->prepare_now = strEQ(value,"0") ? DBDPG_FALSE : DBDPG_TRUE;
	}
	else if (25==kl && strEQ(key, "pg_placeholder_dollaronly")) {
		imp_sth->dollaronly = SvTRUE(valuesv) ? DBDPG_TRUE : DBDPG_FALSE;
	}
	else if (15==kl && strEQ(key, "pg_prepare_name")) {
		Safefree(imp_sth->prepare_name);
		New(0, imp_sth->prepare_name, vl+1, char); /* freed in dbd_st_destroy (and above) */
		Copy(value, imp_sth->prepare_name, vl, char);
		imp_sth->prepare_name[vl] = '\0';
	}
	else {
		return 0;
	}
	return 1;

} /* end of sbs_st_STORE_attrib */


/* ================================================================== */
SV * dbd_st_FETCH_attrib (sth, imp_sth, keysv)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 SV *keysv;
{
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	int i, x, y, sz;
	SV *retsv = Nullsv;
	sql_type_info_t *type_info;

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_FETCH (%s) sth=%d\n", key, sth); }
	
	/* Some can be done before the execute */
	if (15==kl && strEQ(key, "pg_prepare_name")) {
		retsv = newSVpv((char *)imp_sth->prepare_name, 0);
		return retsv;
	}
	else if (17==kl && strEQ(key, "pg_server_prepare")) {
		retsv = newSViv((IV)imp_sth->server_prepare);
		return retsv;
 	}
	else if (14==kl && strEQ(key, "pg_prepare_now")) {
		retsv = newSViv((IV)imp_sth->prepare_now);
		return retsv;
 	}
	else if (11==kl && strEQ(key, "ParamValues")) {
		HV *pvhv = newHV();
		ph_t *currph;
		for (i=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph,i++) {
			if (NULL == currph->value) {
				(void)hv_store_ent 
				  (pvhv,
				   (3==imp_sth->placeholder_type ? newSVpv(currph->fooname,0) : newSViv(i+1)),
				   newSV(0), 0);
			}
			else {
				(void)hv_store_ent
					(pvhv,
					 (3==imp_sth->placeholder_type ? newSVpv(currph->fooname,0) : newSViv(i+1)),
					 newSVpv(currph->value,0),0);
			}
		}
		retsv = newRV_noinc((SV*)pvhv);
		return retsv;
	}
	else if (10==kl && strEQ(key, "ParamTypes")) {
		HV *pvhv = newHV();
		ph_t *currph;
		for (i=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph,i++) {
			if (NULL == currph->bind_type) {
				(void)hv_store_ent
					(pvhv, (3==imp_sth->placeholder_type ? newSVpv(currph->fooname,0) : newSViv(i+1)),
					 newSV(0), 0);
			}
			else {
				(void)hv_store_ent
					(pvhv, (3==imp_sth->placeholder_type ? newSVpv(currph->fooname,0) : newSViv(i+1)),
					 newSVpv(currph->bind_type->type_name,0),0);
			}
		}
		retsv = newRV_noinc((SV*)pvhv);
		return retsv;
	}
	else if (11==kl && strEQ(key, "pg_segments")) {
		AV *arr = newAV();
		seg_t *currseg;
		for (i=0,currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg,i++) {
			av_push(arr, newSVpv(currseg->segment ? currseg->segment : "NULL",0));
		}
		retsv = newRV_noinc((SV*)arr);
		return retsv;
	}

	if (! imp_sth->result) {
		return Nullsv;
	}
	i = DBIc_NUM_FIELDS(imp_sth);
	
	if (4==kl && strEQ(key, "NAME")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			(void)av_store(av, i, newSVpv(PQfname(imp_sth->result, i),0));
		}
	}
	else if (4==kl && strEQ(key, "TYPE")) {
		/* Need to convert the Pg type to ANSI/SQL type. */
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			type_info = pg_type_data((int)PQftype(imp_sth->result, i));
			(void)av_store(av, i, newSViv( type_info ? type_info->type.sql : 0 ) );
		}
	}
	else if (9==kl && strEQ(key, "PRECISION")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			x = PQftype(imp_sth->result, i);
			switch (x) {
			case BPCHAROID:
			case VARCHAROID:
				sz = PQfmod(imp_sth->result, i);
				break;
			case NUMERICOID:
				sz = PQfmod(imp_sth->result, i)-4;
				if (sz > 0)
					sz = sz >> 16;
				break;
			default:
				sz = PQfsize(imp_sth->result, i);
				break;
			}
			(void)av_store(av, i, sz > 0 ? newSViv(sz) : &sv_undef);
		}
	}
	else if (5==kl && strEQ(key, "SCALE")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			x = PQftype(imp_sth->result, i);
			if (NUMERICOID==x) {
				x = PQfmod(imp_sth->result, i)-4;
				(void)av_store(av, i, newSViv(x % (x>>16)));
			}
			else {
				(void)av_store(av, i, &sv_undef);
			}
		}
	}
	else if (8==kl && strEQ(key, "NULLABLE")) {
		AV *av = newAV();
		PGresult *result;
		int status = -1;
		D_imp_dbh_from_sth;
		char *statement;
		int nullable; /* 0 = not nullable, 1 = nullable 2 = unknown */
		retsv = newRV(sv_2mortal((SV*)av));

		New(0, statement, 100, char); /* freed below */
		statement[0] = '\0';
		while(--i >= 0) {
			nullable=2;
			x = PQftable(imp_sth->result, i);
			y = PQftablecol(imp_sth->result, i);
			if (InvalidOid != x && y > 0) { /* We know what table and column this came from */
				sprintf(statement, "SELECT attnotnull FROM pg_catalog.pg_attribute WHERE attrelid=%d AND attnum=%d", x, y);
				statement[strlen(statement)]='\0';
				result = PQexec(imp_dbh->conn, statement);
				status = PQresultStatus(result);
				if (PGRES_TUPLES_OK == status && PQntuples(result)!=0) {
					switch (PQgetvalue(result,0,0)[0]) {
					case 't':
						nullable = 0;
						break;
					case 'f':
					default:
						nullable = 1;
						break;
					}
				}
				PQclear(result);
			}
			(void)av_store(av, i, newSViv(nullable));
		}
		Safefree(statement);
	}
	else if (10==kl && strEQ(key, "CursorName")) {
		retsv = &sv_undef;
	}
	else if (11==kl && strEQ(key, "RowsInCache")) {
		retsv = &sv_undef;
	}
	else if (7==kl && strEQ(key, "pg_size")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			(void)av_store(av, i, newSViv(PQfsize(imp_sth->result, i)));
		}
	}
	else if (7==kl && strEQ(key, "pg_type")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {			
			type_info = pg_type_data((int)PQftype(imp_sth->result,i));
			(void)av_store(av, i, newSVpv(type_info ? type_info->type_name : "unknown", 0));
		}
	}
	else if (13==kl && strEQ(key, "pg_oid_status")) {
	  retsv = newSVuv((unsigned int)PQoidValue(imp_sth->result));
	}
	else if (13==kl && strEQ(key, "pg_cmd_status")) {
		retsv = newSVpv((char *)PQcmdStatus(imp_sth->result), 0);
	}
	else {
		return Nullsv;
	}
	
	return sv_2mortal(retsv);

} /* end of dbd_st_FETCH_attrib */


/* ================================================================== */
int
pg_db_putline (dbh, buffer)
		SV *dbh;
		const char *buffer;
{
		D_imp_dbh(dbh);
		int copystatus;

		if (dbis->debug >= 4)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_putline\n");

		/* We must be in COPY IN state */
		if (PGRES_COPY_IN != imp_dbh->copystate)
			croak("pg_putline can only be called directly after issuing a COPY IN command\n");

#if PGLIBVERSION < 70400
		if (dbis->debug >= 4)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Running PQputline\n");
		copystatus = 0; /* Make compilers happy */
		return PQputline(imp_dbh->conn, buffer);
#else
		if (dbis->debug >= 4)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Running PQputCopyData\n");

		copystatus = PQputCopyData(imp_dbh->conn, buffer, (int)strlen(buffer));
		if (-1 == copystatus) {
			pg_error(dbh, PGRES_FATAL_ERROR, PQerrorMessage(imp_dbh->conn));
			return 0;
		}
		else if (1 != copystatus) {
			croak("PQputCopyData gave a value of %d\n", copystatus);
		}
		return 0;
#endif
}


/* ================================================================== */
int
pg_db_getline (dbh, buffer, length)
		SV * dbh;
		char * buffer;
		int length;
{
		D_imp_dbh(dbh);
		int copystatus;
		char * tempbuf;

		if (dbis->debug >= 4)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_getline\n");

		tempbuf = NULL;

		/* We must be in COPY OUT state */
		if (PGRES_COPY_OUT != imp_dbh->copystate)
			croak("pg_getline can only be called directly after issuing a COPY OUT command\n");

#if PGLIBVERSION < 70400
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Running PQgetline (%d)\n", length);
		copystatus = PQgetline(imp_dbh->conn, buffer, length);
		if (copystatus < 0 || (*buffer == '\\' && *(buffer+1) == '.')) {
			imp_dbh->copystate=0;
			PQendcopy(imp_dbh->conn);
			return -1;
		}
		return copystatus;
#else
		length = 0; /* Make compilers happy */
		if (dbis->debug >= 5)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Running PQgetCopyData\n");
		copystatus = PQgetCopyData(imp_dbh->conn, &tempbuf, 0);

		if (-1 == copystatus) {
			*buffer = '\0';
			imp_dbh->copystate=0;
			PQendcopy(imp_dbh->conn); /* Can't hurt */
			return -1;
		}
		else if (copystatus < 1) {
			pg_error(dbh, PGRES_FATAL_ERROR, PQerrorMessage(imp_dbh->conn));
		}
		else {
			strncpy(buffer, tempbuf, strlen(tempbuf)+1);
			buffer[strlen(tempbuf)] = '\0';
			PQfreemem(tempbuf);
		}
		return 0;
#endif

}


/* ================================================================== */
int
pg_db_endcopy (dbh)
		SV *dbh;
{
		D_imp_dbh(dbh);
		int copystatus;
		PGresult *result;
		ExecStatusType status;

		if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_pg_endcopy\n"); }

		if (0==imp_dbh->copystate)
			croak("pg_endcopy cannot be called until a COPY is issued");

#if PGLIBVERSION < 70400
		if (PGRES_COPY_IN == imp_dbh->copystate) {
			if (dbis->debug >= 5)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Running PQputline with (\\\\.\\n)\n");
			PQputline(imp_dbh->conn, "\\.\n");
		}
		result = 0; /* Make compiler happy */
		copystatus = PQendcopy(imp_dbh->conn);
#else
		if (PGRES_COPY_IN == imp_dbh->copystate) {
			if (dbis->debug >= 5)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Running PQputCopyEnd\n");
			copystatus = PQputCopyEnd(imp_dbh->conn, NULL);
			if (-1 == copystatus) {
				pg_error(dbh, PGRES_FATAL_ERROR, PQerrorMessage(imp_dbh->conn));
				return 1;
			}
			else if (1 != copystatus)
				croak("PQputCopyEnd returned a value of %d\n", copystatus);
			/* Get the final result of the copy */
			result = PQgetResult(imp_dbh->conn);
			status = _sqlstate(imp_dbh, result);
			PQclear(result);
			if (PGRES_COMMAND_OK != status) {
				pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
				return 1;
			}
			copystatus = 0;
		}
		else {
			copystatus = PQendcopy(imp_dbh->conn);
		}
#endif
		imp_dbh->copystate = 0;
		return copystatus;
}


/* ================================================================== */
void
pg_db_pg_server_trace (dbh, fh)
		SV *dbh;
		FILE *fh;
{
		D_imp_dbh(dbh);

		if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_pg_server_trace\n"); }

		PQtrace(imp_dbh->conn, fh);
}


/* ================================================================== */
void
pg_db_pg_server_untrace (dbh)
		 SV *dbh;
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_pg_server_untrace\n"); }

	PQuntrace(imp_dbh->conn);
}


/* ================================================================== */
int
pg_db_savepoint (dbh, imp_dbh, savepoint)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char * savepoint;
{
	int status;
	char *action;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_savepoint (%s)\n", savepoint);

	New(0, action, strlen(savepoint) + 11, char); /* freed below */

	if (imp_dbh->pg_server_version < 80000)
		croak("Savepoints are only supported on server version 8.0 or higher");

	sprintf(action, "savepoint %s", savepoint);

	/* no action if AutoCommit = on or the connection is invalid */
	if ((NULL == imp_dbh->conn) || (DBIc_has(imp_dbh, DBIcf_AutoCommit)))
		return 0;

	/* Start a new transaction if this is the first command */
	if (!imp_dbh->done_begin) {
		status = _result(imp_dbh, "begin");
		if (PGRES_COMMAND_OK != status) {
			pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
			return -2;
		}
		imp_dbh->done_begin = DBDPG_TRUE;
	}

	status = _result(imp_dbh, action);
	Safefree(action);

	if (PGRES_COMMAND_OK != status) {
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		return 0;
	}

	av_push(imp_dbh->savepoints, newSVpv(savepoint,0));
	return 1;
}


/* ================================================================== */
int pg_db_rollback_to (dbh, imp_dbh, savepoint)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char * savepoint;
{
	int status;
	I32 i;
	char *action;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_rollback_to (%s)\n", savepoint);

	New(0, action, strlen(savepoint) + 13, char);

	if (imp_dbh->pg_server_version < 80000)
		croak("Savepoints are only supported on server version 8.0 or higher");

	sprintf(action,"rollback to %s",savepoint);

	/* no action if AutoCommit = on or the connection is invalid */
	if ((NULL == imp_dbh->conn) || (DBIc_has(imp_dbh, DBIcf_AutoCommit)))
		return 0;

	status = _result(imp_dbh, action);
	Safefree(action);

	if (PGRES_COMMAND_OK != status) {
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		return 0;
	}

	for (i = av_len(imp_dbh->savepoints); i >= 0; i--) {
		SV	*elem = *av_fetch(imp_dbh->savepoints, i, 0);
		if (strEQ(SvPV_nolen(elem), savepoint))
			break;
		(void)av_pop(imp_dbh->savepoints);
	}
	return 1;
}


/* ================================================================== */
int pg_db_release (dbh, imp_dbh, savepoint)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char * savepoint;
{
	int status;
	I32 i;
	char *action;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_release (%s)\n", savepoint);

	New(0, action, strlen(savepoint) + 9, char);

	if (imp_dbh->pg_server_version < 80000)
		croak("Savepoints are only supported on server version 8.0 or higher");

	sprintf(action,"release %s",savepoint);

	/* no action if AutoCommit = on or the connection is invalid */
	if ((NULL == imp_dbh->conn) || (DBIc_has(imp_dbh, DBIcf_AutoCommit)))
		return 0;

	status = _result(imp_dbh, action);
	Safefree(action);

	if (PGRES_COMMAND_OK != status) {
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		return 0;
	}

	for (i = av_len(imp_dbh->savepoints); i >= 0; i--) {
		SV	*elem = av_pop(imp_dbh->savepoints);
		if (strEQ(SvPV_nolen(elem), savepoint))
			break;
	}
	return 1;
}

/* Used to ensure we are in a txn, e.g. the lo_ functions below */
static int pg_db_start_txn (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	int status = -1;
	/* If not autocommit, start a new transaction */
	if (!imp_dbh->done_begin && !DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
		status = _result(imp_dbh, "begin");
		if (PGRES_COMMAND_OK != status) {
			pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
			return 0;
		}
		imp_dbh->done_begin = DBDPG_TRUE;
	}
	return 1;
}


/* ================================================================== */
/* Large object functions */


unsigned int pg_db_lo_creat (dbh, mode)
		SV *dbh;
		int mode;
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_creat (%d)\n", mode); }

	if (!pg_db_start_txn(dbh,imp_dbh)) {
	  return 0; /* No other option, because lo_creat returns an Oid */
	}

	return lo_creat(imp_dbh->conn, mode); /* 0 on error */
}

int pg_db_lo_open (dbh, lobjId, mode)
		 SV *dbh;
		 unsigned int lobjId;
		 int mode;
{
	D_imp_dbh(dbh);
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_open (%d) (%d)\n", lobjId, mode); }
	if (!pg_db_start_txn(dbh,imp_dbh)) {
		return -2;
	}
	return lo_open(imp_dbh->conn, lobjId, mode); /* -1 on error */
}

int pg_db_lo_close (dbh, fd)
		 SV *dbh;
		 int fd;
{
	D_imp_dbh(dbh);
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_close (%d)\n", fd); }
	return lo_close(imp_dbh->conn, fd); /* <0 on error, 0 if ok */
}

int pg_db_lo_read (dbh, fd, buf, len)
		 SV *dbh;
		 int fd;
		 char *buf;
		 size_t len;
{
	D_imp_dbh(dbh);
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_read (%d) (%d)\n", fd, len); }
	return lo_read(imp_dbh->conn, fd, buf, len); /* bytes read, <0 on error */
}

int pg_db_lo_write (dbh, fd, buf, len)
		 SV *dbh;
		 int fd;
		 char *buf;
		 size_t len;
{
	D_imp_dbh(dbh);
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_write (%d) (%d)\n", fd, len); }
	return lo_write(imp_dbh->conn, fd, buf, len); /* bytes written, <0 on error */
}

int pg_db_lo_lseek (dbh, fd, offset, whence)
		 SV *dbh;
		 int fd;
		 int offset;
		 int whence;
{
	D_imp_dbh(dbh);
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_lseek (%d) (%d) (%d)\n", fd, offset, whence); }
	return lo_lseek(imp_dbh->conn, fd, offset, whence); /* new position, -1 on error */
}

int pg_db_lo_tell (dbh, fd)
		SV *dbh;
		int fd;
{
	D_imp_dbh(dbh);
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_tell (%d)\n", fd); }
	return lo_tell(imp_dbh->conn, fd); /* current position, <0 on error */
}

int pg_db_lo_unlink (dbh, lobjId)
		 SV *dbh;
		 unsigned int lobjId;
{
	D_imp_dbh(dbh);
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_unlink (%d)\n", lobjId); }
	if (!pg_db_start_txn(dbh,imp_dbh)) {
		return -2;
	}
	return lo_unlink(imp_dbh->conn, lobjId); /* 1 on success, -1 on failure */
}

unsigned int pg_db_lo_import (dbh, filename)
		 SV *dbh;
		 char *filename;
{
	D_imp_dbh(dbh);
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_import (%s)\n", filename); }
	if (!pg_db_start_txn(dbh,imp_dbh)) {
		return 0; /* No other option, because lo_import returns an Oid */
	}
	return lo_import(imp_dbh->conn, filename); /* 0 on error */
}

int pg_db_lo_export (dbh, lobjId, filename)
		 SV *dbh;
		 unsigned int lobjId;
		 char *filename;
{
	D_imp_dbh(dbh);
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_export id:(%d) file:(%s)\n", lobjId, filename); }
	if (!pg_db_start_txn(dbh,imp_dbh)) {
		return -2;
	}
	return lo_export(imp_dbh->conn, lobjId, filename); /* 1 on success, -1 on failure */
}


/* ================================================================== */
int dbd_st_blob_read (sth, imp_sth, lobjId, offset, len, destrv, destoffset)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 int lobjId;
		 long offset;
		 long len;
		 SV *destrv;
		 long destoffset;
{
	D_imp_dbh_from_sth;
	int ret, lobj_fd, nbytes;
	STRLEN nread;
	SV *bufsv;
	char *tmp;
	
	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_blob_read (%d) (%d) (%d)\n", lobjId, offset, len); }

	/* safety checks */
	if (lobjId <= 0) {
		pg_error(sth, PGRES_FATAL_ERROR, "dbd_st_blob_read: lobjId <= 0");
		return 0;
	}
	if (offset < 0) {
		pg_error(sth, PGRES_FATAL_ERROR, "dbd_st_blob_read: offset < 0");
		return 0;
	}
	if (len < 0) {
		pg_error(sth, PGRES_FATAL_ERROR, "dbd_st_blob_read: len < 0");
		return 0;
		}
	if (! SvROK(destrv)) {
		pg_error(sth, PGRES_FATAL_ERROR, "dbd_st_blob_read: destrv not a reference");
		return 0;
	}
	if (destoffset < 0) {
		pg_error(sth, PGRES_FATAL_ERROR, "dbd_st_blob_read: destoffset < 0");
		return 0;
	}
	
	/* dereference destination and ensure it's writable string */
	bufsv = SvRV(destrv);
	if (0==destoffset) {
		sv_setpvn(bufsv, "", 0);
	}
	
	/* open large object */
	lobj_fd = lo_open(imp_dbh->conn, (unsigned)lobjId, INV_READ);
	if (lobj_fd < 0) {
		pg_error(sth, PGRES_FATAL_ERROR, PQerrorMessage(imp_dbh->conn));
		return 0;
	}
	
	/* seek on large object */
	if (offset > 0) {
		ret = lo_lseek(imp_dbh->conn, lobj_fd, (int)offset, SEEK_SET);
		if (ret < 0) {
			pg_error(sth, PGRES_FATAL_ERROR, PQerrorMessage(imp_dbh->conn));
			return 0;
		}
	}
	
	/* read from large object */
	nread = 0;
	SvGROW(bufsv, (STRLEN)(destoffset + nread + BUFSIZ + 1));
	tmp = (SvPVX(bufsv)) + destoffset + nread;
	while ((nbytes = lo_read(imp_dbh->conn, lobj_fd, tmp, BUFSIZ)) > 0) {
		nread += nbytes;
		/* break if user wants only a specified chunk */
		if (len > 0 && nread > (STRLEN)len) {
			nread = (STRLEN)len;
			break;
		}
		SvGROW(bufsv, (STRLEN)(destoffset + nread + BUFSIZ + 1));
		tmp = (SvPVX(bufsv)) + destoffset + nread;
	}
	
	/* terminate string */
	SvCUR_set(bufsv, (STRLEN)(destoffset + nread));
	*SvEND(bufsv) = '\0';
	
	/* close large object */
	ret = lo_close(imp_dbh->conn, lobj_fd);
	if (ret < 0) {
		pg_error(sth, PGRES_FATAL_ERROR, PQerrorMessage(imp_dbh->conn));
		return 0;
	}
	
	return (int)nread;
}

/*
Some information to keep you sane:
typedef enum
{
	PGRES_EMPTY_QUERY = 0,		// empty query string was executed 
1	PGRES_COMMAND_OK,			// a query command that doesn't return
								   anything was executed properly by the
								   backend 
2	PGRES_TUPLES_OK,			// a query command that returns tuples was
								   executed properly by the backend, PGresult
								   contains the result tuples 
3	PGRES_COPY_OUT,				// Copy Out data transfer in progress 
4	PGRES_COPY_IN,				// Copy In data transfer in progress 
5	PGRES_BAD_RESPONSE,			// an unexpected response was recv'd from the
								   backend 
6	PGRES_NONFATAL_ERROR,		// notice or warning message 
7	PGRES_FATAL_ERROR			// query failed 
} ExecStatusType;

*/

/* end of dbdimp.c */

