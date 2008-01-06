/*

  $Id$

  Copyright (c) 2002-2007 Greg Sabino Mullane and others: see the Changes file
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

#define IS_DBI_HANDLE(h)										\
	(SvROK(h) && SvTYPE(SvRV(h)) == SVt_PVHV &&					\
	 SvRMAGICAL(SvRV(h)) && (SvMAGIC(SvRV(h)))->mg_type == 'P')

static void pg_error(SV *h, ExecStatusType error_num, char *error_msg);
static void pg_warn (void * arg, const char * message);
static ExecStatusType _result(imp_dbh_t *imp_dbh, const char *sql);
static ExecStatusType _sqlstate(imp_dbh_t *imp_dbh, PGresult *result);
static int dbd_db_rollback_commit (SV *dbh, imp_dbh_t *imp_dbh, char * action);
static void dbd_st_split_statement (imp_sth_t *imp_sth, int version, char *statement);
static int dbd_st_prepare_statement (SV *sth, imp_sth_t *imp_sth);
static int is_high_bit_set(char *val);
static int dbd_st_deallocate_statement(SV *sth, imp_sth_t *imp_sth);
static PGTransactionStatusType dbd_db_txn_status (imp_dbh_t *imp_dbh);
static int pg_db_start_txn (SV *dbh, imp_dbh_t *imp_dbh);
static int handle_old_async(SV * handle, imp_dbh_t * imp_dbh, int asyncflag);

DBISTATE_DECLARE;

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
	imp_dbh->savepoints = newAV(); /* freed in dbd_db_destroy */

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
	imp_dbh->expand_array   = DBDPG_TRUE;
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
/* Database specific error handling. */
static void pg_error (SV * h, ExecStatusType error_num, char * error_msg)
{
	D_imp_xxh(h);
	char *      err;
	imp_dbh_t * imp_dbh = (imp_dbh_t *)(DBIc_TYPE(imp_xxh) == DBIt_ST ? DBIc_PARENT_COM(imp_xxh) : imp_xxh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_error (%s) number=%d\n",
							error_msg, error_num);

	Newx(err, strlen(error_msg)+1, char); /* freed below */
	strcpy(err, error_msg);

	/* Strip final newline so line number appears for warn/die */
	if (err[strlen(err)] == 10)
		err[strlen(err)] = '\0';

	sv_setiv(DBIc_ERR(imp_xxh), (IV)error_num);
	sv_setpv(DBIc_ERRSTR(imp_xxh), (char*)err);
	sv_setpv(DBIc_STATE(imp_xxh), (char*)imp_dbh->sqlstate);
	if (dbis->debug >= 3) {
		(void)PerlIO_printf
			(DBILOGFP, "dbdpg: sqlstate %s error_num %d error %s\n",
			 imp_dbh->sqlstate, error_num, err);
	}
	Safefree(err);

} /* end of pg_error */


/* ================================================================== */
/* Turn database notices into perl warnings for proper handling. */
static void pg_warn (void * arg, const char * message)
{
	D_imp_dbh( sv_2mortal(newRV((SV*)arg)) );

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_warn (%s) DBIc_WARN=%d PrintWarn=%d\n",
							message, DBIc_WARN(imp_dbh) ? 1 : 0,
							DBIc_is(imp_dbh, DBIcf_PrintWarn) ? 1 : 0);

	if (DBIc_WARN(imp_dbh) && DBIc_is(imp_dbh, DBIcf_PrintWarn))
		warn(message);

} /* end of pg_warn */


/* ================================================================== */
/* Quick command executor used throughout this file */
static ExecStatusType _result(imp_dbh_t * imp_dbh, const char * sql)
{
	PGresult *     result;
	ExecStatusType status;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: _result (%s)\n", sql);

	result = PQexec(imp_dbh->conn, sql);

	status = _sqlstate(imp_dbh, result);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: _result status is %d\n", status);

	PQclear(result);

	return status;

} /* end of _result */


/* ================================================================== */
/* Set the SQLSTATE based on a result, returns the status */
static ExecStatusType _sqlstate(imp_dbh_t * imp_dbh, PGresult * result)
{
	ExecStatusType status   = PGRES_FATAL_ERROR; /* until proven otherwise */
	bool           stateset = DBDPG_FALSE;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: _sqlstate\n");

	if (result)
		status = PQresultStatus(result);

	if (dbis->debug >= 6)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: _sqlstate status is %d\n", status);

	/*
	  Because PQresultErrorField may not work completely when an error occurs, and 
	  we are connecting over TCP/IP, only set it here if non-null, and fall through 
	  to a better default value below.
    */
	if (result && NULL != PQresultErrorField(result,PG_DIAG_SQLSTATE)) {
		strncpy(imp_dbh->sqlstate, PQresultErrorField(result,PG_DIAG_SQLSTATE), 5);
		imp_dbh->sqlstate[5] = '\0';
		stateset = DBDPG_TRUE;
	}

	if (!stateset) {
		/* Do our best to map the status result to a sqlstate code */
		switch (status) {
		case PGRES_EMPTY_QUERY:
		case PGRES_COMMAND_OK:
		case PGRES_TUPLES_OK:
		case PGRES_COPY_OUT:
		case PGRES_COPY_IN:
			strncpy(imp_dbh->sqlstate, "00000", 6); /* SUCCESSFUL COMPLETION */
			break;
		case PGRES_BAD_RESPONSE:
		case PGRES_NONFATAL_ERROR:
			strncpy(imp_dbh->sqlstate, "01000", 6); /* WARNING */
			break;
		case PGRES_FATAL_ERROR:
		default:
			strncpy(imp_dbh->sqlstate, "22000", 6); /* DATA EXCEPTION */
			break;
		}
	}

	if (dbis->debug >= 6)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: _sqlstate set to %s\n", imp_dbh->sqlstate);

	return status;

} /* end of _sqlstate */


/* ================================================================== */
int dbd_db_ping (SV * dbh)
{
	D_imp_dbh(dbh);
	PGTransactionStatusType tstatus;
	ExecStatusType          status;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_ping\n");

	if (NULL == imp_dbh->conn)
		return -1;

	tstatus = dbd_db_txn_status(imp_dbh);

	if (dbis->debug >= 6)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_ping txn_status is %d\n", tstatus);

	if (tstatus >= 4) /* Unknown, so we err on the side of "bad" */
		return -2;

	if (tstatus != 0) /* 2=active, 3=intrans, 4=inerror */
		return 1+tstatus ;

	/* Even though it may be reported as normal, we have to make sure by issuing a command */

	status = _result(imp_dbh, "SELECT 'DBD::Pg ping test'");

	if (PGRES_TUPLES_OK == status)
		return 1;

	return -3;

} /* end of dbd_db_ping */


/* ================================================================== */
static PGTransactionStatusType dbd_db_txn_status (imp_dbh_t * imp_dbh)
{

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_txn_status\n");

	return PQtransactionStatus(imp_dbh->conn);

} /* end of dbd_db_txn_status */


/* rollback and commit share so much code they get one function: */

/* ================================================================== */
static int dbd_db_rollback_commit (SV * dbh, imp_dbh_t * imp_dbh, char * action)
{
	PGTransactionStatusType tstatus;
	ExecStatusType          status;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_%s (AutoCommit is %d) (BegunWork is %d)\n",
							action,
							DBIc_is(imp_dbh, DBIcf_AutoCommit) ? 1 : 0,
							DBIc_is(imp_dbh, DBIcf_BegunWork) ? 1 : 0);
	
	/* No action if AutoCommit = on or the connection is invalid */
	if ((NULL == imp_dbh->conn) || (DBIc_has(imp_dbh, DBIcf_AutoCommit)))
		return 0;

	/* We only perform these actions if we need to. For newer servers, we 
	   ask it for the status directly and double-check things */

	tstatus = dbd_db_txn_status(imp_dbh);
	if (PQTRANS_IDLE == tstatus) { /* Not in a transaction */
		if (imp_dbh->done_begin) {
			/* We think we ARE in a transaction but we really are not */
			if (dbis->debug >= 1)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Warning: invalid done_begin turned off\n");
			imp_dbh->done_begin = DBDPG_FALSE;
		}
	}
	else if (PQTRANS_ACTIVE == tstatus) { /* Still active - probably in a COPY */
		if (dbis->debug >= 1)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Command in progress, so no done_begin checking!\n");
	}
	else if (PQTRANS_INTRANS == tstatus || PQTRANS_INERROR == tstatus) { /* In a (possibly failed) transaction */
		if (!imp_dbh->done_begin) {
			/* We think we are NOT in a transaction but we really are */
			if (dbis->debug >= 1)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Warning: invalid done_begin turned on\n");
			imp_dbh->done_begin = DBDPG_TRUE;
		}
	}
	else { /* Something is wrong: transaction status unknown */
		if (dbis->debug >= 1)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Warning: cannot determine transaction status\n");
	}

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

	/* We just did a rollback or a commit, so savepoints are not relevant, and we cannot be in a PGRES_COPY state */
	av_undef(imp_dbh->savepoints);
	imp_dbh->copystate=0;

	return 1;

} /* end of dbd_db_rollback_commit */

/* ================================================================== */
int dbd_db_commit (SV * dbh, imp_dbh_t * imp_dbh)
{
	return dbd_db_rollback_commit(dbh, imp_dbh, "commit");
}

/* ================================================================== */
int dbd_db_rollback (SV * dbh, imp_dbh_t * imp_dbh)
{
	return dbd_db_rollback_commit(dbh, imp_dbh, "rollback");
}


/* ================================================================== */
int dbd_db_disconnect (SV * dbh, imp_dbh_t * imp_dbh)
{
	
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_disconnect\n");

	/* We assume that disconnect will always work	
	   since most errors imply already disconnected. */

	DBIc_ACTIVE_off(imp_dbh);
	
	if (NULL != imp_dbh->conn) {
		/* Attempt a rollback */
		if (0 != dbd_db_rollback(dbh, imp_dbh) && dbis->debug >= 4)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_disconnect: AutoCommit=off -> rollback\n");
		
		PQfinish(imp_dbh->conn);
		imp_dbh->conn = NULL;
	}

	/* We don't free imp_dbh since a reference still exists	*/
	/* The DESTROY method is the only one to 'free' memory.	*/
	/* Note that statement objects may still exists for this dbh! */

	return 1;

} /* end of dbd_db_disconnect */


/* ================================================================== */
void dbd_db_destroy (SV * dbh, imp_dbh_t * imp_dbh)
{
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_destroy\n");

	if (DBIc_ACTIVE(imp_dbh))
		(void)dbd_db_disconnect(dbh, imp_dbh);

	if (imp_dbh->async_sth) { /* Just in case */
		if (imp_dbh->async_sth->result)
			PQclear(imp_dbh->async_sth->result);
		imp_dbh->async_sth = NULL;
	}

	av_undef(imp_dbh->savepoints);
	sv_free((SV *)imp_dbh->savepoints);
	Safefree(imp_dbh->sqlstate);

	DBIc_IMPSET_off(imp_dbh);

} /* end of dbd_db_destroy */


/* ================================================================== */
SV * dbd_db_FETCH_attrib (SV * dbh, imp_dbh_t * imp_dbh, SV * keysv)
{
	STRLEN kl;
	char * key = SvPV(keysv,kl);
	SV *   retsv = Nullsv;
	
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_FETCH (%s) dbh=%d\n", key, dbh);
	
	switch (kl) {

	case 5: /* pg_db */

		if (strEQ("pg_db", key))
			retsv = newSVpv(PQdb(imp_dbh->conn),0);
		break;

	case 6: /* pg_pid */

		if (strEQ("pg_pid", key))
			retsv = newSViv((IV)PQbackendPID(imp_dbh->conn));
		break;

	case 7: /* pg_user  pg_pass  pg_port  pg_host */

		if (strEQ("pg_user", key))
			retsv = newSVpv(PQuser(imp_dbh->conn),0);
		else if (strEQ("pg_pass", key))
			retsv = newSVpv(PQpass(imp_dbh->conn),0);
		else if (strEQ("pg_port", key))
			retsv = newSVpv(PQport(imp_dbh->conn),0);
		else if (strEQ("pg_host", key)) {
			retsv = PQhost(imp_dbh->conn) ? newSVpv(PQhost(imp_dbh->conn),0) : Nullsv;
		}
		break;

	case 9: /* pg_socket */

		if (strEQ("pg_socket", key))
			retsv = newSViv((IV)PQsocket(imp_dbh->conn));
		break;

	case 10: /* AutoCommit  pg_bool_tf  pg_pid_number  pg_options */

		if (strEQ("AutoCommit", key))
			retsv = boolSV(DBIc_has(imp_dbh, DBIcf_AutoCommit));
		else if (strEQ("pg_bool_tf", key))
			retsv = newSViv((IV)imp_dbh->pg_bool_tf);
		else if (strEQ("pg_pid_number", key))
			retsv = newSViv((IV)imp_dbh->pid_number);
		else if (strEQ("pg_options", key))
			retsv = newSVpv(PQoptions(imp_dbh->conn),0);
		break;

	case 11: /* pg_INV_READ  pg_protocol */

		if (strEQ("pg_INV_READ", key))
			retsv = newSViv((IV)INV_READ);
		else if (strEQ("pg_protocol", key))
			retsv = newSViv((IV)imp_dbh->pg_protocol);
		break;

	case 12: /* pg_INV_WRITE */

		if (strEQ("pg_INV_WRITE", key))
			retsv = newSViv((IV) INV_WRITE );
		break;

	case 13: /* pg_errorlevel */

		if (strEQ("pg_errorlevel", key))
			retsv = newSViv((IV)imp_dbh->pg_errorlevel);
		break;

	case 14: /* pg_lib_version  pg_prepare_now  pg_enable_utf8 */

		if (strEQ("pg_lib_version", key))
			retsv = newSViv((IV) PGLIBVERSION );
		else if (strEQ("pg_prepare_now", key))
			retsv = newSViv((IV)imp_dbh->prepare_now);
#ifdef is_utf8_string
		else if (strEQ("pg_enable_utf8", key))
			retsv = newSViv((IV)imp_dbh->pg_enable_utf8);
#endif
		break;

	case 15: /* pg_default_port pg_async_status pg_expand_array */

		if (strEQ("pg_default_port", key))
			retsv = newSViv((IV) PGDEFPORT );
		else if (strEQ("pg_async_status", key))
			retsv = newSViv((IV)imp_dbh->async_status);
		else if (strEQ("pg_expand_array", key))
			retsv = newSViv((IV)imp_dbh->expand_array);
		break;

	case 17: /* pg_server_prepare  pg_server_version */

		if (strEQ("pg_server_prepare", key))
			retsv = newSViv((IV)imp_dbh->server_prepare);
		else if (strEQ("pg_server_version", key))
			retsv = newSViv((IV)imp_dbh->pg_server_version);
		break;

	case 25: /* pg_placeholder_dollaronly */

		if (strEQ("pg_placeholder_dollaronly", key))
			retsv = newSViv((IV)imp_dbh->dollaronly);
		break;
	}
	
	if (!retsv)
		return Nullsv;
	
	if (retsv == &sv_yes || retsv == &sv_no) {
		return retsv; /* no need to mortalize yes or no */
	}
	return sv_2mortal(retsv);

} /* end of dbd_db_FETCH_attrib */


/* ================================================================== */
int dbd_db_STORE_attrib (SV * dbh, imp_dbh_t * imp_dbh, SV * keysv, SV * valuesv)
{
	STRLEN       kl;
	char *       key = SvPV(keysv,kl);
	unsigned int newval = SvTRUE(valuesv);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_STORE (%s) (%d)\n", key, newval);
	
	switch (kl) {

	case 10: /* AutoCommit  pg_bool_tf */

		if (strEQ("AutoCommit", key)) {
			if (newval == DBIc_has(imp_dbh, DBIcf_AutoCommit))
				return 1;
			if (newval!=0) { /* It was off but is now on, so do a final commit */
				if (0!=dbd_db_commit(dbh, imp_dbh) && dbis->debug >= 4)
					(void)PerlIO_printf(DBILOGFP, "dbdpg: Setting AutoCommit to 'on' forced a commit\n");
			}
			DBIc_set(imp_dbh, DBIcf_AutoCommit, newval);
			return 1;
		}
		if (strEQ("pg_bool_tf", key)) {
			imp_dbh->pg_bool_tf = newval!=0 ? DBDPG_TRUE : DBDPG_FALSE;
			return 1;
		}

	case 13: /* pg_errorlevel */

		if (strEQ("pg_errorlevel", key)) {
			/* Introduced in 7.4 servers */
			if (imp_dbh->pg_protocol >= 3) {
				newval = SvIV(valuesv);
				/* Default to "1" if an invalid value is passed in */
				imp_dbh->pg_errorlevel = 0==newval ? 0 : 2==newval ? 2 : 1;
				(void)PQsetErrorVerbosity(imp_dbh->conn, imp_dbh->pg_errorlevel); /* pre-7.4 does nothing */
				if (dbis->debug >= 5)
					(void)PerlIO_printf(DBILOGFP, "dbdpg: Reset error verbosity to %d\n", imp_dbh->pg_errorlevel);
			}
			return 1;
		}

	case 14: /* pg_prepare_now  pg_enable_utf8 */

		if (strEQ("pg_prepare_now", key)) {
			if (imp_dbh->pg_protocol >= 3) {
				imp_dbh->prepare_now = newval ? DBDPG_TRUE : DBDPG_FALSE;
			}
			return 1;
		}

#ifdef is_utf8_string
		if (strEQ("pg_enable_utf8", key)) {
			imp_dbh->pg_enable_utf8 = newval!=0 ? DBDPG_TRUE : DBDPG_FALSE;
			return 1;
		}
#endif

	case 15: /* pg_expand_array */

		if (strEQ("pg_expand_array", key)) {
			imp_dbh->expand_array = newval ? DBDPG_TRUE : DBDPG_FALSE;
			return 1;
		}

	case 17: /* pg_server_prepare */

		if (strEQ("pg_server_prepare", key)) {
			/* No point changing this if the server does not support it */
			if (imp_dbh->pg_protocol >= 3) {
				newval = SvIV(valuesv);
				/* Default to "2" if an invalid value is passed in */
				imp_dbh->server_prepare = 0==newval ? 0 : 1==newval ? 1 : 2;
			}
			return 1;
		}

	case 25: /* pg_placeholder_dollaronly */

		if (strEQ("pg_placeholder_dollaronly", key)) {
			imp_dbh->dollaronly = newval ? DBDPG_TRUE : DBDPG_FALSE;
			return 1;
		}
	}

	return 0;

} /* end of dbd_db_STORE_attrib */


/* ================================================================== */
SV * dbd_st_FETCH_attrib (SV * sth, imp_sth_t * imp_sth, SV * keysv)
{
	STRLEN            kl;
	char *            key = SvPV(keysv,kl);
	SV *              retsv = Nullsv;
	int               fields, x;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_FETCH (%s) sth=%d\n", key, sth);
	
	/* Some can be done before we have a result: */
	switch (kl) {

	case 9: /* pg_direct */

		if (strEQ("pg_direct", key))
			retsv = newSViv((IV)imp_sth->direct);
		break;

	case 10: /* ParamTypes  pg_segments */

		if (strEQ("ParamTypes", key)) {
			HV *pvhv = newHV();
			ph_t *currph;
			int i;
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
		}
		else if (strEQ("pg_segments", key)) {
			AV *arr = newAV();
			seg_t *currseg;
			int i;
			for (i=0,currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg,i++) {
				av_push(arr, newSVpv(currseg->segment ? currseg->segment : "NULL",0));
			}
			retsv = newRV_noinc((SV*)arr);
		}
		break;

	case 11: /* ParamValues */

		if (strEQ("ParamValues", key)) {
			HV *pvhv = newHV();
			ph_t *currph;
			int i;
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
		}
		break;

	case 14: /* pg_prepare_now */

		if (strEQ("pg_prepare_now", key))
			retsv = newSViv((IV)imp_sth->prepare_now);
		break;

	case 15: /* pg_prepare_name */

		if (strEQ("pg_prepare_name", key))
			retsv = newSVpv((char *)imp_sth->prepare_name, 0);
		break;

	case 17: /* pg_server_prepare */

		if (strEQ("pg_server_prepare", key))
			retsv = newSViv((IV)imp_sth->server_prepare);
		break;

	case 25: /* pg_placeholder_dollaronly */

		if (strEQ("pg_placeholder_dollaronly", key))
			retsv = newSViv((IV)imp_sth->dollaronly);
		break;

	}

	if (retsv != Nullsv)
		return retsv;

	if (! imp_sth->result) {
		if (dbis->debug >= 1)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Cannot fetch value of %s pre-execute\n", key);
		return Nullsv;
	}

	fields = DBIc_NUM_FIELDS(imp_sth);
	
	switch (kl) {

	case 4: /* NAME  TYPE */

		if (strEQ("NAME", key)) {
			AV *av = newAV();
			char *fieldname;
			SV * sv_fieldname;
			retsv = newRV(sv_2mortal((SV*)av));
			while(--fields >= 0) {
				//fieldname = newSVpv(PQfname(imp_sth->result, fields),0);
				fieldname = PQfname(imp_sth->result, fields);
				sv_fieldname = newSVpv(fieldname,0);
#ifdef is_utf8_string
				if (is_high_bit_set(fieldname) && is_utf8_string((unsigned char *)fieldname, strlen(fieldname)))
					SvUTF8_on(sv_fieldname);
#endif
				(void)av_store(av, fields, sv_fieldname);
			}
		}
		else if (strEQ("TYPE", key)) {
			/* Need to convert the Pg type to ANSI/SQL type. */
			sql_type_info_t * type_info;
			AV *av = newAV();
			retsv = newRV(sv_2mortal((SV*)av));
			while(--fields >= 0) {
				type_info = pg_type_data((int)PQftype(imp_sth->result, fields));
				(void)av_store(av, fields, newSViv( type_info ? type_info->type.sql : 0 ) );
			}
		}
		break;

	case 5: /* SCALE */

		if (strEQ("SCALE", key)) {
			AV *av = newAV();
			retsv = newRV(sv_2mortal((SV*)av));
			while(--fields >= 0) {
				x = PQftype(imp_sth->result, fields);
				if (PG_NUMERIC==x) {
					x = PQfmod(imp_sth->result, fields)-4;
					(void)av_store(av, fields, newSViv(x % (x>>16)));
				}
				else {
					(void)av_store(av, fields, &sv_undef);
				}
			}
		}
		break;

	case 7: /* pg_size  pg_type */

		if (strEQ("pg_size", key)) {
			AV *av = newAV();
			retsv = newRV(sv_2mortal((SV*)av));
			while(--fields >= 0) {
				(void)av_store(av, fields, newSViv(PQfsize(imp_sth->result, fields)));
			}
		}
		else if (strEQ("pg_type", key)) {
			sql_type_info_t * type_info;
			AV *av = newAV();
			retsv = newRV(sv_2mortal((SV*)av));
			while(--fields >= 0) {			
				type_info = pg_type_data((int)PQftype(imp_sth->result,fields));
				(void)av_store(av, fields, newSVpv(type_info ? type_info->type_name : "unknown", 0));
			}
		}
		break;

	case 8: /* pg_async  NULLABLE */

		if (strEQ("pg_async", key))
			retsv = newSViv((IV)imp_sth->async_flag);
		else if (strEQ("NULLABLE", key)) {
			AV *av = newAV();
			PGresult *result;
			int status = -1;
			D_imp_dbh_from_sth;
			char *statement;
			int nullable; /* 0 = not nullable, 1 = nullable 2 = unknown */
			int y;
			retsv = newRV(sv_2mortal((SV*)av));

			Newx(statement, 100, char); /* freed below */
			statement[0] = '\0';
			while(--fields >= 0) {
				nullable=2;
				x = PQftable(imp_sth->result, fields);
				y = PQftablecol(imp_sth->result, fields);
				if (InvalidOid != x && y > 0) { /* We know what table and column this came from */
					sprintf(statement,
							"SELECT attnotnull FROM pg_catalog.pg_attribute WHERE attrelid=%d AND attnum=%d", x, y);
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
				(void)av_store(av, fields, newSViv(nullable));
			}
			Safefree(statement);
		}
		break;

	case 9: /* PRECISION */

		if (strEQ("PRECISION", key)) {
			AV *av = newAV();
			int sz = 0;
			retsv = newRV(sv_2mortal((SV*)av));
			while(--fields >= 0) {
				x = PQftype(imp_sth->result, fields);
				switch (x) {
				case PG_BPCHAR:
				case PG_VARCHAR:
					sz = PQfmod(imp_sth->result, fields);
					break;
				case PG_NUMERIC:
					sz = PQfmod(imp_sth->result, fields)-4;
					if (sz > 0)
						sz = sz >> 16;
					break;
				default:
					sz = PQfsize(imp_sth->result, fields);
					break;
				}
				(void)av_store(av, fields, sz > 0 ? newSViv(sz) : &sv_undef);
			}
		}
		break;

	case 10: /* CursorName */

		if (strEQ("CursorName", key))
			retsv = &sv_undef;
		break;

	case 11: /* RowsInCache */

		if (strEQ("RowsInCache", key))
			retsv = &sv_undef;
		break;

	case 13: /* pg_oid_status  pg_cmd_status */
		if (strEQ("pg_oid_status", key))
			retsv = newSVuv((unsigned int)PQoidValue(imp_sth->result));
		else if (strEQ("pg_cmd_status", key))
			retsv = newSVpv((char *)PQcmdStatus(imp_sth->result), 0);
		break;

	}

	if (retsv == Nullsv)
		return Nullsv;

	return sv_2mortal(retsv);

} /* end of dbd_st_FETCH_attrib */


/* ================================================================== */
int dbd_st_STORE_attrib (SV * sth, imp_sth_t * imp_sth, SV * keysv, SV * valuesv)
{
	STRLEN kl;
	char * key = SvPV(keysv,kl);
	STRLEN vl;
	char * value = SvPV(valuesv,vl);
	//	unsigned int newval = SvTRUE(valuesv);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_STORE (%s) (%s) sth=%d\n", key, value, sth);
	
	switch (kl) {

	case 8: /* pg_async */

		if (strEQ("pg_async", key)) {
			imp_sth->async_flag = SvIV(valuesv);
			return 1;
		}

	case 14: /* pg_prepare_now */

		if (strEQ("pg_prepare_now", key)) {
			imp_sth->prepare_now = strEQ(value,"0") ? DBDPG_FALSE : DBDPG_TRUE;
			return 1;
		}

	case 15: /* pg_prepare_name */

		if (strEQ("pg_prepare_name", key)) {
			Safefree(imp_sth->prepare_name);
			Newx(imp_sth->prepare_name, vl+1, char); /* freed in dbd_st_destroy */
			Copy(value, imp_sth->prepare_name, vl, char);
			imp_sth->prepare_name[vl] = '\0';
			return 1;
		}

	case 17: /* pg_server_prepare*/

		if (strEQ("pg_server_prepare", key)) {
			imp_sth->server_prepare = strEQ(value,"0") ? DBDPG_FALSE : DBDPG_TRUE;
			return 1;
		}

	case 25: /* pg_placeholder_dollaronly */

		if (strEQ("pg_placeholder_dollaronly", key)) {
			imp_sth->dollaronly = SvTRUE(valuesv) ? DBDPG_TRUE : DBDPG_FALSE;
			return 1;
		}
	}

	return 0;


} /* end of sbs_st_STORE_attrib */


/* ================================================================== */
int dbd_discon_all (SV * drh, imp_drh_t * imp_drh)
{
	
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_discon_all drh=%d\n", drh);

	/* The disconnect_all concept is flawed and needs more work */
	if (!PL_dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
		sv_setiv(DBIc_ERR(imp_drh), (IV)1);
		sv_setpv(DBIc_ERRSTR(imp_drh), "disconnect_all not implemented");
	}

	return 0;

} /* end of dbd_discon_all */


/* Deprecated in favor of $dbh->{pg_socket} */
/* ================================================================== */
int dbd_db_getfd (SV * dbh, imp_dbh_t * imp_dbh)
{

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_getfd dbh=%d\n", dbh);

	return PQsocket(imp_dbh->conn);

} /* end of dbd_db_getfd */


/* ================================================================== */
SV * dbd_db_pg_notifies (SV * dbh, imp_dbh_t * imp_dbh)
{
	int        status;
	PGnotify * notify;
	AV *       ret;
	SV *       retsv;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_db_pg_notifies\n");

	status = PQconsumeInput(imp_dbh->conn);
	if (0 == status) { 
		pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
		return &sv_undef;
	}

	notify = PQnotifies(imp_dbh->conn);

	if (!notify)
		return &sv_undef; 

	ret=newAV();
	av_push(ret, newSVpv(notify->relname,0) );
	av_push(ret, newSViv(notify->be_pid) );
	
 	PQfreemem(notify);

	retsv = newRV(sv_2mortal((SV*)ret));

	return sv_2mortal(retsv);

} /* end of dbd_db_pg_notifies */


/* ================================================================== */
int dbd_st_prepare (SV * sth, imp_sth_t * imp_sth, char * statement, SV * attribs)
{
	D_imp_dbh_from_sth;
	STRLEN mypos=0, wordstart, newsize; /* Used to find and set firstword */
	SV **svp; /* To help parse the arguments */

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_prepare (%s)\n", statement);

	/* Set default values for this statement handle */
	imp_sth->placeholder_type = 0;
	imp_sth->numsegs          = 0;
	imp_sth->numphs           = 0;
	imp_sth->numbound         = 0;
	imp_sth->cur_tuple        = 0;
	imp_sth->rows             = -1; /* per DBI spec */
	imp_sth->totalsize        = 0;
	imp_sth->async_flag       = 0;
	imp_sth->async_status     = 0;
	imp_sth->prepare_name     = NULL;
	imp_sth->firstword        = NULL;
	imp_sth->result	          = NULL;
	imp_sth->type_info        = NULL;
	imp_sth->seg              = NULL;
	imp_sth->ph               = NULL;
	imp_sth->prepared_by_us   = DBDPG_FALSE; /* Set to 1 when actually done preparing */
	imp_sth->onetime          = DBDPG_FALSE; /* Allow internal shortcut */
	imp_sth->direct           = DBDPG_FALSE;
	imp_sth->is_dml           = DBDPG_FALSE; /* Not preparable DML until proved otherwise */
	imp_sth->has_binary       = DBDPG_FALSE; /* Are any of the params binary? */
	imp_sth->has_default      = DBDPG_FALSE; /* Are any of the params DEFAULT? */
	imp_sth->has_current      = DBDPG_FALSE; /* Are any of the params DEFAULT? */


	/* We inherit some preferences from the database handle */
	imp_sth->server_prepare   = imp_dbh->server_prepare;
	imp_sth->prepare_now      = imp_dbh->prepare_now;
	imp_sth->dollaronly       = imp_dbh->dollaronly;

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
		if ((svp = hv_fetch((HV*)SvRV(attribs),"pg_async", 8, 0)) != NULL) {
		  imp_sth->async_flag = SvIV(*svp);
		}
	}

	/* Figure out the first word in the statement */
	while (*statement && isSPACE(*statement)) {
		mypos++;
		statement++;
	}
	if (isALPHA(*statement)) {
		wordstart = mypos;
		while (isALPHA(*statement)) {
			mypos++;
			statement++;
		}
		newsize = mypos-wordstart;
		Newx(imp_sth->firstword, newsize+1, char); /* freed in dbd_st_destroy */
		Copy(statement-newsize, imp_sth->firstword, newsize, char);
		imp_sth->firstword[newsize] = '\0';

		/* Try to prevent transaction commands unless "pg_direct" is set */
		if (0==strcasecmp(imp_sth->firstword, "BEGIN") ||
			0==strcasecmp(imp_sth->firstword, "END") ||
			0==strcasecmp(imp_sth->firstword, "ABORT") ||
			0==strcasecmp(imp_sth->firstword, "COMMIT") ||
			0==strcasecmp(imp_sth->firstword, "ROLLBACK") ||
			0==strcasecmp(imp_sth->firstword, "RELEASE") ||
			0==strcasecmp(imp_sth->firstword, "SAVEPOINT")
			) {
			if (!imp_sth->direct)
				croak ("Please use DBI functions for transaction handling");
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
	if (dbis->debug >= 5)
	(void)PerlIO_printf(DBILOGFP,
	"dbdpg: Immediate prepare decision: dml=%d direct=%d protocol=%d server_prepare=%d prepare_now=%d PGLIBVERSION=%d\n",
	 imp_sth->is_dml, imp_sth->direct, imp_dbh->pg_protocol, imp_sth->server_prepare, imp_sth->prepare_now, PGLIBVERSION);

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

	return 1;

} /* end of dbd_st_prepare */


/* ================================================================== */
static void dbd_st_split_statement (imp_sth_t * imp_sth, int version, char * statement)
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
		if (dbis->debug >= 10)
			(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_split_statement (%s)\n", statement);
		else
			(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_split_statement\n");
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
		imp_sth->numsegs   = 1;
		imp_sth->numphs    = 0;
		imp_sth->totalsize = strlen(statement);

		Newx(imp_sth->seg, 1, seg_t); /* freed in dbd_st_destroy */
		imp_sth->seg->placeholder = 0;
		imp_sth->seg->nextseg     = NULL;
		imp_sth->seg->ph          = NULL;

		if (imp_sth->totalsize > 0) {
			Newx(imp_sth->seg->segment, imp_sth->totalsize+1, char); /* freed in dbd_st_destroy */
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

		/* Quick hack, will work on this more later: */
		if (ch < 0) {
			croak("Invalid string - utf-8 problem?");
		}

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
				Newx(dollarstring, sectionsize, char); /* note: a true array, not a null-terminated string */
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
		Newx(newseg, 1, seg_t); /* freed in dbd_st_destroy */
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
				Newx(newph, 1, ph_t); /* freed in dbd_st_destroy */
				newseg->ph        = newph;
				newph->nextph     = NULL;
				newph->bind_type  = NULL;
				newph->value      = NULL;
				newph->quoted     = NULL;
				newph->referenced = DBDPG_FALSE;
				newph->defaultval = DBDPG_TRUE;
				newph->isdefault  = DBDPG_FALSE;
				newph->iscurrent  = DBDPG_FALSE;
				Newx(newph->fooname, sectionsize+1, char); /* freed in dbd_st_destroy */
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
			Newx(newseg->segment, sectionsize+1, char); /* freed in dbd_st_destroy */
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
			Newx(newph, 1, ph_t); /* freed in dbd_st_destroy */
			newph->nextph     = NULL;
			newph->bind_type  = NULL;
			newph->value      = NULL;
			newph->quoted     = NULL;
			newph->fooname    = NULL;
			newph->referenced = DBDPG_FALSE;
			newph->defaultval = DBDPG_TRUE;
			newph->isdefault  = DBDPG_FALSE;
			newph->iscurrent  = DBDPG_FALSE;
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

	return;

} /* end dbd_st_split_statement */



/* ================================================================== */
static int dbd_st_prepare_statement (SV * sth, imp_sth_t * imp_sth)
{

	D_imp_dbh_from_sth;
	char *       statement;
	unsigned int x;
	STRLEN       execsize;
	PGresult *   result;
	int          status = -1;
	seg_t *      currseg;
	bool         oldprepare = DBDPG_TRUE;
	int          params = 0;
	Oid *        paramTypes = NULL;
	ph_t *       currph;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_prepare_statement\n");

#if PGLIBVERSION >= 80000
	oldprepare = DBDPG_FALSE;
#endif

	Renew(imp_sth->prepare_name, 25, char); /* freed in dbd_st_destroy */

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

	Newx(statement, execsize+1, char); /* freed below */

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
		if (currseg->segment != NULL)
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
		if (result) {
			status = PQresultStatus(result);
			PQclear(result);
		}
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
int dbd_bind_ph (SV * sth, imp_sth_t * imp_sth, SV * ph_name, SV * newvalue, IV sql_type, SV * attribs, int is_inout, IV maxlen)
{

	D_imp_dbh_from_sth;
	char * name = Nullch;
	STRLEN name_len;
	ph_t * currph = NULL;
	int    x, phnum;
	SV **  svp;
	bool   reprepare = DBDPG_FALSE;
	int    pg_type = 0;
	char * value_string = NULL;
	bool   is_array = DBDPG_FALSE;

   	maxlen = 0; /* not used, this makes the compiler happy */

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_bind_ph ph_name: (%s) newvalue: %s(%lu)\n",
							neatsvpv(ph_name,0), neatsvpv(newvalue,0), SvOK(newvalue));

	if (is_inout!=0)
		croak("bind_inout not supported by this driver");

	if (0==imp_sth->numphs)
		croak("Statement has no placeholders to bind");

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
			name++;
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
		else if (SvTYPE(SvRV(newvalue)) == SVt_PVAV) {
			SV * quotedval;
			quotedval = pg_stringify_array(newvalue,",",imp_dbh->pg_server_version);
			currph->valuelen = sv_len(quotedval);
			Renew(currph->value, currph->valuelen+1, char); /* freed in dbd_st_destroy */
			currph->value = SvPVutf8_nolen(quotedval);
			currph->bind_type = pg_type_data(PG_CSTRINGARRAY);
			is_array = DBDPG_TRUE;
		}
		else {
			croak("Cannot bind a reference\n");
		}
	}
	if (dbis->debug >= 5) {
		(void)PerlIO_printf(DBILOGFP, "dbdpg: Bind (%s) <== (%s) (type=%ld)\n", name, neatsvpv(newvalue,0), (long)sql_type);
		if (attribs) {
			(void)PerlIO_printf(DBILOGFP, "dbdpg: Bind attribs (%s)", neatsvpv(attribs,0));
		}
	}

	/* We ignore attribs for these special cases */
	if (currph->isdefault || currph->iscurrent || is_array) {
		if (NULL == currph->bind_type) {
			imp_sth->numbound++;
			currph->bind_type = pg_type_data(PG_UNKNOWN);
		}
		return 1;
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
		currph->bind_type = pg_type_data(PG_UNKNOWN);
		if (!currph->bind_type)
			croak("Default type is bad!!!!???");
	}

	if (pg_type || sql_type) {
		currph->defaultval = DBDPG_FALSE;
		/* Possible re-prepare, depending on whether the type name also changes */
		if (imp_sth->prepared_by_us && NULL != imp_sth->prepare_name)
			reprepare = DBDPG_TRUE;
		/* Mark this statement as having binary if the type is bytea */
		if (PG_BYTEA==currph->bind_type->type_id)
			imp_sth->has_binary = DBDPG_TRUE;
	}

	/* convert to a string ASAP */
	if (!SvPOK(newvalue) && SvOK(newvalue)) {
		(void)sv_2pv(newvalue, &na);
	}

	/* upgrade to at least string */
	(void)SvUPGRADE(newvalue, SVt_PV);

	if (SvOK(newvalue)) {
		value_string = SvPV(newvalue, currph->valuelen);
		Renew(currph->value, currph->valuelen+1, char); /* freed in dbd_st_destroy */
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
			 PG_BYTEA==currph->bind_type->type_id ? "(binary, not shown)" : value_string);

	return 1;

} /* end of dbd_bind_ph */

/* ================================================================== */
SV * pg_stringify_array(SV *input, const char * array_delim, int server_version) {

	AV * toparr;
	AV * currarr;
	AV * lastarr;
	int done;
	int array_depth = 0;
	int array_items;
	int inner_arrays = 0;
	int xy, yz;
	SV * svitem;
	char * string;
	STRLEN svlen;
	SV * value;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_stringify_array\n");

	toparr = (AV *) SvRV(input);
	value = newSVpv("{", 1);

	/* Empty arrays are easy */
	if (av_len(toparr) < 0) {
		av_clear(toparr);
		sv_catpv(value, "}");
		return value;
	}

	done = 0;
	currarr = lastarr = toparr;
	while (!done) {
		/* Grab the first item of the current array */
		svitem = *av_fetch(currarr, 0, 0);

		/* If a ref, die if not an array, else keep descending */
		if (SvROK(svitem)) {
			if (SvTYPE(SvRV(svitem)) != SVt_PVAV)
				croak("Arrays must contain only scalars and other arrays");
			array_depth++;

			/* Squirrel away this level before we leave it */
			lastarr = currarr;

			/* Set the current array to this item */
			currarr = (AV *)SvRV(svitem);

			/* If this is an empty array, stop here */
			if (av_len(currarr) < 0)
				done = 1;
		}
		else
			done = 1;
	}

	inner_arrays = array_depth ? 1+av_len(lastarr) : 0;

	/* How many items are in each inner array? */
	array_items = array_depth ? (1+av_len((AV*)SvRV(*av_fetch(lastarr,0,0)))) : 1+av_len(lastarr);

	for (xy=1; xy < array_depth; xy++) {
		sv_catpv(value, "{");
	}

	for (xy=0; xy < inner_arrays || !array_depth; xy++) {
		if (array_depth) {
			svitem = *av_fetch(lastarr, xy, 0);
			if (!SvROK(svitem))
				croak ("Not a valid array!");
			currarr = (AV*)SvRV(svitem);
			if (SvTYPE(currarr) != SVt_PVAV)
				croak("Arrays must contain only scalars and other arrays!");
			if (1+av_len(currarr) != array_items)
				croak("Invalid array - all arrays must be of equal size");
			sv_catpv(value, "{");
		}
		for (yz=0; yz < array_items; yz++) {
			svitem = *av_fetch(currarr, yz, 0);

			if (SvROK(svitem))
				croak("Arrays must contain only scalars and other arrays");

			if (!SvOK(svitem)) { /* Insert NULL if we can */
				/* Only version 8.2 and up can handle NULLs in arrays */
				if (server_version < 80200)
					croak("Cannot use NULLs in arrays until version 8.2");
				sv_catpv(value, "NULL"); /* Beware of array_nulls config param! */
			}
			else {
				sv_catpv(value, "\"");
				string = SvPV(svitem, svlen);
				while (svlen--) {
					sv_catpvf(value, "%s%c", /* upgrades to utf8 for us */
							  '\"'==*string ? "\\" : 
							  '\\'==*string ? "\\\\" :
							  "", *string);
					string++;
				}
				sv_catpv(value, "\"");
			}

			if (yz < array_items-1)
				sv_catpv(value, array_delim);
		}

		if (!array_items) {
			sv_catpv(value, "\"\"");
		}

		sv_catpv(value, "}");
		if (xy < inner_arrays-1)
			sv_catpv(value, array_delim);
		if (!array_depth)
			break;
	}

	for (xy=0; xy<array_depth; xy++) {
		sv_catpv(value, "}");
	}

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_stringify_array returns %s\n", neatsvpv(value,0));

	return value;

} /* end of pg_stringify_array */

/* ================================================================== */
SV * pg_destringify_array(char * input, sql_type_info_t * coltype) {

	AV*    av;              /* The main array we are returning a reference to */
	AV*    newav;           /* Temporary array */
	AV*    currentav;       /* The current array level */
	AV*    topav;           /* Where each item starts at */
	char*  string;
	STRLEN section_size = 0;
	bool   in_quote = 0;
	int    opening_braces = 0;
	int    closing_braces = 0;

	/*
	  Note: we don't do careful balance checking here, as this is coming straight from 
	  the Postgres backend, and we rely on it to give us a sane and balanced structure
	*/

	/* Eat the opening brace and perform a sanity check */
	if ('{' != *(input++))
		croak("Tried to destringify a non-array!: %s", input);

	/* Count how deep this array goes */
	while ('{' == *input) {
		opening_braces++;
		input++;
	}
	input -= opening_braces;

	Newx(string, strlen(input), char); /* Freed at end of this function */
	string[0] = '\0';

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_stringify_array: -->%s<-- quote=%c\n", input, coltype->array_delimeter);

	av = currentav = topav = newAV();

	while (*input != '\0') {
		if (in_quote) {
			if ('"' == *input) {
				in_quote = 0;
				/* String will be stored by following delim or brace */
				input++;
				continue;
			}
			if ('\\' == *input) { /* Eat backslashes */
				input++;
			}
			string[section_size++] = *input;
		}
		else if ('{' == *input) {
			newav = newAV();
			av_push(currentav, newRV_noinc((SV*)newav));
			currentav = newav;
		}
		else if (coltype->array_delimeter == *input) {
		}
		else if ('}' == *input) {
		}
		else if ('"' == *input) {
			in_quote = 1;
		}
		else {
			string[section_size++] = *input;
		}

		if ('}' == *input
			|| (coltype->array_delimeter == *input && '}' != *(input-1))) {
			string[section_size] = '\0';
			if (4 == section_size && 0 == strncmp(string, "NULL", 4) && '"' != *(input-1)) {
				av_push(currentav, &PL_sv_undef);
			}
			else {
				if (1 == coltype->svtype)
					av_push(currentav, newSViv(SvIV(newSVpvn(string,section_size))));
				else if (2 == coltype->svtype)
					av_push(currentav, newSVnv(SvNV(newSVpvn(string,section_size))));
				else
					av_push(currentav, newSVpvn(string, section_size));
			}
			section_size = 0;
		}

		/* Handle all touching closing braces */
		if ('}' == *input) {
			if (closing_braces) {
				while ('}' == *input) {
					input++;
				}
			}
			else {
				while ('}' == *input) {
					closing_braces++;
					input++;
				}
				/* Set the new topav if required */
				if ('\0' != *input && opening_braces > closing_braces) {
					closing_braces = opening_braces - closing_braces;
					while (closing_braces--) {
						topav = (AV*)SvRV(AvARRAY(topav)[0]);
					}
				}
			}
			currentav = topav;
		}
		else {
			input++;
		}
	}
	Safefree(string);

	return newRV((SV*)av);

} /* end of pg_destringify_array */


/* ================================================================== */
int pg_quickexec (SV * dbh, const char * sql, int asyncflag)
{
	D_imp_dbh(dbh);
	PGresult *     result;
	ExecStatusType status = PGRES_FATAL_ERROR; /* Assume the worst */
	char *         cmdStatus = NULL;
	int            rows = 0;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbdpg_quickexec query=(%s) async=(%d) async_status=(%d)\n",
							sql, asyncflag, imp_dbh->async_status);

	if (NULL == imp_dbh->conn)
		croak("execute on disconnected handle");

	/* Abort if we are in the middle of a copy */
	if (imp_dbh->copystate!=0)
		croak("Must call pg_endcopy before issuing more commands");

	/* If we are still waiting on an async, handle it */
	if (imp_dbh->async_status) {
	  if (dbis->debug >= 4) (void)PerlIO_printf(DBILOGFP, "dbdpg: handling old async\n");
	  rows = handle_old_async(dbh, imp_dbh, asyncflag);
	  if (rows)
		return rows;
	}

	/* If not autocommit, start a new transaction */
	if (!imp_dbh->done_begin && !DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
		status = _result(imp_dbh, "begin");
		if (PGRES_COMMAND_OK != status) {
			pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
			return -2;
		}
		imp_dbh->done_begin = DBDPG_TRUE;
	}

	/* Asynchronous commands get kicked off and return undef */
	if (asyncflag & PG_ASYNC) {
	  if (dbis->debug >= 4) (void)PerlIO_printf(DBILOGFP, "dbdpg: Going asychronous with do()\n");
	  if (! PQsendQuery(imp_dbh->conn, sql)) {
		if (dbis->debug >= 4) (void)PerlIO_printf(DBILOGFP, "dbdpg: PQsendQuery failed\n");
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		return -2;
	  }
	  imp_dbh->async_status = 1;
	  imp_dbh->async_sth = NULL; // Needed?
	  if (dbis->debug >= 4) (void)PerlIO_printf(DBILOGFP, "dbdpg: PQsendQuery worked\n");
	  return 0;
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
/* Return value <= -2:error, >=0:ok row count, (-1=unknown count) */
int dbd_st_execute (SV * sth, imp_sth_t * imp_sth)
{
	D_imp_dbh_from_sth;
	ph_t *        currph;
	int           status = -1;
	STRLEN        execsize, x;
	const char ** paramValues = NULL;
	int *         paramLengths = NULL;
	int *         paramFormats = NULL;
	Oid *         paramTypes = NULL;
	seg_t *       currseg;
	char *        statement = NULL;
	char *        cmdStatus = NULL;
	int           num_fields;
	int           ret = -2;
	
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_execute\n");
	
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

	/* Check for old async transactions */
	if (imp_dbh->async_status) {
	  if (dbis->debug >= 7) {
		(void)PerlIO_printf
		  (DBILOGFP, "dbdpg: Attempting to handle existing async transaction\n");
	  }	  
	  ret = handle_old_async(sth, imp_dbh, imp_sth->async_flag);
	  if (ret)
		return ret;
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
	if (imp_sth->result) {
		PQclear(imp_sth->result);
		imp_sth->result = NULL;
	}

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
				strncpy(currph->quoted, "DEFAULT", 8);
				currph->quotedlen = 7;
			}
			else if (currph->iscurrent) {
				Renew(currph->quoted, 8, char); /* freed in dbd_st_destroy */
				strncpy(currph->quoted, "CURRENT_TIMESTAMP", 18);
				currph->quotedlen = 17;
			}
			else if (NULL == currph->value) {
				Renew(currph->quoted, 5, char); /* freed in dbd_st_destroy */
				strncpy(currph->quoted, "NULL", 5);
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
				if (PG_BYTEA==currph->bind_type->type_id) {
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
		if (imp_sth->async_flag & PG_ASYNC)
		  ret = PQsendQueryPrepared
			(imp_dbh->conn, imp_sth->prepare_name, imp_sth->numphs, paramValues, paramLengths, paramFormats, 0);
		else
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
			Newx(statement, execsize+1, char); /* freed below */
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
			if (imp_sth->async_flag & PG_ASYNC)
			  ret = PQsendQueryParams
				(imp_dbh->conn, statement, imp_sth->numphs, paramTypes, paramValues, paramLengths, paramFormats, 0);
			else
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

			Newx(statement, execsize+1, char); /* freed below */
			statement[0] = '\0';
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				strcat(statement, currseg->segment);
				if (currseg->placeholder!=0)
					strcat(statement, currseg->ph->quoted);
			}
			statement[execsize] = '\0';

			if (dbis->debug >= 5)
				(void)PerlIO_printf(DBILOGFP, "dbdpg: Running %s with (%s)\n", 
									imp_sth->async_flag & 1 ? "PQsendQuery" : "PQexec", statement);
			
			if (imp_sth->async_flag & PG_ASYNC)
			  ret = PQsendQuery(imp_dbh->conn, statement);
			else
			  imp_sth->result = PQexec(imp_dbh->conn, statement);

		} /* end PQexec */

		Safefree(statement);

	} /* end non-prepared exec */

	/* Some form of PQexec/PQsendQuery has been run at this point */

	Safefree(paramValues);
	Safefree(paramLengths);
	Safefree(paramFormats);			

	/* If running asynchronously, we don't stick around for the result */
	if (imp_sth->async_flag & PG_ASYNC) {
		if (dbis->debug >= 2)
		  (void)PerlIO_printf
			(DBILOGFP, "dbdpg: Early return for async query");
		imp_dbh->async_status = 1;
		imp_sth->async_status = 1;
		imp_dbh->async_sth = imp_sth;
		return 0;
	}

	status = _sqlstate(imp_dbh, imp_sth->result);

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
static int is_high_bit_set(char * val)
{
	while (*val)
		if (*val++ & 0x80) return 1;
	return 0;
}


/* ================================================================== */
AV * dbd_st_fetch (SV * sth, imp_sth_t * imp_sth)
{
	D_imp_dbh_from_sth;
	sql_type_info_t * type_info;
	int               num_fields;
	char *            value;
	char *            p;
	int               i;
	int               chopblanks;
	STRLEN            value_len = 0;
	STRLEN            len;
	AV *              av;
	
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_fetch\n");

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
			if (imp_sth->type_info[i] == NULL) {
				if (dbis->debug >= 1)
					(void)PerlIO_printf(DBILOGFP, "dbdpg: Unknown type returned by Postgres: %d. Setting to UNKNOWN\n",
										PQftype(imp_sth->result, i));
				imp_sth->type_info[i] = pg_type_data(PG_UNKNOWN);
			}
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

			if (type_info
				&& 0 == strncmp(type_info->arrayout, "array", 5)
				&& imp_dbh->expand_array) {
				AvARRAY(av)[i] = pg_destringify_array(value, type_info);
			}
			else {
				if (type_info) {
					type_info->dequote(value, &value_len); /* dequote in place */
					if (PG_BOOL == type_info->type_id && imp_dbh->pg_bool_tf)
						*value = ('1' == *value) ? 't' : 'f';
				}
				else
					value_len = strlen(value);
			
				sv_setpvn(sv, value, value_len);
			
				if (type_info && (PG_BPCHAR == type_info->type_id) && chopblanks) {
					p = SvEND(sv);
					len = SvCUR(sv);
					while(len && ' ' == *--p)
						--len;
					if (len != SvCUR(sv)) {
						SvCUR_set(sv, len);
						*SvEND(sv) = '\0';
					}
				}
			}
#ifdef is_utf8_string
			if (imp_dbh->pg_enable_utf8 && type_info) {
				SvUTF8_off(sv);
				switch (type_info->type_id) {
				case PG_CHAR:
				case PG_TEXT:
				case PG_BPCHAR:
				case PG_VARCHAR:
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
/* Pop off savepoints to the specified savepoint name */
static void pg_db_free_savepoints_to (SV * dbh, imp_dbh_t * imp_dbh, char * savepoint)
{
	I32 i;
	for (i = av_len(imp_dbh->savepoints); i >= 0; i--) {
		SV * elem = av_pop(imp_dbh->savepoints);
		if (strEQ(SvPV_nolen(elem), savepoint)) {
			sv_2mortal(elem);
			break;
		}
		sv_2mortal(elem);
	}
}


/* ================================================================== */
int dbd_st_rows (SV * sth, imp_sth_t * imp_sth)
{
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_rows sth=%d\n", sth);
	
	return imp_sth->rows;

} /* end of dbd_st_rows */


/* ================================================================== */
int dbd_st_finish (SV * sth, imp_sth_t * imp_sth)
{
	
	D_imp_dbh_from_sth;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbdpg_finish sth=%d async=%d\n",
							sth, imp_dbh->async_status);
	
	if (DBIc_ACTIVE(imp_sth) && imp_sth->result) {
		PQclear(imp_sth->result);
		imp_sth->result = NULL;
		imp_sth->rows = 0;
	}
	
	/* Are we in the middle of an async for this statement handle? */
	if (imp_dbh->async_status) {
	  if (imp_sth->async_status) {
		handle_old_async(sth, imp_dbh, PG_OLDQUERY_WAIT);
	  }
	}

	imp_sth->async_status = 0;
	imp_dbh->async_sth = NULL;

	DBIc_ACTIVE_off(imp_sth);
	return 1;

} /* end of sbs_st_finish */


/* ================================================================== */
static int dbd_st_deallocate_statement (SV * sth, imp_sth_t * imp_sth)
{
	D_imp_dbh_from_sth;
	char                    tempsqlstate[6];
	char *                  stmt;
	int                     status;
	PGTransactionStatusType tstatus;
	
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_deallocate_statement\n");

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
				Newx(cmd, SvLEN(sp) + 13, char); /* Freed below */
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

	Newx(stmt, strlen("DEALLOCATE ") + strlen(imp_sth->prepare_name) + 1, char); /* freed below */

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
void dbd_st_destroy (SV * sth, imp_sth_t * imp_sth)
{
	D_imp_dbh_from_sth;
	seg_t * currseg;
	seg_t * nextseg;
	ph_t *  currph;
	ph_t *  nextph;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_destroy\n");

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

	if (imp_dbh->async_status) {
	  handle_old_async(sth, imp_dbh, PG_OLDQUERY_WAIT);
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

	if (imp_sth->result) {
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

	if (imp_dbh->async_sth)
		imp_dbh->async_sth = NULL;

	DBIc_IMPSET_off(imp_sth); /* let DBI know we've done it */

} /* end of dbd_st_destroy */


/* ================================================================== */
int
pg_db_putline (SV * dbh, const char * buffer)
{
	D_imp_dbh(dbh);
	int copystatus;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_putline\n");

	/* We must be in COPY IN state */
	if (PGRES_COPY_IN != imp_dbh->copystate)
		croak("pg_putline can only be called directly after issuing a COPY IN command\n");

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
}


/* ================================================================== */
int
pg_db_getline (SV * dbh, SV * svbuf, int length)
{
	D_imp_dbh(dbh);
	int    copystatus;
	char * tempbuf;
	char * buffer;

	buffer = SvPV_nolen(svbuf);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_getline\n");

	tempbuf = NULL;

	/* We must be in COPY OUT state */
	if (PGRES_COPY_OUT != imp_dbh->copystate)
		croak("pg_getline can only be called directly after issuing a COPY OUT command\n");

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
		sv_setpv(svbuf, tempbuf);
		PQfreemem(tempbuf);
	}
	return 0;

}


/* ================================================================== */
int pg_db_endcopy (SV * dbh)
{
	D_imp_dbh(dbh);
	int            copystatus;
	PGresult *     result;
	ExecStatusType status;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_pg_endcopy\n");

	if (0==imp_dbh->copystate)
		croak("pg_endcopy cannot be called until a COPY is issued");

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

	imp_dbh->copystate = 0;
	return copystatus;
}


/* ================================================================== */
void pg_db_pg_server_trace (SV * dbh, FILE * fh)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_pg_server_trace\n");

	PQtrace(imp_dbh->conn, fh);
}


/* ================================================================== */
void pg_db_pg_server_untrace (SV * dbh)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_pg_server_untrace\n");

	PQuntrace(imp_dbh->conn);
}


/* ================================================================== */
int pg_db_savepoint (SV * dbh, imp_dbh_t * imp_dbh, char * savepoint)
{
	int    status;
	char * action;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_savepoint (%s)\n", savepoint);

	Newx(action, strlen(savepoint) + 11, char); /* freed below */

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
int pg_db_rollback_to (SV * dbh, imp_dbh_t * imp_dbh, char * savepoint)
{
	int    status;
	I32    i;
	char * action;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_rollback_to (%s)\n", savepoint);

	Newx(action, strlen(savepoint) + 13, char);

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

	pg_db_free_savepoints_to(dbh, imp_dbh, savepoint);
	return 1;
}


/* ================================================================== */
int pg_db_release (SV * dbh, imp_dbh_t * imp_dbh, char * savepoint)
{
	int    status;
	I32    i;
	char * action;

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_release (%s)\n", savepoint);

	Newx(action, strlen(savepoint) + 9, char);

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

	pg_db_free_savepoints_to(dbh, imp_dbh, savepoint);
	return 1;
}


/* ================================================================== */
/* Used to ensure we are in a txn, e.g. the lo_ functions below */
static int pg_db_start_txn (SV * dbh, imp_dbh_t * imp_dbh)
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


/* Large object functions */

/* ================================================================== */
unsigned int pg_db_lo_creat (SV * dbh, int mode)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_creat (%d)\n", mode);

	if (!pg_db_start_txn(dbh,imp_dbh))
		return 0; /* No other option, because lo_creat returns an Oid */

	return lo_creat(imp_dbh->conn, mode); /* 0 on error */
}

/* ================================================================== */
int pg_db_lo_open (SV * dbh, unsigned int lobjId, int mode)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_open (%d) (%d)\n", lobjId, mode);

	if (!pg_db_start_txn(dbh,imp_dbh))
		return -2;

	return lo_open(imp_dbh->conn, lobjId, mode); /* -1 on error */
}

/* ================================================================== */
int pg_db_lo_close (SV * dbh, int fd)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_close (%d)\n", fd);

	return lo_close(imp_dbh->conn, fd); /* <0 on error, 0 if ok */
}

/* ================================================================== */
int pg_db_lo_read (SV * dbh, int fd, char * buf, size_t len)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_read (%d) (%d)\n", fd, len);

	return lo_read(imp_dbh->conn, fd, buf, len); /* bytes read, <0 on error */
}

/* ================================================================== */
int pg_db_lo_write (SV * dbh, int fd, char * buf, size_t len)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_write (%d) (%d)\n", fd, len);

	return lo_write(imp_dbh->conn, fd, buf, len); /* bytes written, <0 on error */
}

/* ================================================================== */
int pg_db_lo_lseek (SV * dbh, int fd, int offset, int whence)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_lseek (%d) (%d) (%d)\n", fd, offset, whence);

	return lo_lseek(imp_dbh->conn, fd, offset, whence); /* new position, -1 on error */
}

/* ================================================================== */
int pg_db_lo_tell (SV * dbh, int fd)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_tell (%d)\n", fd);

	return lo_tell(imp_dbh->conn, fd); /* current position, <0 on error */
}

/* ================================================================== */
int pg_db_lo_unlink (SV * dbh, unsigned int lobjId)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_unlink (%d)\n", lobjId);

	if (!pg_db_start_txn(dbh,imp_dbh))
		return -2;

	return lo_unlink(imp_dbh->conn, lobjId); /* 1 on success, -1 on failure */
}

/* ================================================================== */
unsigned int pg_db_lo_import (SV * dbh, char * filename)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_import (%s)\n", filename);

	if (!pg_db_start_txn(dbh,imp_dbh))
		return 0; /* No other option, because lo_import returns an Oid */

	return lo_import(imp_dbh->conn, filename); /* 0 on error */
}

/* ================================================================== */
int pg_db_lo_export (SV * dbh, unsigned int lobjId, char * filename)
{
	D_imp_dbh(dbh);

	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: pg_db_lo_export id:(%d) file:(%s)\n", lobjId, filename);

	if (!pg_db_start_txn(dbh,imp_dbh))
		return -2;

	return lo_export(imp_dbh->conn, lobjId, filename); /* 1 on success, -1 on failure */
}


/* ================================================================== */
int dbd_st_blob_read (SV * sth, imp_sth_t * imp_sth, int lobjId, long offset, long len, SV * destrv, long destoffset)
{
	D_imp_dbh_from_sth;

	int    ret, lobj_fd, nbytes;
	STRLEN nread;
	SV *   bufsv;
	char * tmp;
	
	if (dbis->debug >= 4)
		(void)PerlIO_printf(DBILOGFP, "dbdpg: dbd_st_blob_read (%d) (%d) (%d)\n", lobjId, offset, len);

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

} /* end of dbd_st_blob_read */



/* ================================================================== */
/* Return the result of an asynchronous query, waiting if needed */
int dbdpg_result (h, imp_dbh)
		 SV *h;
		 imp_dbh_t *imp_dbh;
{

	PGresult *result;
	ExecStatusType status = PGRES_FATAL_ERROR;
	int rows;
	char *cmdStatus = NULL;

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbdpg_result\n"); }

	if (1 != imp_dbh->async_status) {
		pg_error(h, PGRES_FATAL_ERROR, "No asynchronous query is running\n");
		return -2;
	}	

	imp_dbh->copystate = 0; /* Assume not in copy mode until told otherwise */

	while ((result = PQgetResult(imp_dbh->conn)) != NULL) {
	  /* TODO: Better multiple result-set handling */
	  status = _sqlstate(imp_dbh, result);
	  switch (status) {
	  case PGRES_TUPLES_OK:
		rows = PQntuples(result);

		if (imp_dbh->async_sth) {
		  imp_dbh->async_sth->cur_tuple = 0;
		  DBIc_NUM_FIELDS(imp_dbh->async_sth) = PQnfields(result);
		  DBIc_ACTIVE_on(imp_dbh->async_sth);
		}

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
		pg_error(h, status, PQerrorMessage(imp_dbh->conn));
		break;
	  case PGRES_FATAL_ERROR:
	  default:
		rows = -2;
		pg_error(h, status, PQerrorMessage(imp_dbh->conn));
		break;
	  }

	  if (imp_dbh->async_sth) {
		if (imp_dbh->async_sth->result) /* For potential multi-result sets */
		  PQclear(imp_dbh->async_sth->result);
		imp_dbh->async_sth->result = result;
	  }
	  else {
		  PQclear(result);
	  }
	}

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbdpg_result returning %d\n", rows); }
	if (imp_dbh->async_sth) {
	  imp_dbh->async_sth->rows = rows;
	  imp_dbh->async_sth->async_status = 0;
	}
	imp_dbh->async_status = 0;
	return rows;

} /* end of dbdpg_result */


/* 
==================================================================
Indicates if an asynchronous query has finished yet
Accepts either a database or a statement handle
Returns:
  -1 if no query is running (and raises an exception)
  +1 if the query is finished
   0 if the query is still running
  -2 for other errors
==================================================================
*/

int dbdpg_ready (h, imp_dbh)
		 SV *h;
		 imp_dbh_t *imp_dbh;
{

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: pg_st_ready\n"); }

	if (0 == imp_dbh->async_status) {
		pg_error(h, PGRES_FATAL_ERROR, "No asynchronous query is running\n");
		return -1;
	}	

	if (!PQconsumeInput(imp_dbh->conn)) {
	  pg_error(h, PGRES_FATAL_ERROR, PQerrorMessage(imp_dbh->conn));
	  return -2;
	}

	return ! PQisBusy(imp_dbh->conn);

} /* end of dbdpg_ready */


/*
Attempt to cancel a running asynchronous query
Returns true if the cancel succeeded, and false if it did not
If it did successfully cancel the query, it will also do a rollback.
Note that queries which have finished do not cause a rollback.
In this case, pg_cancel will return false.
NOTE: We only return true if we cancelled and rolled back!
*/

int dbdpg_cancel(h, imp_dbh)
	 SV *h;
	 imp_dbh_t *imp_dbh;
{

	PGcancel *cancel;
	char errbuf[256];
	PGresult *result;
	ExecStatusType status;

	if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: dbdpg_cancel, async=%d\n", imp_dbh->async_status); }

	if (0 == imp_dbh->async_status) {
	  pg_error(h, PGRES_FATAL_ERROR, "No asynchronous query is running");
	  return DBDPG_FALSE;
	}

	if (-1 == imp_dbh->async_status) {
	  pg_error(h, PGRES_FATAL_ERROR, "Asychronous query has already been cancelled");
	  return DBDPG_FALSE;
	}

	/* Get the cancel structure */
	cancel = PQgetCancel(imp_dbh->conn);

	/* This almost always works. If not, free our structure and complain looudly */
	if (! PQcancel(cancel,errbuf,sizeof(errbuf))) {
	  PQfreeCancel(cancel);
	  if (dbis->debug >= 1) { (void)PerlIO_printf(DBILOGFP, "dbdpg: PQcancel failed: %s\n", errbuf); }
	  pg_error(h, PGRES_FATAL_ERROR, "PQcancel failed");
	  return DBDPG_FALSE;
	}
	PQfreeCancel(cancel);

	/* Whatever else happens, we should no longer be inside of an async query */
	imp_dbh->async_status = -1;
	if (imp_dbh->async_sth)
	  imp_dbh->async_sth->async_status = -1;

	/* Read in the result - assume only one */
	result = PQgetResult(imp_dbh->conn);
	if (!result) {
	  pg_error(h, PGRES_FATAL_ERROR, "Failed to get a result after PQcancel");
	  return DBDPG_FALSE;
	}

	status = _sqlstate(imp_dbh, result);

	/* If we actually cancelled a running query, perform a rollback */
	if (0 == strncmp(imp_dbh->sqlstate, "57014", 5)) {
	  if (dbis->debug >= 0) { (void)PerlIO_printf(DBILOGFP, "dbdpg: Rolling back after cancelled query\n"); }
	  dbd_db_rollback(h, imp_dbh);
	  //	  PQexec(imp_dbh->conn, "ROLLBACK");
	  return DBDPG_TRUE;
	}

	/* If we got any other error, make sure we report it */
	if (0 != strncmp(imp_dbh->sqlstate, "00000", 5)) {
	  if (dbis->debug >= 0) { (void)PerlIO_printf(DBILOGFP, "dbdpg: Query was not cancelled: was already finished\n"); }
	  pg_error(h, status, PQerrorMessage(imp_dbh->conn));
	}
	
	return DBDPG_FALSE;

} /* end of dbdpg_cancel */


int dbdpg_cancel_sth(sth, imp_sth)
	 SV *sth;
	 imp_sth_t *imp_sth;
{

    D_imp_dbh_from_sth;
	bool cancel_result;

	cancel_result = dbdpg_cancel(sth, imp_dbh);

	dbd_st_finish(sth, imp_sth);

	return cancel_result;

} /* end of dbdpg_cancel */


/*
Finish up an existing async query, either by cancelling it,
or by waiting for a result.

 */
static int handle_old_async(SV * handle, imp_dbh_t * imp_dbh, int asyncflag)
{

  PGresult *result;
  ExecStatusType status;

  if (dbis->debug >= 4) { (void)PerlIO_printf(DBILOGFP, "dbdpg: handle_old_sync flag=%d\n", asyncflag); }

  if (asyncflag & PG_OLDQUERY_CANCEL) {
	/* Cancel the outstanding query */
	if (dbis->debug >= 1) { (void)PerlIO_printf(DBILOGFP, "dbdpg: Cancelling old async command\n"); }
	if (PQisBusy(imp_dbh->conn)) {
	  PGcancel *cancel;
	  char errbuf[256];
	  int cresult;
	  if (dbis->debug >= 1) { (void)PerlIO_printf(DBILOGFP, "dbdpg: Attempting to cancel query\n"); }
	  cancel = PQgetCancel(imp_dbh->conn);
	  cresult = PQcancel(cancel,errbuf,255);
	  if (! cresult) {
		if (dbis->debug >= 1) { (void)PerlIO_printf(DBILOGFP, "dbdpg: PQcancel failed: %s\n", errbuf); }
		pg_error(handle, PGRES_FATAL_ERROR, "Could not cancel previous command");
		return -2;
	  }
	  PQfreeCancel(cancel);
	  /* Suck up the cancellation notice */
	  while ((result = PQgetResult(imp_dbh->conn)) != NULL) {
	  }
	  /* We need to rollback! - reprepare!? */
	  PQexec(imp_dbh->conn, "rollback");
	  imp_dbh->done_begin = DBDPG_FALSE;
	}
  }
  else if (asyncflag & PG_OLDQUERY_WAIT || imp_dbh->async_status == -1) {
	/* Finish up the outstanding query and throw out the result, unless an error */
	if (dbis->debug >= 1) { (void)PerlIO_printf(DBILOGFP, "dbdpg: Waiting for old async command to finish\n"); }
	while ((result = PQgetResult(imp_dbh->conn)) != NULL) {
	  status = _sqlstate(imp_dbh, result);
	  PQclear(result);
	  if (status == PGRES_COPY_IN) { /* In theory, this should be caught by copystate, but we'll be careful */
		if (-1 == PQputCopyEnd(imp_dbh->conn, NULL)) {
		  pg_error(handle, PGRES_FATAL_ERROR, PQerrorMessage(imp_dbh->conn));
		  return -2;
		}
	  }
	  else if (status == PGRES_COPY_OUT) { /* Won't be as nice with this one */
		pg_error(handle, PGRES_FATAL_ERROR, "Must finish copying first");
		return -2;
	  }
	  else if (status != PGRES_EMPTY_QUERY
			   && status != PGRES_COMMAND_OK
			   && status != PGRES_TUPLES_OK) {
		pg_error(handle, status, PQerrorMessage(imp_dbh->conn));
		return -2;
	  }
	}
  }
  else {
	pg_error(handle, PGRES_FATAL_ERROR, "Cannot execute until previous async query has finished");
	return -2;
  }

  /* If we made it this far, safe to assume there is no running query */
  imp_dbh->async_status = 0;
  if (imp_dbh->async_sth)
  	imp_dbh->async_sth->async_status = 0;

  return 0;

} /* end of handle_old_async */



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

