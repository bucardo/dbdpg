/*

   $Id$

   Copyright (c) 2002-2005 PostgreSQL Global Development Group
   Portions Copyright (c) 2002 Jeffrey W. Baker
   Portions Copyright (c) 1997-2000 Edmund Mergl
   Portions Copyright (c) 1994-1997 Tim Bunce
   
   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


#include "Pg.h"
#include <math.h>

#define DBDPG_TRUE 1
#define DBDPG_FALSE 0

/* strcasecmp() does not exist on Windows (!) */
#ifdef WIN32
#define strcasecmp(s1,s2) lstrcmpiA((s1), (s2))
#endif

/* XXX DBI should provide a better version of this */
#define IS_DBI_HANDLE(h) (SvROK(h) && SvTYPE(SvRV(h)) == SVt_PVHV && SvRMAGICAL(SvRV(h)) && (SvMAGIC(SvRV(h)))->mg_type == 'P')

DBISTATE_DECLARE;

/* Someday, we can abandon pre-7.4 and life will be much easier... */
#if PGLIBVERSION < 70400
/* Limited emulation - use with care! And upgrade already... :) */
typedef enum
{
	PQTRANS_IDLE,				  /* connection idle */
	PQTRANS_ACTIVE,				/* command in progress */
	PQTRANS_INTRANS,			/* idle, within transaction block */
	PQTRANS_INERROR,			/* idle, within failed transaction */
	PQTRANS_UNKNOWN				/* cannot determine status */
} PGTransactionStatusType;
PGresult *PQexecPrepared() { croak("Called wrong PQexecPrepared\n"); }
PGresult *PQexecParams() { croak("Called wrong PQexecParams\n"); }
Oid PQftable() { return InvalidOid; }
int PQftablecol() { return 0; }
int PQsetErrorVerbosity() { return 0; }
#define PG_DIAG_SQLSTATE 'C'
#endif

/* an important feature was left out of libpq for 7.4, so we need this check */
#if PGLIBVERSION < 80000
PGresult *PQprepare() { croak ("Called wrong PQprepare"); }
#endif

#ifndef PGErrorVerbosity
typedef enum
{
	PGERROR_TERSE,				/* single-line error messages */
	PGERROR_DEFAULT,			/* recommended style */
	PGERROR_VERBOSE				/* all the facts, ma'am */
} PGErrorVerbosity;
#endif

void pg_error();
ExecStatusType _result();
void dbd_st_split_statement();
int dbd_st_prepare_statement();
int dbd_db_transaction_status();
int dbd_st_deallocate_statement();
PGTransactionStatusType dbd_db_txn_status();
#include "large_object.c"

/* ================================================================== */
/* Quick result grabber used throughout this file */
ExecStatusType _result(imp_dbh, com)
		 imp_dbh_t *imp_dbh;
		 const char *com;
{
	PGresult *result;
	ExecStatusType status;

	result = PQexec(imp_dbh->conn, com);

	status = result ? PQresultStatus(result) : -1;

#if PGLIBVERSION >= 70400
	strncpy(imp_dbh->sqlstate,
					NULL == PQresultErrorField(result,PG_DIAG_SQLSTATE) ? "00000" : 
					PQresultErrorField(result,PG_DIAG_SQLSTATE),
					5);
	imp_dbh->sqlstate[5] = '\0';
#else
	strcpy(imp_dbh->sqlstate, "S1000"); /* DBI standard says this is the default */
#endif

	PQclear(result);

	return status;

} /* end of _result */


/* ================================================================== */
/* Turn database notices into perl warnings for proper handling. */
static void pg_warn (arg, message)
		 void *arg;
		 const char *message;
{
	D_imp_dbh( sv_2mortal(newRV((SV*)arg)) );
	
	if (DBIc_WARN(imp_dbh))
		warn(message);
}


/* ================================================================== */
/* Database specific error handling. */
void pg_error (h, error_num, error_msg)
		 SV *h;
		 int error_num;
		 char *error_msg;
{
	D_imp_xxh(h);
	char *err, *src, *dst; 
	STRLEN len = strlen(error_msg);
	imp_dbh_t	*imp_dbh = (imp_dbh_t *)(DBIc_TYPE(imp_xxh) == DBIt_ST ? DBIc_PARENT_COM(imp_xxh) : imp_xxh);
	
	New(0, err, len+1, char); /* freed below */
	if (!err)
		return;
	
	src = error_msg;
	dst = err;
	
	/* copy error message without trailing newlines */
	while (*src != '\0') {
		*dst++ = *src++;
	}
	*dst = '\0';
	
	sv_setiv(DBIc_ERR(imp_xxh), (IV)error_num);		 /* set err early */
	sv_setpv(DBIc_ERRSTR(imp_xxh), (char*)err);
	sv_setpvn(DBIc_STATE(imp_xxh), (char*)imp_dbh->sqlstate, 5);
	DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), DBIc_ERRSTR(imp_xxh));
	if (dbis->debug >= 3) {
		PerlIO_printf(DBILOGFP, "%s error %d recorded: %s\n",
									err, error_num, SvPV_nolen(DBIc_ERRSTR(imp_xxh)));
	}
	Safefree(err);

} /* end of pg_error */


/* ================================================================== */
void dbd_init (dbistate)
		 dbistate_t *dbistate;
{
	DBIS = dbistate;
}


/* ================================================================== */
int dbd_db_login (dbh, imp_dbh, dbname, uid, pwd)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char *dbname;
		 char *uid;
		 char *pwd;
{
	dTHR;
	
	char *conn_str, *dest, inquote = 0;
	STRLEN connect_string_size;
	
	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_db_login\n"); }
	
	/* DBD::Pg syntax: 'dbname=dbname;host=host;port=port' */
	/* libpq syntax: 'dbname=dbname host=host port=port user=uid password=pwd' */

	/* Figure out how large our connection string is going to be */
	connect_string_size = strlen(dbname);
	if (strlen(uid)) {
		connect_string_size += strlen(" user=''") + 2*strlen(uid);
	}
	if (strlen(pwd)) {
		connect_string_size += strlen(" password=''") + 2*strlen(pwd);
	}

	New(0, conn_str, connect_string_size+1, char); /* freed below */
	if (!conn_str)
		croak("No memory");
	
	/* Change all semi-colons in dbname to a space, unless quoted */
	dest = conn_str;
	while (*dbname) {
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
		while(*dest)
			dest++;
		while(*uid) {
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
		while(*dest)
			dest++;
		while(*pwd) {
			if ('\''==*pwd || '\\'==*pwd)
				*(dest++)='\\';
			*(dest++)=*(pwd++);
		}
		*dest = '\0';
		strcat(conn_str, "'");
	}

	if (dbis->debug >= 5)
		PerlIO_printf(DBILOGFP, "  dbdpg: login connection string: (%s)\n", conn_str);
	
	/* Make a connection to the database */

	imp_dbh->conn = PQconnectdb(conn_str);
	Safefree(conn_str);
	
	/* Check to see that the backend connection was successfully made */
	if (CONNECTION_OK != PQstatus(imp_dbh->conn)) {
		pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
		PQfinish(imp_dbh->conn);
		return 0;
	}
	
	/* Enable warnings to go through perl */
	PQsetNoticeProcessor(imp_dbh->conn, pg_warn, (void *)SvRV(dbh));
	
	/* Figure out what protocol this server is using */
	imp_dbh->pg_protocol = PGLIBVERSION >= 70400 ? PQprotocolVersion(imp_dbh->conn) : 0;

	/* Figure out this particular backend's version */
#if PGLIBVERSION >= 80000
	imp_dbh->pg_server_version = PQserverVersion(imp_dbh->conn);
#else
	imp_dbh->pg_server_version = -1;
	{
		PGresult *result;
		ExecStatusType status;
		int	cnt, vmaj, vmin, vrev;
	
		result = PQexec(imp_dbh->conn, "SELECT version(), 'DBD::Pg'");
		status = result ? PQresultStatus(result) : -1;
	
		if (PGRES_TUPLES_OK != status || !PQntuples(result)) {
			if (dbis->debug >= 4)
				PerlIO_printf(DBILOGFP, "  Could not get version from the server, status was %d\n", status);
		}
		else {
			cnt = sscanf(PQgetvalue(result,0,0), "PostgreSQL %d.%d.%d", &vmaj, &vmin, &vrev);
			PQclear(result);
			if (cnt >= 2) {
				if (cnt == 2)
					vrev = 0;
				imp_dbh->pg_server_version = (100 * vmaj + vmin) * 100 + vrev;
			}
		}
	}
#endif

	Renew(imp_dbh->sqlstate, 6, char); /* freed in dbd_db_destroy (and above) */
	if (!imp_dbh->sqlstate)
		croak("No memory");	
	imp_dbh->sqlstate[0] = '\0';
	strcpy(imp_dbh->sqlstate, "S1000");
	imp_dbh->done_begin = 0; /* We are not inside a transaction */
	imp_dbh->pg_bool_tf = 0;
	imp_dbh->pg_enable_utf8 = 0;
	imp_dbh->prepare_number = 1;
	imp_dbh->prepare_now = 0;
	imp_dbh->pg_errorlevel = 1; /* Matches PG default */
  imp_dbh->savepoints = newAV();
	imp_dbh->copystate = 0;

	/* If the server can handle it, we default to "smart", otherwise "off" */
	imp_dbh->server_prepare = imp_dbh->pg_protocol >= 3 ? 
	/* If using 3.0 protocol but not yet version 8, switch to "smart" */
		PGLIBVERSION >= 80000 ? 1 : 2 : 0;

	DBIc_IMPSET_on(imp_dbh); /* imp_dbh set up now */
	DBIc_ACTIVE_on(imp_dbh); /* call disconnect before freeing */

	return imp_dbh->pg_server_version;

} /* end of dbd_db_login */



/* ================================================================== */
int dbd_db_ping (dbh)
		 SV *dbh;
{
	D_imp_dbh(dbh);
	ExecStatusType status;

	/* Since this is a very explicit call, we do not rely on PQstatus,
		 which can have stale information */

	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_db_ping\n"); }

	if (NULL == imp_dbh->conn)
		return 0;

	status = _result(imp_dbh,"SELECT 'DBD::Pg ping test'");

	if (dbis->debug >= 8)
		PerlIO_printf(DBILOGFP, "  ping returned a value of %d\n", status);

	if (PGRES_TUPLES_OK != status)
		return 0;
		
	return 1;

} /* end of dbd_db_ping */


/* ================================================================== */
PGTransactionStatusType dbd_db_txn_status (imp_dbh)
		 imp_dbh_t *imp_dbh;
{

	/* Non - 7.3 *compiled* servers (our PG library) always return unknown */

	return PGLIBVERSION >= 70400 ? PQtransactionStatus(imp_dbh->conn) : PQTRANS_UNKNOWN;

} /* end of dbd_db_txn_status */


/* rollback and commit share so much code they get one function: */

/* ================================================================== */
int dbd_db_rollback_commit (dbh, imp_dbh, action)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char * action;
{

	PGTransactionStatusType tstatus;
	ExecStatusType status;

	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "%s\n", action); }
	
	/* no action if AutoCommit = on or the connection is invalid */
	if ((NULL == imp_dbh->conn) || (DBDPG_TRUE == DBIc_has(imp_dbh, DBIcf_AutoCommit)))
		return 0;

	/* We only perform these actions if we need to. For newer servers, we 
		 ask it for the status directly and double-check things */

#if PGLIBVERSION >= 70400
	tstatus = dbd_db_txn_status(imp_dbh);
	if (PQTRANS_IDLE == tstatus) { /* Not in a transaction */
		if (imp_dbh->done_begin) {
			/* We think we ARE in a transaction but we really are not */
			if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "Warning: invalid done_begin turned off\n"); }
			imp_dbh->done_begin = 0;
		}
	}
	else if (PQTRANS_UNKNOWN != tstatus) { /* In a transaction */
		if (!imp_dbh->done_begin) {
			/* We think we are NOT in a transaction but we really are */
			if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "Warning: invalid done_begin turned on\n"); }
			imp_dbh->done_begin = 1;
		}
	}
	else { /* Something is wrong: transaction status unknown */
		if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "Warning: cannot determine transaction status\n"); }
	}
#endif

	/* If begin_work has been called, turn AutoCommit back on and BeginWork off */
	if (DBIc_has(imp_dbh, DBIcf_BegunWork)) {
		DBIc_set(imp_dbh, DBIcf_AutoCommit, 1);
		DBIc_set(imp_dbh, DBIcf_BegunWork, 0);
	}

	if (!imp_dbh->done_begin)
		return 1;

	status = _result(imp_dbh, action);
		
	if (PGRES_COMMAND_OK != status) {
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		return 0;
	}

	av_clear(imp_dbh->savepoints);
	imp_dbh->done_begin = 0;
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
	dTHR;
	
	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_db_disconnect\n"); }

	/* We assume that disconnect will always work	
		 since most errors imply already disconnected. */

	DBIc_ACTIVE_off(imp_dbh);
	
	if (NULL != imp_dbh->conn) {
		/* Rollback if needed */
		if (dbd_db_rollback(dbh, imp_dbh) && dbis->debug >= 4)
			PerlIO_printf(DBILOGFP, "dbd_db_disconnect: AutoCommit=off -> rollback\n");
		
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
	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_db_destroy\n"); }

	av_undef(imp_dbh->savepoints);
	Safefree(imp_dbh->sqlstate);

	if (DBIc_ACTIVE(imp_dbh)) {
		dbd_db_disconnect(dbh, imp_dbh);
	}

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
	int oldval, newval = SvTRUE(valuesv);

	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_db_STORE\n"); }
	
	if (10==kl && strEQ(key, "AutoCommit")) {
		oldval = DBIc_has(imp_dbh, DBIcf_AutoCommit);
		if (oldval == newval)
			return 1;
		if (oldval) {
			/* Commit if necessary */
			if (dbd_db_commit(dbh, imp_dbh) && dbis->debug >= 5)
				PerlIO_printf(DBILOGFP, "dbd_db_STORE: AutoCommit on forced a commit\n");
		}
		DBIc_set(imp_dbh, DBIcf_AutoCommit, newval);
		return 1;
	}
	else if (10==kl && strEQ(key, "pg_bool_tf")) {
		imp_dbh->pg_bool_tf = newval ? 1 : 0;
	}
#ifdef is_utf8_string
	else if (14==kl && strEQ(key, "pg_enable_utf8")) {
		imp_dbh->pg_enable_utf8 = newval ? 1 : 0;
	}
#endif
	else if (13==kl && strEQ(key, "pg_errorlevel")) {
		/* Introduced in 7.4 servers */
		if (imp_dbh->pg_protocol >= 3) {
			newval = SvIV(valuesv);
			/* Default to "1" if an invalid value is passed in */
			imp_dbh->pg_errorlevel = 0==newval ? 0 : 2==newval ? 2 : 1;
			PQsetErrorVerbosity(imp_dbh->conn, imp_dbh->pg_errorlevel);
			if (dbis->debug >= 5)
				PerlIO_printf(DBILOGFP, "Reset error verbosity to %d\n", imp_dbh->pg_errorlevel);
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
			imp_dbh->prepare_now = newval ? 1 : 0;
		}
	}
	else {
		return 0;
	}

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
	
	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_db_FETCH\n"); }
	
	if (10==kl && strEQ(key, "AutoCommit")) {
		retsv = boolSV(DBIc_has(imp_dbh, DBIcf_AutoCommit));
	} else if (10==kl && strEQ(key, "pg_bool_tf")) {
		retsv = newSViv((IV)imp_dbh->pg_bool_tf);
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
	} else if (14==kl && strEQ(key, "pg_lib_version")) {
		retsv = newSViv((IV) PGLIBVERSION );
	} else if (17==kl && strEQ(key, "pg_server_version")) {
		retsv = newSViv((IV)imp_dbh->pg_server_version);
	}
	/* All the following are called too infrequently to bother caching */

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
	dTHR;
	
	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_discon_all\n"); }
	
	/* The disconnect_all concept is flawed and needs more work */
	if (!PL_dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
		sv_setiv(DBIc_ERR(imp_drh), (IV)1);
		sv_setpv(DBIc_ERRSTR(imp_drh),
						 (char*)"disconnect_all not implemented");
		DBIh_EVENT2(drh, ERROR_event,
								DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh));
	}
	return DBDPG_FALSE;

} /* end of dbd_discon_all */


/* ================================================================== */
int dbd_db_getfd (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{

	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_db_getfd\n"); }
	
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
	
	if (dbis->debug >= 3) { PerlIO_printf(DBILOGFP, "dbd_db_pg_notifies\n"); }
	
	status = PQconsumeInput(imp_dbh->conn);
	if (0 == status) { 
		pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
		return 0;
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
	
	return retsv;

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

	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_st_prepare: >%s<\n", statement); }

	/* Set default values for this statement handle */
	imp_sth->is_dml = 0; /* Not preparable DML until proved otherwise */
	imp_sth->prepared_by_us = 0; /* Set to 1 when actually done preparing */
	imp_sth->has_binary = 0; /* Are any of the params binary? */
	imp_sth->result	= NULL;
	imp_sth->cur_tuple = 0;
	imp_sth->placeholder_type = 0;
	imp_sth->rows = -1;
	imp_sth->totalsize = imp_sth->numsegs = imp_sth->numphs = imp_sth->numbound = 0;
	imp_sth->direct = 0;
	imp_sth->prepare_name = NULL;
	imp_sth->seg = NULL;
	imp_sth->ph = NULL;
	imp_sth->type_info = NULL;

	/* We inherit our prepare preferences from the database handle */
	imp_sth->server_prepare = imp_dbh->server_prepare;
	imp_sth->prepare_now = imp_dbh->prepare_now;

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
			imp_sth->direct = 0==SvIV(*svp) ? 0 : 1;
		if ((svp = hv_fetch((HV*)SvRV(attribs),"pg_prepare_now", 14, 0)) != NULL) {
			if (imp_dbh->pg_protocol >= 3) {
				imp_sth->prepare_now = 0==SvIV(*svp) ? 0 : 1;
			}
		}
	}

	/* Figure out the first word in the statement */
	while (*statement && isSPACE(*statement)) {
		mypos++;
		statement++;
	}
	if (!*statement || !isALPHA(*statement)) {
		imp_sth->firstword = NULL;
	}
	else {
		wordstart = mypos;
		while(*statement && isALPHA(*statement)) {
			mypos++;
			statement++;
		}
		newsize = mypos-wordstart;
		New(0, imp_sth->firstword, newsize+1, char); /* freed in dbd_st_destroy */
		if (!imp_sth->firstword)
			croak ("No memory");
		Copy(statement-newsize,imp_sth->firstword,newsize,char);
		imp_sth->firstword[newsize] = '\0';
		/* Try to prevent transaction commands unless "pg_direct" is set */
		if (!strcasecmp(imp_sth->firstword, "END") ||
				!strcasecmp(imp_sth->firstword, "BEGIN") ||
				!strcasecmp(imp_sth->firstword, "ABORT") ||
				!strcasecmp(imp_sth->firstword, "COMMIT") ||
				!strcasecmp(imp_sth->firstword, "ROLLBACK") ||
				!strcasecmp(imp_sth->firstword, "RELEASE") ||
				!strcasecmp(imp_sth->firstword, "SAVEPOINT")
				) {
			if (!imp_sth->direct)
				croak ("Please use DBI functions for transaction handling");
			imp_sth->is_dml = 1; /* Close enough for our purposes */
		}
		/* Note whether this is preparable DML */
		if (!strcasecmp(imp_sth->firstword, "SELECT") ||
				!strcasecmp(imp_sth->firstword, "INSERT") ||
				!strcasecmp(imp_sth->firstword, "UPDATE") ||
				!strcasecmp(imp_sth->firstword, "DELETE")
				) {
			imp_sth->is_dml = 1;
		}
	}
	statement -= mypos; /* Rewind statement */

	/* Break the statement into segments by placeholder */
	dbd_st_split_statement(sth, imp_sth, statement);

	/*
		We prepare it right away if:
		1. The statement is DML
		2. The attribute "direct" is false
		3. The backend can handle server-side prepares
		4. The attribute "pg_server_prepare" is not 0
		5. The attribute "pg_prepare_now" is true
    6. We are compiled on a 8 or greater server
	*/
	if (imp_sth->is_dml && 
			!imp_sth->direct &&
			imp_dbh->pg_protocol >= 3 &&
			0 != imp_sth->server_prepare &&
			imp_sth->prepare_now &&
			PGLIBVERSION >= 80000
			) {
		if (dbis->debug >= 5)
			PerlIO_printf(DBILOGFP, "  dbdpg: immediate prepare\n");

		if (dbd_st_prepare_statement(sth, imp_sth)) {
			croak (PQerrorMessage(imp_dbh->conn));
		}
	}

	DBIc_IMPSET_on(imp_sth);

	return imp_sth->numphs;

} /* end of dbd_st_prepare */


/* ================================================================== */
void dbd_st_split_statement (sth, imp_sth, statement)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 const char *statement;
{

	/* Builds the "segment" and "placeholder" structures for a statement handle */

	STRLEN mypos, sectionstart, sectionstop, newsize;
	unsigned int backslashes, topdollar, x;
	char ch, block, quote, placeholder_type, found;
	seg_t *newseg, *currseg = NULL;
	ph_t *newph, *thisph, *currph = NULL;

	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_st_split_statement\n"); }

	if (imp_sth->direct) { /* User has specifically asked that we not parse placeholders */
		imp_sth->numsegs = 1;
		imp_sth->numphs = 0;
		Renew(imp_sth->seg, 1, seg_t); /* freed in dbd_st_destroy (and above) */
		if (!imp_sth->seg)
			croak ("No memory");
		imp_sth->seg->nextseg = NULL;
		imp_sth->seg->placeholder = 0;
		imp_sth->seg->ph = NULL;
		imp_sth->totalsize = newsize = strlen(statement);
		if (newsize) {
			New(0, imp_sth->seg->segment, newsize+1, char); /* freed in dbd_st_destroy */
			if (!imp_sth->seg->segment)
				croak("No memory");
			Copy(statement, imp_sth->seg->segment, newsize, char);
			imp_sth->seg->segment[newsize] = '\0';
		}
		else {
			imp_sth->seg->segment = NULL;
		}
		while(*statement++) { }
		statement--;
	}

	sectionstart = 1;
	mypos = block = quote = backslashes = 0;
	while ((ch = *statement++)) {

		mypos++;

		/* Check for the end of a block */
		if (block) {
			if (
					/* dashdash and slashslash only terminate at newline */
					(('-' == block || '/' == block) && '\n' == ch) ||
					/* slashstar ends with a matching starslash */
					('*' == block && '*' == ch && '/' == *statement) ||
					/* end of array */
					(']' == ch)
					) {
				block = 0;
			}
			if (*statement)
				continue;
		}

		/* Check for the end of a quote */
		if (quote) {
			if (ch == quote) {
				if (!(backslashes & 1)) 
					quote = 0;
			}
			if (*statement)
				continue;
		}

		/* Check for the start of a quote */
		if ('\'' == ch || '"' == ch) {
			if (!(backslashes & 1))
				quote = ch;
			if (*statement)
				continue;
		}

		/* If a backslash, just count them to handle escaped quotes */
		if ('\\' == ch) {
			backslashes++;
			if (*statement)
				continue;
		}
		else {
			backslashes=0;
		}

		/* Check for the start of a 2 character block (e.g. comments) */
		if (('-' == ch && '-' == *statement) ||
				('/' == ch && '/' == *statement) ||
				('/' == ch && '*' == *statement)) {
			block = *statement;
			if (*statement)
				continue;
		}

		/* Check for the start of an array */
		if ('[' == ch) {
			block = ']'; 
			if (*statement)
				continue;
		}

		/* All we care about at this point is placeholder characters and end of string */
		if ('?' != ch && '$' != ch && ':' != ch && *statement)
			continue;

		placeholder_type = 0;
		sectionstop=mypos-1;
	
		/* Normal question mark style */
		if ('?' == ch) {
			placeholder_type = 1;
		}
		/* Dollar sign placeholder style */
		else if ('$' == ch && isDIGIT(*statement)) {
			if ('0' == *statement)
				croak("Invalid placeholder value");
			while(isDIGIT(*statement)) {
				++statement;
				++mypos;
			}
			placeholder_type = 2;
		}
		/* Colon style */
		else if (':' == ch) {
			/* Skip multiple colons (casting, e.g. "myval::float") */
			if (':' == *statement) {
				while(':' == *statement) {
					++statement;
					++mypos;
				}
				continue;
			}
			if (isALNUM(*statement)) {
				while(isALNUM(*statement)) {
					++statement;
					++mypos;
				}
				placeholder_type = 3;
			}
		}

		/* Check for conflicting placeholder types */
		if (placeholder_type) {
			if (imp_sth->placeholder_type && placeholder_type != imp_sth->placeholder_type)
				croak("Cannot mix placeholder styles \"%s\" and \"%s\"",
							1==imp_sth->placeholder_type ? "?" : 2==imp_sth->placeholder_type ? "$1" : ":foo",
							1==placeholder_type ? "?" : 2==placeholder_type ? "$1" : ":foo");
		}
		
		if (!placeholder_type && *statement)
			continue;

		/* If we got here, we have a segment that needs to be saved */
		New(0, newseg, 1, seg_t);  /* freed in dbd_st_destroy */
		if (!newseg)
			croak ("No memory");
		newseg->nextseg = NULL;
		newseg->placeholder = 0;
		newseg->ph = NULL;

		if (1==placeholder_type) {
			newseg->placeholder = ++imp_sth->numphs;
		}
		else if (2==placeholder_type) {
			newseg->placeholder = atoi(statement-(mypos-sectionstop-1));
		}
		else if (3==placeholder_type) {
			newsize = mypos-sectionstop;
			/* Have we seen this placeholder yet? */
			for (x=1,thisph=imp_sth->ph; NULL != thisph; thisph=thisph->nextph,x++) {
				if (!strncmp(thisph->fooname, statement-newsize, newsize)) {
					newseg->placeholder = x;
					newseg->ph = thisph;
					break;
				}
			}
			if (!newseg->placeholder) {
				imp_sth->numphs++;
				newseg->placeholder = imp_sth->numphs;
				New(0, newph, 1, ph_t); /* freed in dbd_st_destroy */
				newseg->ph = newph;
				if (!newph)
					croak("No memory");
				newph->nextph = NULL;
				newph->bind_type = NULL;
				newph->value = NULL;
				newph->quoted = NULL;
				newph->referenced = 0;
				newph->defaultval = 1;
				New(0, newph->fooname, newsize+1, char); /* freed in dbd_st_destroy */
				if (!newph->fooname)
					croak("No memory");
				Copy(statement-newsize, newph->fooname, newsize, char);
				newph->fooname[newsize] = '\0';
				if (NULL==currph) {
					imp_sth->ph = newph;
				}
				else {
					currph->nextph = newph;
				}
				currph = newph;
			}
		} /* end if placeholder_type */
		
		newsize = sectionstop-sectionstart+1;
		if (! placeholder_type)
			newsize++;
		if (newsize) {
			New(0, newseg->segment, newsize+1, char); /* freed in dbd_st_destroy */
			if (!newseg->segment)
				croak("No memory");
			Copy(statement-(mypos-sectionstart+1), newseg->segment, newsize, char);
			newseg->segment[newsize] = '\0';
			imp_sth->totalsize += newsize;
		}
		else {
			newseg->segment = NULL;
		}
		if (dbis->debug >= 5)
			PerlIO_printf(DBILOGFP, "  dbdpg segment: \"%s\"\n", newseg->segment);
		
		/* Tie it in to the previous one */
		if (NULL==currseg) {
			imp_sth->seg = newseg;
		}
		else {
			currseg->nextseg = newseg;
		}
		currseg = newseg;
		sectionstart = mypos+1;
		imp_sth->numsegs++;

		/* Bail unless it we have a placeholder ready to go */
		if (!placeholder_type)
			continue;

		imp_sth->placeholder_type = placeholder_type;

	} /* end statement parsing */

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

		for (x=1; x<=topdollar; x++) {
			for (found=0, currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				if (currseg->placeholder==x) {
					found=1;
					break;
				}
			}
			if (!found)
				croak("Invalid placeholders: must start at $1 and increment one at a time");
		}
		if (dbis->debug >= 5)
			PerlIO_printf(DBILOGFP, " dbdpg: set number of placeholders to %d\n", topdollar);
		imp_sth->numphs = topdollar;
	}

	/* Create sequential placeholders */
	if (3 != imp_sth->placeholder_type) {
		currseg = imp_sth->seg;
		for (x=1; x <= imp_sth->numphs; x++) {
			New(0, newph, 1, ph_t); /* freed in dbd_st_destroy */
			if (!newph)
				croak("No memory");
			newph->nextph = NULL;
			newph->bind_type = NULL;
			newph->value = NULL;
			newph->quoted = NULL;
			newph->referenced = 0;
			newph->defaultval = 1;
			newph->fooname = NULL;
			/* Let the correct segment point to it */
			while (!currseg->placeholder)
				currseg = currseg->nextseg;
			currseg->ph = newph;
			currseg = currseg->nextseg;
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
		PerlIO_printf(DBILOGFP, "  dbdpg placeholder type: %d numsegs: %d  numphs: %d\n",
									imp_sth->placeholder_type, imp_sth->numsegs, imp_sth->numphs);
		PerlIO_printf(DBILOGFP, "  Placeholder numbers, ph id, and segments:\n");
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			PerlIO_printf(DBILOGFP, "    PH: (%d) ID: (%d) SEG: (%s)\n", currseg->placeholder, NULL==currseg->ph ? 0 : currseg->ph, currseg->segment);
		}
		PerlIO_printf(DBILOGFP, "  Placeholder number, fooname, id:\n");
		for (x=1,currph=imp_sth->ph; NULL != currph; currph=currph->nextph,x++) {
			PerlIO_printf(DBILOGFP, "    #%d FOONAME: (%s) ID: (%d)\n", x, currph->fooname, currph);
		}
	}

	DBIc_NUM_PARAMS(imp_sth) = imp_sth->numphs;

} /* end dbd_st_split_statement */



/* ================================================================== */
int dbd_st_prepare_statement (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{

	D_imp_dbh_from_sth;
	char *statement;
	unsigned int x;
	STRLEN execsize;
	PGresult *result;
	ExecStatusType status;
	seg_t *currseg;
	bool oldprepare = 1;
	unsigned int params = 0;
	Oid *paramTypes = NULL;
	ph_t *currph;

#if PGLIBVERSION >= 80000
	oldprepare = 0;
#endif

	Renew(imp_sth->prepare_name, 25, char); /* freed in dbd_st_destroy (and above) */
	if (!imp_sth->prepare_name)
		croak("No memory");

	/* Name is simply "dbdpg_#" */
	sprintf(imp_sth->prepare_name,"dbdpg_%d", imp_dbh->prepare_number);
	imp_sth->prepare_name[strlen(imp_sth->prepare_name)]='\0';

	if (dbis->debug >= 5)
		PerlIO_printf(DBILOGFP, "  dbdpg: new statement name \"%s\", oldprepare is %d\n",
									imp_sth->prepare_name, oldprepare);

	/* PQprepare was not added until 8.0 */

	execsize = imp_sth->totalsize;
	if (oldprepare)
		execsize += strlen("PREPARE  AS ") + strlen(imp_sth->prepare_name);

	if (imp_sth->numphs) {
		if (oldprepare) {
			execsize += strlen("()");
			execsize += imp_sth->numphs-1; /* for the commas */
		}
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (!currseg->placeholder)
				continue;
			/* The parameter itself: dollar sign plus digit(s) */
			for (x=1; x<7; x++) {
				if (currseg->placeholder < pow(10,x))
					break;
			}
			if (x>=7)
				croak("Too many placeholders!");
			execsize += x+1;
			if (oldprepare) {
				/* The parameter type, only once per number please */
				if (0==currseg->ph->referenced)
					execsize += strlen(currseg->ph->bind_type->type_name);
				currseg->ph->referenced = 1;
			}
		}
	}

	New(0, statement, execsize+1, char); /* freed below */
	if (!statement)
		croak("No memory");

	if (oldprepare) {
		sprintf(statement, "PREPARE %s", imp_sth->prepare_name);
		if (imp_sth->numphs) {
			strcat(statement, "(");
			for (x=0, currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				if (currseg->placeholder && 1==currseg->ph->referenced) {
					if (x)
						strcat(statement, ",");
					strcat(statement, currseg->ph->bind_type->type_name);
					x=1;
					currseg->ph->referenced = 0;
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
			sprintf(statement, "%s$%d", statement, currseg->placeholder);
		}
	}

	statement[execsize] = '\0';

	if (dbis->debug >= 6)
		PerlIO_printf(DBILOGFP, "  prepared statement: >%s<\n", statement);

	if (oldprepare) {
		status = _result(imp_dbh, statement);
	}
	else {
		if (imp_sth->numbound) {
			params = imp_sth->numphs;
			paramTypes = calloc(imp_sth->numphs, sizeof(*paramTypes));
			for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph) {
				paramTypes[x++] = currph->defaultval ? 0 : currph->bind_type->type_id;
			}
		}
		result = PQprepare(imp_dbh->conn, imp_sth->prepare_name, statement, params, paramTypes);
		Safefree(paramTypes);
		status = result ? PQresultStatus(result) : -1;
		if (dbis->debug >= 6)
			PerlIO_printf(DBILOGFP, "  dbdpg: Using PQprepare\n");
	}
	Safefree(statement);
	if (PGRES_COMMAND_OK != status) {
		pg_error(sth,status,PQerrorMessage(imp_dbh->conn));
		return -2;
	}

	imp_sth->prepared_by_us = 1; /* Done here so deallocate is not called spuriously */
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
	unsigned int x, phnum;
	SV **svp;
	bool reprepare = 0;
	int pg_type = 0;
	char *value_string;

	if (dbis->debug >= 4) {
		PerlIO_printf(DBILOGFP, "dbd_bind_ph\n");
		PerlIO_printf(DBILOGFP, " bind params: ph_name: %s newvalue: %s(%d)\n", 
									neatsvpv(ph_name,0), neatsvpv(newvalue,0), SvOK(newvalue));
	}

	if (is_inout)
		croak("bind_inout not supported by this driver");

	if (!imp_sth->numphs) {
		croak("Statement has no placeholders to bind");
	}

	/* Check the placeholder name and transform to a standard form */
	if (SvGMAGICAL(ph_name)) {
		mg_get(ph_name);
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
			if (!strcmp(currph->fooname, name)) {
				x=1;
				break;
			}
		}
		if (!x)
			croak("Cannot bind unknown placeholder '%s'", name);
	}
	else { /* We have a number */	
		if ('$' == *name)
			*name++;
		phnum = atoi(name);
		if (phnum<1 || phnum>imp_sth->numphs)
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
	if ((SvROK(newvalue) &&!IS_DBI_HANDLE(newvalue) &&!SvAMAGIC(newvalue))) {
		/* dbi handle allowed for cursor variables */
		croak("Cannot bind a reference (%s) (%s) (%d) type=%d %d %d %d", neatsvpv(newvalue,0), SvAMAGIC(newvalue),
					SvTYPE(SvRV(newvalue)) == SVt_PVAV ? 1 : 0, SvTYPE(newvalue), SVt_PVAV, SVt_PV, 0);
	}
	if (dbis->debug >= 5) {
		PerlIO_printf(DBILOGFP, "		 bind %s <== %s (type %ld", name, neatsvpv(newvalue,0), (long)sql_type);
		if (attribs) {
			PerlIO_printf(DBILOGFP, ", attribs: %s", neatsvpv(attribs,0));
		}
		PerlIO_printf(DBILOGFP, ")\n");
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
		if (!(currph->bind_type = sql_type_data(sql_type))) {
			croak("Cannot bind %s unknown sql_type %" IVdf, name, sql_type);
		}
		if (!(currph->bind_type = pg_type_data(currph->bind_type->type.pg))) {
			croak("Cannot find a pg_type for %" IVdf, sql_type);
		}
 	}
	else if (NULL == currph->bind_type) { /* "sticky" data type */
		/* This is the default type, but we will honor defaultval if we can */
		currph->bind_type = pg_type_data(VARCHAROID);
		if (!currph->bind_type)
			croak("Default type is bad!!!!???");
	}

	if (pg_type || sql_type) {
		currph->defaultval = 0;
		/* Possible re-prepare, depending on whether the type name also changes */
		if (imp_sth->prepared_by_us && NULL != imp_sth->prepare_name)
			reprepare=1;
		/* Mark this statement as having binary if the type is bytea */
		if (BYTEAOID==currph->bind_type->type_id)
			imp_sth->has_binary = 1;
	}

	/* convert to a string ASAP */
	if (!SvPOK(newvalue) && SvOK(newvalue)) {
		sv_2pv(newvalue, &na);
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
		currph->value = NULL;
		currph->valuelen = 0;
	}

	if (reprepare) {
		if (dbis->debug >= 5)
			PerlIO_printf(DBILOGFP, "  dbdpg: binding has forced a re-prepare\n");
		/* Deallocate sets the prepare_name to NULL */
		if (dbd_st_deallocate_statement(sth, imp_sth)) {
			/* Deallocation failed. Let's mark it and move on */
			imp_sth->prepare_name = NULL;
			if (dbis->debug >= 4)
				PerlIO_printf(DBILOGFP, "  dbdpg: failed to deallocate!\n");
		}
	}

	if (dbis->debug >= 10)
		PerlIO_printf(DBILOGFP, "  dbdpg: placeholder \"%s\" bound as type \"%s\"(%d), length %d, value of \"%s\"\n",
									name, currph->bind_type->type_name, currph->bind_type->type_id, currph->valuelen,
									BYTEAOID==currph->bind_type->type_id ? "(binary, not shown)" : value_string);

	return 1;

} /* end of dbd_bind_ph */


/* ================================================================== */
int dbd_st_execute (sth, imp_sth) /* <= -2:error, >=0:ok row count, (-1=unknown count) */
		 SV *sth;
		 imp_sth_t *imp_sth;
{

	D_imp_dbh_from_sth;
	ph_t *currph;
	ExecStatusType status = -1;
	STRLEN execsize, x;
	const char **paramValues;
	int *paramLengths = NULL, *paramFormats = NULL;
	Oid *paramTypes = NULL;
	seg_t *currseg;
	char *statement, *cmdStatus, *cmdTuples;
	int num_fields, ret = -2;
	
	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_st_execute\n"); }
	
	if (NULL == imp_dbh->conn)
		croak("execute on disconnected handle");

	/* Abort if we are in the middle of a copy */
	if (imp_dbh->copystate)
		croak("Must call pg_endcopy before issuing more commands");

	/* Ensure that all the placeholders have been bound */
	if (imp_sth->numphs) {
		for (currph=imp_sth->ph; NULL != currph; currph=currph->nextph) {
			if (NULL == currph->bind_type) {
				pg_error(sth, -1, "execute called with an unbound placeholder");
				return -2;
			}
		}
	}


	/* If not autocommit, start a new transaction */
	if (!imp_dbh->done_begin && DBDPG_FALSE == DBIc_has(imp_dbh, DBIcf_AutoCommit)) {
		status = _result(imp_dbh, "begin");
		if (PGRES_COMMAND_OK != status) {
			pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
			return -2;
		}
		imp_dbh->done_begin = 1;
	}

	/* clear old result (if any) */
	if (imp_sth->result)
		PQclear(imp_sth->result);

	/*
		Now, we need to build the statement to send to the backend
		 We are using one of PQexec, PQexecPrepared, or PQexecParams
		 First, we figure out the size of the statement...
	*/

	execsize = imp_sth->totalsize; /* Total of all segments */

	/* If using plain old PQexec, we need to quote each value ourselves */
	if (imp_dbh->pg_protocol < 3 || 
			(1 != imp_sth->server_prepare && 
			 imp_sth->numbound != imp_sth->numphs)) {
		for (currph=imp_sth->ph; NULL != currph; currph=currph->nextph) {
			if (NULL == currph->value) {
				Renew(currph->quoted, 5, char); /* freed in dbd_st_execute (and above) */
				if (!currph->quoted)
					croak("No memory");
				currph->quoted[0] = '\0';
				strcpy(currph->quoted, "NULL");
				currph->quotedlen = 4;
			}
			else {
				currph->quoted = currph->bind_type->quote(currph->value, currph->valuelen, &currph->quotedlen);
			}
		}
		/* Set the size of each actual in-place placeholder */
		for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
			if (currseg->placeholder)
				execsize += currseg->ph->quotedlen;
		}
	}
	else { /* We are using a server that can handle PQexecParams/PQexecPrepared */
		/* Put all values into an array to pass to PQexecPrepared */
		paramValues = calloc(imp_sth->numphs, sizeof(*paramValues));
		for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph) {
			paramValues[x++] = currph->value;
		}

		/* Binary or regular? */

		if (imp_sth->has_binary) {
			paramLengths = calloc(imp_sth->numphs, sizeof(*paramLengths));
			paramFormats = calloc(imp_sth->numphs, sizeof(*paramFormats));
			for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph,x++) {
				if (BYTEAOID==currph->bind_type->type_id) {
					paramLengths[x] = currph->valuelen;
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
		6a. The attribute "pg_server_prepare" is 1
		OR
		6b. All placeholders are bound (and "pg_server_prepare" is 2)
	*/
	if (dbis->debug >= 6) {
		PerlIO_printf(DBILOGFP, "  dbdpg: PQexec* choice: dml=%d, direct=%d, protocol=%d, server_prepare=%d numbound=%d, numphs=%d\n", imp_sth->is_dml, imp_sth->direct, imp_dbh->pg_protocol, imp_sth->server_prepare, imp_sth->numbound, imp_sth->numphs);
	}
	if (imp_sth->is_dml && 
			!imp_sth->direct &&
			imp_dbh->pg_protocol >= 3 &&
			0 != imp_sth->server_prepare &&
			1 <= imp_sth->numphs &&
			(1 == imp_sth->server_prepare ||
			 (imp_sth->numbound == imp_sth->numphs)
			 )){
	
		if (dbis->debug >= 5)
			PerlIO_printf(DBILOGFP, "  dbdpg: using PQexecPrepare\n");

		/* Prepare if it has not already been prepared (or it needs repreparing) */
		if (NULL == imp_sth->prepare_name) {
			if (imp_sth->prepared_by_us) {
				if (dbis->debug >= 5)
					PerlIO_printf(DBILOGFP, "  dbdpg: re-preparing statement\n");
			}
			if (dbd_st_prepare_statement(sth, imp_sth)) {
				return -2;
			}
		}
		else {
			if (dbis->debug >= 5)
				PerlIO_printf(DBILOGFP, "  dbdpg: using previously prepared statement \"%s\"\n", imp_sth->prepare_name);
		}
		
		if (dbis->debug >= 10) {
			for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph,x++) {
				PerlIO_printf(DBILOGFP, "  PQexecPrepared item #%d\n", x);
				PerlIO_printf(DBILOGFP, "   Value: (%s)\n", paramValues[x]);
				PerlIO_printf(DBILOGFP, "   Length: (%d)\n", paramLengths ? paramLengths[x] : 0);
				PerlIO_printf(DBILOGFP, "   Format: (%d)\n", paramFormats ? paramFormats[x] : 0);
			}
		}
		
		if (dbis->debug >= 5)
			PerlIO_printf(DBILOGFP, "  dbdpg: calling PQexecPrepared for %s\n", imp_sth->prepare_name);
		imp_sth->result = PQexecPrepared(imp_dbh->conn, imp_sth->prepare_name, imp_sth->numphs,
																		 paramValues, paramLengths, paramFormats, 0);

		Safefree(paramValues);
		Safefree(paramLengths);
		Safefree(paramFormats);			

	} /* end new-style prepare */
	else {
		
		/* prepare via PQexec or PQexecParams */


		/* PQexecParams */

		if (imp_dbh->pg_protocol >= 3 &&
				imp_sth->numphs &&
				(1 == imp_sth->server_prepare || 
				 imp_sth->numbound == imp_sth->numphs)) {

			if (dbis->debug >= 5)
				PerlIO_printf(DBILOGFP, "  dbdpg: using PQexecParams\n");

			/* Figure out how big the statement plus placeholders will be */
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				if (!currseg->placeholder)
					continue;
				/* The parameter itself: dollar sign plus digit(s) */
				for (x=1; x<7; x++) {
					if (currseg->placeholder < pow(10,x))
						break;
				}
				if (x>=7)
					croak("Too many placeholders!");
				execsize += x+1;
			}

			/* Create the statement */
			New(0, statement, execsize+1, char); /* freed below */
			if (!statement)
				croak("No memory");
			statement[0] = '\0';
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				strcat(statement, currseg->segment);
				if (currseg->placeholder)
					sprintf(statement, "%s$%d", statement, currseg->placeholder);
			}
			statement[execsize] = '\0';
			
			/* Populate paramTypes */
			paramTypes = calloc(imp_sth->numphs, sizeof(*paramTypes));
			for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph) {
				paramTypes[x++] = currph->defaultval ? 0 : currph->bind_type->type_id;
			}
		
			if (dbis->debug >= 10) {
				for (x=0,currph=imp_sth->ph; NULL != currph; currph=currph->nextph,x++) {
					PerlIO_printf(DBILOGFP, "  PQexecParams item #%d\n", x);
					PerlIO_printf(DBILOGFP, "   Type: (%d)\n", paramTypes[x]);
					PerlIO_printf(DBILOGFP, "   Value: (%s)\n", paramValues[x]);
					PerlIO_printf(DBILOGFP, "   Length: (%d)\n", paramLengths ? paramLengths[x] : 0);
					PerlIO_printf(DBILOGFP, "   Format: (%d)\n", paramFormats ? paramFormats[x] : 0);
				}
			}

			if (dbis->debug >= 5)
				PerlIO_printf(DBILOGFP, "  dbdpg: calling PQexecParams for: %s\n", statement);
			imp_sth->result = PQexecParams(imp_dbh->conn, statement, imp_sth->numphs, paramTypes,
																		 paramValues, paramLengths, paramFormats, 0);
			Safefree(paramTypes);
			Safefree(paramValues);
			Safefree(paramLengths);
			Safefree(paramFormats);
			Safefree(statement);
		}
		
		/* PQexec */

		else {

			if (dbis->debug >= 5)
				PerlIO_printf(DBILOGFP, "  dbdpg: using PQexec\n");

			/* Go through and quote each value, then turn into a giant statement */
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				if (currseg->placeholder)
					execsize += currseg->ph->quotedlen;
			}
			New(0, statement, execsize+1, char); /* freed below */
			if (!statement)
				croak("No memory");
			statement[0] = '\0';
			for (currseg=imp_sth->seg; NULL != currseg; currseg=currseg->nextseg) {
				strcat(statement, currseg->segment);
				if (currseg->placeholder)
					strcat(statement, currseg->ph->quoted);
			}
			statement[execsize] = '\0';

			if (dbis->debug >= 5)
				PerlIO_printf(DBILOGFP, "  dbdpg: calling PQexec for: %s\n", statement);
			
			imp_sth->result = PQexec(imp_dbh->conn, statement);
			Safefree(statement);

		} /* end PQexec */

	} /* end non-prepared exec */

	/* Some form of PQexec has been run at this point */

	status = imp_sth->result ? PQresultStatus(imp_sth->result) : -1;

	/* We don't want the result cleared yet, so we don't use _result */

#if PGLIBVERSION >= 70400
	strncpy(imp_dbh->sqlstate,
					NULL == PQresultErrorField(imp_sth->result,PG_DIAG_SQLSTATE) ? "00000" : 
					PQresultErrorField(imp_sth->result,PG_DIAG_SQLSTATE),
					5);
	imp_dbh->sqlstate[5] = '\0';
#else
	strcpy(imp_dbh->sqlstate, "S1000"); /* DBI standard says this is the default */
#endif

	cmdStatus = imp_sth->result ? (char *)PQcmdStatus(imp_sth->result) : "";
	cmdTuples = imp_sth->result ? (char *)PQcmdTuples(imp_sth->result) : "";
	
	if (dbis->debug >= 5)
		PerlIO_printf(DBILOGFP, "  dbdpg: received a status of %d\n", status);

	imp_dbh->copystate = 0; /* Assume not in copy mode until told otherwise */
	if (PGRES_TUPLES_OK == status) {
		num_fields = PQnfields(imp_sth->result);
		imp_sth->cur_tuple = 0;
		DBIc_NUM_FIELDS(imp_sth) = num_fields;
		DBIc_ACTIVE_on(imp_sth);
		ret = PQntuples(imp_sth->result);
		if (dbis->debug >= 5)
			PerlIO_printf(DBILOGFP, "  dbdpg: status was PGRES_TUPLES_OK, fields=%d, tuples=%d\n",
										num_fields, ret);
	}
	else if (PGRES_COMMAND_OK == status) {
		/* non-select statement */
		if (dbis->debug >= 5)
			PerlIO_printf(DBILOGFP, "  dbdpg: status was PGRES_COMMAND_OK\n");
		if (! strncmp(cmdStatus, "DELETE", 6) || ! strncmp(cmdStatus, "INSERT", 6) || ! strncmp(cmdStatus, "UPDATE", 6)) {
			ret = atoi(cmdTuples);
		} else {
			/* We assume that no rows are affected for successful commands (e.g. ALTER TABLE) */
			return 0;
		}
	}
	else if (PGRES_COPY_OUT == status || PGRES_COPY_IN == status) {
		/* Copy Out/In data transfer in progress */
		imp_dbh->copystate = status;
		return -1;
	}
	else {
		pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
		return -2;
	}
	
	/* store the number of affected rows */
	
	imp_sth->rows = ret;
	
	return ret;

} /* end of dbd_st_execute */


/* ================================================================== */
is_high_bit_set(val)
		 char *val;
{
	while (*val++)
		if (*val & 0x80) return 1;
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
	int i, pg_type, chopblanks;
	STRLEN value_len;
	STRLEN len;
	AV *av;
	D_imp_dbh_from_sth;
	
	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_st_fetch\n"); }

	/* Check that execute() was executed successfully */
	if ( !DBIc_ACTIVE(imp_sth) ) {
		pg_error(sth, 1, "no statement executing\n");	
		return Nullav;
	}
	
	if (imp_sth->cur_tuple == PQntuples(imp_sth->result) ) {
		if (dbis->debug >= 5)
			PerlIO_printf(DBILOGFP, "  dbdpg: fetched the last tuple (%d)\n", imp_sth->cur_tuple);
		imp_sth->cur_tuple = 0;
		DBIc_ACTIVE_off(imp_sth);
		return Nullav; /* we reached the last tuple */
	}
	
	av = DBIS->get_fbav(imp_sth);
	num_fields = AvFILL(av)+1;
	
	chopblanks = DBIc_has(imp_sth, DBIcf_ChopBlanks);

	/* Set up the type_info array if we have not seen it yet */
	if (NULL==imp_sth->type_info) {
		imp_sth->type_info = calloc(num_fields, sizeof(*imp_sth->type_info)); /* freed in dbd_st_destroy */
		for (i = 0; i < num_fields; ++i) {
			imp_sth->type_info[i] = pg_type_data(PQftype(imp_sth->result, i));
		}
	}
	
	for (i = 0; i < num_fields; ++i) {
		SV *sv;

		if (dbis->debug >= 5)
			PerlIO_printf(DBILOGFP, "  dbdpg: fetching a field\n");

		sv = AvARRAY(av)[i];
		if (PQgetisnull(imp_sth->result, imp_sth->cur_tuple, i)) {
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
				switch(type_info->type_id) {
				case CHAROID:
				case TEXTOID:
				case BPCHAROID:
				case VARCHAROID:
					if (is_high_bit_set(value) && is_utf8_string(value, value_len)) {
						SvUTF8_on(sv);
					}
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
	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_st_rows\n"); }
	
	return imp_sth->rows;

} /* end of dbd_st_rows */


/* ================================================================== */
int dbd_st_finish (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	dTHR;
	
	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_st_finish\n"); }
	
	if (DBIc_ACTIVE(imp_sth) && imp_sth->result) {
		PQclear(imp_sth->result);
		imp_sth->result = 0;
		imp_sth->rows = 0;
	}
	
	DBIc_ACTIVE_off(imp_sth);
	return 1;

} /* end of sbs_st_finish */


/* ================================================================== */
int dbd_st_deallocate_statement (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	char *stmt;
	PGresult *result;
	ExecStatusType status;
	PGTransactionStatusType tstatus;
	D_imp_dbh_from_sth;
	
	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_st_deallocate_statement\n"); }

	if (NULL == imp_dbh->conn || NULL == imp_sth->prepare_name)
		return 0;
	
	/* What is our status? */
	tstatus = dbd_db_txn_status(imp_dbh);
	if (dbis->debug >= 5)
		PerlIO_printf(DBILOGFP, "  dbdpg: transaction status is %d\n", tstatus);

	/* If we are in a failed transaction, rollback before deallocating */
	if (PQTRANS_INERROR == tstatus) {
		if (dbis->debug >= 4)
			PerlIO_printf(DBILOGFP, "  dbdpg: Issuing rollback before deallocate\n", tstatus);
		{
			/* If a savepoint has been set, rollback to the last savepoint instead of the entire transaction */
			I32	alen = av_len(imp_dbh->savepoints);
			if (alen > -1) {
		if (dbis->debug >= 4)
			PerlIO_printf(DBILOGFP, "  dbdpg: Issuing rollback before deallocate2\n", tstatus);
				SV		*sp = av_pop(imp_dbh->savepoints);
				char	cmd[SvLEN(sp) + 13];
				sprintf(cmd,"rollback to %s",SvPV_nolen(sp));
				status = _result(imp_dbh, cmd);
			}
			else {
				status = _result(imp_dbh, "ROLLBACK");
				imp_dbh->done_begin = 0;
			}
		}
		if (PGRES_COMMAND_OK != status) {
			/* This is not fatal, it just means we cannot deallocate */
			if (dbis->debug >= 4)
				PerlIO_printf(DBILOGFP, "  dbdpg: Rollback failed, so no deallocate\n");
			return 1;
		}
	}

	New(0, stmt, strlen("DEALLOCATE ") + strlen(imp_sth->prepare_name) + 1, char); /* freed below */
	if (!stmt)
		croak("No memory");

	sprintf(stmt, "DEALLOCATE %s", imp_sth->prepare_name);

	if (dbis->debug >= 5)
		PerlIO_printf(DBILOGFP, "  dbdpg: deallocating \"%s\"\n", imp_sth->prepare_name);

	status = _result(imp_dbh, stmt);
	Safefree(stmt);
	if (PGRES_COMMAND_OK != status) {
		pg_error(sth,status, PQerrorMessage(imp_dbh->conn));
		return 2;
	}

	imp_sth->prepare_name = NULL;

	return 0;

} /* end of dbd_st_deallocate_statement */


/* ================================================================== */
void dbd_st_destroy (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{

	seg_t *currseg, *nextseg;
	ph_t *currph, *nextph;

	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_st_destroy\n"); }

	if (NULL == imp_sth->seg) /* Already been destroyed! */
		croak("dbd_st_destroy called twice!");

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

	/* Free all the placeholders */
	currph = imp_sth->ph;
	while (NULL != currph) {
		Safefree(currph->fooname);
		Safefree(currph->value);
		Safefree(currph->quoted);
		nextph = currph->nextph;
		Safefree(currph);
		currph = nextph;
	}

	/* Deallocate only if we named this statement ourselves */
	if (imp_sth->prepared_by_us) {
		if (dbd_st_deallocate_statement(sth, imp_sth)) {
			if (dbis->debug >= 4)
				PerlIO_printf(DBILOGFP, "  dbdpg: could not deallocate\n");
		}
	}	
	Safefree(imp_sth->prepare_name);

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

	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_st_STORE\n"); }
	
	if (17==kl && strEQ(key, "pg_server_prepare")) {
		imp_sth->server_prepare = strEQ(value,"0") ? 0 : 1;
	}
	else if (14==kl && strEQ(key, "pg_prepare_now")) {
		imp_sth->prepare_now = strEQ(value,"0") ? 0 : 1;
	}
	else if (15==kl && strEQ(key, "pg_prepare_name")) {
		Safefree(imp_sth->prepare_name);
		New(0, imp_sth->prepare_name, vl+1, char); /* freed in dbd_st_destroy (and above) */
		if (!imp_sth->prepare_name)
			croak("No memory");
		Copy(value, imp_sth->prepare_name, vl, char);
		imp_sth->prepare_name[vl] = '\0';
	}
	else {
		return 0;
	}

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
	char *type_name;
	sql_type_info_t *type_info;

	if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_st_FETCH\n"); }
	
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
				hv_store_ent(pvhv, 3==imp_sth->placeholder_type ? newSVpv(currph->fooname,0) : 
										 newSViv(i+1), Nullsv, i);
			}
			else {
				hv_store_ent(pvhv, 3==imp_sth->placeholder_type ? newSVpv(currph->fooname,0) : 
										 newSViv(i+1), newSVpv(currph->value,0),i);
			}
		}
		retsv = newRV_noinc((SV*)pvhv);
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
			av_store(av, i, newSVpv(PQfname(imp_sth->result, i),0));
		}
	}
	else if (4==kl && strEQ(key, "TYPE")) {
		/* Need to convert the Pg type to ANSI/SQL type. */
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			type_info = pg_type_data(PQftype(imp_sth->result, i));
			av_store(av, i, newSViv( type_info ? type_info->type.sql : 0 ) );
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
				sz = (PQfmod(imp_sth->result, i)-4) >> 16;
				break;
			default:
				sz = PQfsize(imp_sth->result, i);
				break;
			}
			av_store(av, i, sz > 0 ? newSViv(sz) : &sv_undef);
		}
	}
	else if (5==kl && strEQ(key, "SCALE")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			x = PQftype(imp_sth->result, i);
			if (NUMERICOID==x) {
				x = PQfmod(imp_sth->result, i)-4;
				av_store(av, i, newSViv(x % (x>>16)));
			}
			else {
				av_store(av, i, &sv_undef);
			}
		}
	}
	else if (8==kl && strEQ(key, "NULLABLE")) {
		AV *av = newAV();
		PGresult *result;
		PGTransactionStatusType status;
		D_imp_dbh_from_sth;
		char *statement;
		int nullable; /* 0 = not nullable, 1 = nullable 2 = unknown */
		retsv = newRV(sv_2mortal((SV*)av));

		New(0, statement, 100, char); /* freed below */
		if (!statement)
			croak("No memory");
		statement[0] = '\0';
		while(--i >= 0) {
			nullable=2;
			x = PQftable(imp_sth->result, i);
			y = PQftablecol(imp_sth->result, i);
			if (InvalidOid != x && y > 0) { /* We know what table and column this came from */
				sprintf(statement, "SELECT attnotnull FROM pg_catalog.pg_attribute WHERE attrelid=%d AND attnum=%d", x, y);
				statement[strlen(statement)]='\0';
				result = PQexec(imp_dbh->conn, statement);
				status = imp_sth->result ? PQresultStatus(imp_sth->result) : -1;
				if (PGRES_TUPLES_OK == status && PQntuples(result)) {
					switch(PQgetvalue(result,0,0)[0]) {
					case 't':
						nullable = 0;
						break;
					case 'f':
						nullable = 1;
					}
				}
				PQclear(result);
			}
			av_store(av, i, newSViv(nullable));
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
			av_store(av, i, newSViv(PQfsize(imp_sth->result, i)));
		}
	}
	else if (7==kl && strEQ(key, "pg_type")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {			
			type_info = pg_type_data(PQftype(imp_sth->result,i));
			type_name = (type_info) ? type_info->type_name : "unknown";
			av_store(av, i, newSVpv(type_name, 0));			
		}
	}
	else if (13==kl && strEQ(key, "pg_oid_status")) {
		retsv = newSViv(PQoidValue(imp_sth->result));
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
		char *buffer;
{
		D_imp_dbh(dbh);
		int result;

		/* We must be in COPY IN state */
		if (PGRES_COPY_IN != imp_dbh->copystate)
			croak("pg_putline can only be called directly after issuing a COPY command\n");

#if PGLIBVERSION < 70400
		if (dbis->debug >= 4)
			PerlIO_printf(DBILOGFP, "  dbdpg: PQputline\n");
		return PQputline(imp_dbh->conn, buffer);
#else
		if (dbis->debug >= 4)
			PerlIO_printf(DBILOGFP, "  dbdpg: PQputCopyData\n");

		result = PQputCopyData(imp_dbh->conn, buffer, strlen(buffer));
		if (-1 == result) {
			pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
			return 0;
		}
		else if (1 != result) {
			croak("PQputCopyData gave a value of %d\n", result);
		}
		return 0;
#endif
}


/* ================================================================== */
int
pg_db_getline (dbh, buffer, length)
		SV *dbh;
		char *buffer;
		int length;
{
		D_imp_dbh(dbh);
		int result;
		char *tempbuf;

		/* We must be in COPY OUT state */
		if (PGRES_COPY_OUT != imp_dbh->copystate)
			croak("pg_getline can only be called directly after issuing a COPY command\n");

		if (dbis->debug >= 4)
			PerlIO_printf(DBILOGFP, "  dbdpg: PQgetline\n");

#if PGLIBVERSION < 70400
		result = PQgetline(imp_dbh->conn, buffer, length);
		if (result < 0 || (*buffer == '\\' && *(buffer+1) == '.')) {
			imp_dbh->copystate=0;
			PQendcopy(imp_dbh->conn);
			return -1;
		}
		return result;
#else
		result = PQgetCopyData(imp_dbh->conn, &tempbuf, 0);
		if (-1 == result) {
			*buffer = '\0';
			imp_dbh->copystate=0;
			return -1;
		}
		else if (result < 1) {
			pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
		}
		else {
			strcpy(buffer, tempbuf);
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
		int res;
		PGresult *result;
		ExecStatusType status;

		if (0==imp_dbh->copystate)
			croak("pg_endcopy cannot be called until a COPY is issued");

		if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_pg_endcopy\n"); }

#if PGLIBVERSION < 70400
		if (PGRES_COPY_IN == imp_dbh->copystate)
			PQputline(imp_dbh->conn, "\\.\n");
		res = PQendcopy(imp_dbh->conn);
#else
		if (PGRES_COPY_IN == imp_dbh->copystate) {
			if (dbis->debug >= 4) { PerlIO_printf(DBILOGFP, "dbd_pg_endcopy: PQputCopyEnd\n"); }
			res = PQputCopyEnd(imp_dbh->conn, NULL);
			if (-1 == res) {
				pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
				return 1;
			}
			else if (1 != res)
				croak("PQputCopyEnd returned a value of %d\n", res);
			/* Get the final result of the copy */
			result = PQgetResult(imp_dbh->conn);
			if (1 != PQresultStatus(result)) {
				pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
				return 1;
			}
			PQclear(result);
			res = 0;;
		}
		else {
			res = PQendcopy(imp_dbh->conn);
		}
#endif
		imp_dbh->copystate = 0;
		return res;
}


/* ================================================================== */
void
pg_db_pg_server_trace (dbh, fh)
		SV *dbh;
		FILE *fh;
{
		D_imp_dbh(dbh);

		PQtrace(imp_dbh->conn, fh);
}


/* ================================================================== */
void
pg_db_pg_server_untrace (dbh)
		 SV *dbh;
{
	D_imp_dbh(dbh);

	PQuntrace(imp_dbh->conn);
}


/* ================================================================== */
int
pg_db_savepoint (dbh, imp_dbh, savepoint)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char * savepoint;
{
	PGTransactionStatusType tstatus;
	ExecStatusType status;
	char action[strlen(savepoint) + 11];

	if (imp_dbh->pg_server_version < 80000)
		croak("Savepoints are only supported on server version 8.0 or higher");

	sprintf(action,"savepoint %s",savepoint);

	if (dbis->debug >= 4)
		PerlIO_printf(DBILOGFP, "  dbdpg: %s\n", action);

	/* no action if AutoCommit = on or the connection is invalid */
	if ((NULL == imp_dbh->conn) || (DBDPG_TRUE == DBIc_has(imp_dbh, DBIcf_AutoCommit)))
		return 0;

	/* Start a new transaction if this is the first command */
	if (!imp_dbh->done_begin) {
		status = _result(imp_dbh, "begin");
		if (PGRES_COMMAND_OK != status) {
			pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
			return -2;
		}
		imp_dbh->done_begin = 1;
	}

	status = _result(imp_dbh, action);

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
	PGTransactionStatusType tstatus;
	ExecStatusType status;
	I32 i;
	char action[strlen(savepoint) + 13];

	if (imp_dbh->pg_server_version < 80000)
		croak("Savepoints are only supported on server version 8.0 or higher");

	sprintf(action,"rollback to %s",savepoint);

	if (dbis->debug >= 4)
		PerlIO_printf(DBILOGFP, "  dbdpg: %s\n", action);

	/* no action if AutoCommit = on or the connection is invalid */
	if ((NULL == imp_dbh->conn) || (DBDPG_TRUE == DBIc_has(imp_dbh, DBIcf_AutoCommit)))
		return 0;

	status = _result(imp_dbh, action);

	if (PGRES_COMMAND_OK != status) {
		pg_error(dbh, status, PQerrorMessage(imp_dbh->conn));
		return 0;
	}

	for (i = av_len(imp_dbh->savepoints); i >= 0; i--) {
		SV	*elem = *av_fetch(imp_dbh->savepoints, i, 0);
		if (strEQ(SvPV_nolen(elem), savepoint))
			break;
		av_pop(imp_dbh->savepoints);
	}
	return 1;
}


/* ================================================================== */
int pg_db_release (dbh, imp_dbh, savepoint)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char * savepoint;
{
	PGTransactionStatusType tstatus;
	ExecStatusType status;
	I32 i;
	char action[strlen(savepoint) + 9];

	if (imp_dbh->pg_server_version < 80000)
		croak("Savepoints are only supported on server version 8.0 or higher");

	sprintf(action,"release %s",savepoint);

	if (dbis->debug >= 4)
		PerlIO_printf(DBILOGFP, "  dbdpg: %s\n", action);

	/* no action if AutoCommit = on or the connection is invalid */
	if ((NULL == imp_dbh->conn) || (DBDPG_TRUE == DBIc_has(imp_dbh, DBIcf_AutoCommit)))
		return 0;

	status = _result(imp_dbh, action);

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

/* end of dbdimp.c */
