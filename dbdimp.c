/*

   $Id$

   Copyright (c) 2002-2004 PostgreSQL Global Development Group
   Copyright (c) 1997,1998,1999,2000 Edmund Mergl
   Copyright (c) 2002 Jeffrey W. Baker
   Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
   
   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


/* 
   hard-coded OIDs:   (here we need the postgresql types)
                    pg_sql_type()  1042 (bpchar), 1043 (varchar)
                    ddb_st_fetch() 1042 (bpchar),   16 (bool)
                    ddb_preparse() 1043 (varchar)
                    pgtype_bind_ok()
*/

#include "Pg.h"
#include<assert.h>
#include"types.h"

/* XXX DBI should provide a better version of this */
#define IS_DBI_HANDLE(h)  (SvROK(h) && SvTYPE(SvRV(h)) == SVt_PVHV && SvRMAGICAL(SvRV(h)) && (SvMAGIC(SvRV(h)))->mg_type == 'P')

DBISTATE_DECLARE;


void pg_error();

#include "large_object.c"
#include "prescan_stmt.c"

void
dbd_init (dbistate)
		 dbistate_t *dbistate;
{
	DBIS = dbistate;
}


int
dbd_discon_all (drh, imp_drh)
		 SV *drh;
	imp_drh_t *imp_drh;
{
	dTHR;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_discon_all\n"); }
	
	/* The disconnect_all concept is flawed and needs more work */
	if (!dirty && !SvTRUE(perl_get_sv("DBI::PERL_ENDING",0))) {
		sv_setiv(DBIc_ERR(imp_drh), (IV)1);
		sv_setpv(DBIc_ERRSTR(imp_drh),
						 (char*)"disconnect_all not implemented");
		DBIh_EVENT2(drh, ERROR_event,
								DBIc_ERR(imp_drh), DBIc_ERRSTR(imp_drh));
	}
	return FALSE;
}




/* Turn database notices into perl warnings for proper handling. */

static void
pg_warn (arg, message)
		 void *arg;
		 const char *message;
{
	D_imp_dbh( sv_2mortal(newRV((SV*)arg)) );
	
	if (DBIc_WARN(imp_dbh))
		warn( message );
}

/* Database specific error handling. */

void
pg_error (h, error_num, error_msg)
		 SV *h;
		 int error_num;
		 char *error_msg;
{
	D_imp_xxh(h);
	char *err, *src, *dst; 
	int len = strlen(error_msg);
	
	err = (char *)safemalloc(len + 1);
	if (!err)
		return;
	
	src = error_msg;
	dst = err;
	
	/* copy error message without trailing newlines */
	while (*src != '\0' && *src != '\n') {
		*dst++ = *src++;
	}
	*dst = '\0';
	
	sv_setiv(DBIc_ERR(imp_xxh), (IV)error_num);		 /* set err early */
	sv_setpv(DBIc_ERRSTR(imp_xxh), (char*)err);
	DBIh_EVENT2(h, ERROR_event, DBIc_ERR(imp_xxh), DBIc_ERRSTR(imp_xxh));
	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP, "%s error %d recorded: %s\n",
									err, error_num, SvPV(DBIc_ERRSTR(imp_xxh),na));
	}
	safefree(err);
}


/* ================================================================== */

int
dbd_db_login (dbh, imp_dbh, dbname, uid, pwd)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 char *dbname;
		 char *uid;
		 char *pwd;
{
	dTHR;
	
	char *conn_str;
	char *src;
	char *dest;
	int inquote = 0;
	
	PGresult *pgres_ret;
	char *vstring, *vstart, *vnext; /* Stuff for getting version info */
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "pg_db_login\n"); }
	
	/* build connect string */
	/* DBD-Pg syntax: 'dbname=dbname;host=host;port=port' */
	/* pgsql syntax: 'dbname=dbname host=host port=port user=uid password=pwd' */
	
	conn_str = (char *)safemalloc(strlen(dbname) + strlen(uid) + strlen(pwd) + 16 + 1);
	if (! conn_str)
		return 0;
	
	src = dbname;
	dest = conn_str;
	/* Change all semi-colons to a space, unless quoted */
	while (*src) {
		if (*src == '"')
			inquote = ! inquote;
		else if (*src == ';' && !inquote)
			*dest++ = ' ';
		else
			*dest++ = *src;
		src++;
	}
	*dest = '\0';
	
	if (strlen(uid)) {
		strcat(conn_str, " user=");
		strcat(conn_str, uid);
		if (strlen(pwd)) {
			strcat(conn_str, " password=");
			strcat(conn_str, pwd);
		}
	}
	
	if (dbis->debug >= 2) { PerlIO_printf(DBILOGFP, "pg_db_login: conn_str = >%s<\n", conn_str); }
	
	/* make a connection to the database */
	imp_dbh->conn = PQconnectdb(conn_str);
	safefree(conn_str);
	
	/* check to see that the backend connection was successfully made */
	if (PQstatus(imp_dbh->conn) != CONNECTION_OK) {
		pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
		PQfinish(imp_dbh->conn);
		return 0;
	}
	
	/* Enable warnings to go through perl */
	PQsetNoticeProcessor(imp_dbh->conn, pg_warn, (void *)SvRV(dbh));
	
	/* Quick basic version check -- not robust a'tall TODO: rewrite */
	pgres_ret = PQexec(imp_dbh->conn, "SELECT version()");
	if (pgres_ret && PQresultStatus(pgres_ret) == PGRES_TUPLES_OK) {
		vstring = PQgetvalue(pgres_ret, 0,0); /* Tuple 0 ,filed 0 */
		vstart = index(vstring, ' ');
		
		imp_dbh->version.major = strtol(vstart, &vnext, 10);
		imp_dbh->version.minor = strtol(vnext+1, NULL, 10);
		imp_dbh->version.ver = strtod(vstart, NULL);
		
	} else {
		imp_dbh->version.major = 0;
		imp_dbh->version.minor = 0;
		imp_dbh->version.ver = 0.0;
	}
	PQclear(pgres_ret);
	
	/* PerlIO_printf(DBILOGFP, "v.ma: %i, v.mi: %i v.ver: %f\n",
		 imp_dbh->version.major, imp_dbh->version.minor, imp_dbh->version.ver);
		 
		 if(imp_dbh->version.ver >= 7.3)
		 PerlIO_printf(DBILOGFP, "Greater than 7.3\n");
	*/
	
	imp_dbh->init_commit = 1;			/* initialize AutoCommit */
	imp_dbh->pg_auto_escape = 1;		/* initialize pg_auto_escape */
	imp_dbh->pg_bool_tf = 0;					/* initialize pg_bool_tf */
#ifdef is_utf8_string
	imp_dbh->pg_enable_utf8 = 0;				/* initialize pg_enable_utf8 */
#endif
	
	DBIc_IMPSET_on(imp_dbh);			/* imp_dbh set up now */
	DBIc_ACTIVE_on(imp_dbh);			/* call disconnect before freeing */
	return 1;
}


int 
dbd_db_getfd (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	char id;
	SV* retsv;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_getfd\n"); }
	
	return PQsocket(imp_dbh->conn);
}

SV * 
dbd_db_pg_notifies (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	char id;
	PGnotify* notify;
	AV* ret;
	SV* retsv;
	int status;

	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_pg_notifies\n"); }
	
	status = PQconsumeInput(imp_dbh->conn);
	if (status == 0) { 
		pg_error(dbh, PQstatus(imp_dbh->conn), PQerrorMessage(imp_dbh->conn));
		return 0;
	}
	
	notify = PQnotifies(imp_dbh->conn);
	
	if (!notify) return &sv_undef; 
	
	ret=newAV();
	
	av_push(ret, newSVpv(notify->relname,0) );
	av_push(ret, newSViv(notify->be_pid) );
	
	/* Should free notify memory with PQfreemem() */
	
	retsv = newRV(sv_2mortal((SV*)ret));
	
	return retsv;
}

int
dbd_db_ping (dbh)
		 SV *dbh;
{
	char id;
	D_imp_dbh(dbh);
	PGresult* result;
	ExecStatusType status;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_ping\n"); }
	
	if (NULL != imp_dbh->conn) {
		result = PQexec(imp_dbh->conn, " ");
		status = result ? PQresultStatus(result) : -1;
		PQclear(result);
		
		if (PGRES_EMPTY_QUERY != status) {
			return 0;
		}
		
		return 1;
	}
	
	return 0;
}


int
dbd_db_commit (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_commit\n"); }
	
	/* no commit if AutoCommit = on */
	if (DBIc_has(imp_dbh, DBIcf_AutoCommit) != FALSE) {
		return 0;
	}
	
	if (NULL != imp_dbh->conn) {
		PGresult* result = 0;
		ExecStatusType commitstatus, beginstatus;
		
		/* execute commit */
		result = PQexec(imp_dbh->conn, "commit");
		commitstatus = result ? PQresultStatus(result) : -1;
		PQclear(result);
		
		/* check result */
		if (commitstatus != PGRES_COMMAND_OK) {
			/* Only put the error message in DBH->errstr */
			pg_error(dbh, commitstatus, PQerrorMessage(imp_dbh->conn));
		}
		
		/* start new transaction. AutoCommit must be FALSE, ref. 20 lines up */
		result = PQexec(imp_dbh->conn, "begin");
		beginstatus = result ? PQresultStatus(result) : -1;
		PQclear(result);
		if (beginstatus != PGRES_COMMAND_OK) {
			/* Maybe add some loud barf here? Raising some very high error? */
			pg_error(dbh, beginstatus, "begin failed\n");
			return 0;
		}
		
		/* if the initial COMMIT failed, return 0 now */
		if (commitstatus != PGRES_COMMAND_OK) {
			return 0;
		}
		
		return 1;
	}
	
	return 0;
}


/* TODO: Tx fix that was done to commit needs to be done here also. #rl */
int
dbd_db_rollback (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_rollback\n"); }
	
	/* no rollback if AutoCommit = on */
	if (DBIc_has(imp_dbh, DBIcf_AutoCommit) != FALSE) {
		return 0;
	}
	
	if (NULL != imp_dbh->conn) {
		PGresult* result = 0;
		ExecStatusType status;
		
		/* execute rollback */
		result = PQexec(imp_dbh->conn, "rollback");
		status = result ? PQresultStatus(result) : -1;
		PQclear(result);
		
		/*TODO Correct error message. If returning on error 
			will screw up transaction state? Begin will not get called! */
		/* check result */
		if (status != PGRES_COMMAND_OK) {
			pg_error(dbh, status, "rollback failed\n");
			return 0;
		}
		
		/* start new transaction. AutoCommit must be FALSE, ref. 20 lines up */
		result = PQexec(imp_dbh->conn, "begin");
		status = result ? PQresultStatus(result) : -1;
		PQclear(result);
		if (status != PGRES_COMMAND_OK) {
			pg_error(dbh, status, "begin failed\n");
			return 0;
		}
		
		return 1;
	}
	
	return 0;
}


int
dbd_db_disconnect (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	dTHR;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_disconnect\n"); }
	
	/* We assume that disconnect will always work	*/
	/* since most errors imply already disconnected.
	 * XXX: Um we turn active off, then return 0 on a rollback failing? 
	 * Check to see what happenens -- will we leak memory? :rl
	 */
	DBIc_ACTIVE_off(imp_dbh);
	
	if (NULL != imp_dbh->conn) {
		/* rollback if AutoCommit = off */
		if (DBIc_has(imp_dbh, DBIcf_AutoCommit) == FALSE) {
			PGresult* result = 0;
			ExecStatusType status;
			result = PQexec(imp_dbh->conn, "rollback");
			status = result ? PQresultStatus(result) : -1;
			PQclear(result);
			if (status != PGRES_COMMAND_OK) {
				pg_error(dbh, status, "rollback failed\n");
				return 0;
			}
			if (dbis->debug >= 2) { PerlIO_printf(DBILOGFP, "dbd_db_disconnect: AutoCommit=off -> rollback\n"); }
		}
		
		PQfinish(imp_dbh->conn);
		
		imp_dbh->conn = NULL;
	}
	
	/* We don't free imp_dbh since a reference still exists	*/
	/* The DESTROY method is the only one to 'free' memory.	*/
	/* Note that statement objects may still exists for this dbh!	*/
	return 1;
}


void
dbd_db_destroy (dbh, imp_dbh)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
{
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_destroy\n"); }
	
	if (DBIc_ACTIVE(imp_dbh)) {
		dbd_db_disconnect(dbh, imp_dbh);
	}
	
	/* Nothing in imp_dbh to be freed	*/
	DBIc_IMPSET_off(imp_dbh);
}


int
dbd_db_STORE_attrib (dbh, imp_dbh, keysv, valuesv)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 SV *keysv;
		 SV *valuesv;
{
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	int newval = SvTRUE(valuesv);
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_STORE\n"); }
	
	if (kl==10 && strEQ(key, "AutoCommit")) {
		int oldval = DBIc_has(imp_dbh, DBIcf_AutoCommit);
		DBIc_set(imp_dbh, DBIcf_AutoCommit, newval);
		if (oldval == FALSE && newval != FALSE && imp_dbh->init_commit) {
			/* do nothing, fall through */
			if (dbis->debug >= 2) { PerlIO_printf(DBILOGFP, "dbd_db_STORE: initialize AutoCommit to on\n"); }
		} else if (oldval == FALSE && newval != FALSE) {
			if (NULL != imp_dbh->conn) {
				/* commit any outstanding changes */
				PGresult* result = 0;
				ExecStatusType status;
				result = PQexec(imp_dbh->conn, "commit");
				status = result ? PQresultStatus(result) : -1;
				PQclear(result);
				if (status != PGRES_COMMAND_OK) {
					pg_error(dbh, status, "commit failed\n");
					return 0;
				}
			}			
			if (dbis->debug >= 2) { PerlIO_printf(DBILOGFP, "dbd_db_STORE: switch AutoCommit to on: commit\n"); }
		} else if ((oldval != FALSE && newval == FALSE) || (oldval == FALSE && newval == FALSE && imp_dbh->init_commit)) {
			if (NULL != imp_dbh->conn) {
				/* start new transaction */
				PGresult* result = 0;
				ExecStatusType status;
				result = PQexec(imp_dbh->conn, "begin");
				status = result ? PQresultStatus(result) : -1;
				PQclear(result);
				if (status != PGRES_COMMAND_OK) {
					pg_error(dbh, status, "begin failed\n");
					return 0;
				}
			}
			if (dbis->debug >= 2) { PerlIO_printf(DBILOGFP, "dbd_db_STORE: switch AutoCommit to off: begin\n"); }
		}
		/* only needed once */
		imp_dbh->init_commit = 0;
		return 1;
	} else if (kl==14 && strEQ(key, "pg_auto_escape")) {
		imp_dbh->pg_auto_escape = newval;
	} else if (kl==10 && strEQ(key, "pg_bool_tf")) {
		imp_dbh->pg_bool_tf = newval;
#ifdef is_utf8_string
	} else if (kl==14 && strEQ(key, "pg_enable_utf8")) {
		imp_dbh->pg_enable_utf8 = newval;
#endif
	} else {
		return 0;
	}
}


SV *
dbd_db_FETCH_attrib (dbh, imp_dbh, keysv)
		 SV *dbh;
		 imp_dbh_t *imp_dbh;
		 SV *keysv;
{
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	SV *retsv = Nullsv;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_db_FETCH\n"); }
	
	if (kl==10 && strEQ(key, "AutoCommit")) {
		retsv = boolSV(DBIc_has(imp_dbh, DBIcf_AutoCommit));
	} else if (kl==14 && strEQ(key, "pg_auto_escape")) {
		retsv = newSViv((IV)imp_dbh->pg_auto_escape);
	} else if (kl==10 && strEQ(key, "pg_bool_tf")) {
		retsv = newSViv((IV)imp_dbh->pg_bool_tf);
#ifdef is_utf8_string
	} else if (kl==14 && strEQ(key, "pg_enable_utf8")) {
		retsv = newSViv((IV)imp_dbh->pg_enable_utf8);
#endif
	} else if (kl==11 && strEQ(key, "pg_INV_READ")) {
		retsv = newSViv((IV)INV_READ);
	} else if (kl==12 && strEQ(key, "pg_INV_WRITE")) {
		retsv = newSViv((IV)INV_WRITE);
	}
	
	if (!retsv)
		return Nullsv;
	
	if (retsv == &sv_yes || retsv == &sv_no) {
		return retsv; /* no need to mortalize yes or no */
	}
	return sv_2mortal(retsv);
}


/* ================================================================== */


int
dbd_st_prepare (sth, imp_sth, statement, attribs)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 char *statement;
		 SV *attribs;
{
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_prepare: statement = >%s<\n", statement); }
	
	/* scan statement for '?', ':1' and/or ':foo' style placeholders */
	if((dbd_preparse(sth, imp_sth, statement)) == 0)
		return 0;
	
	if (is_tx_stmt(statement)) {
		warn("please use DBI functions for transaction handling");
		return(0);
	}
	
	/* initialize new statement handle */
	imp_sth->result	= 0;
	imp_sth->cur_tuple = 0;
	
	DBIc_IMPSET_on(imp_sth);
	return 1;
}


int
dbd_preparse (sth, imp_sth, statement)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 const char *statement;
{
	static unsigned int prep_stmt_id = 0;
	int place_holder_count, stmt_len, status;
	int digits, i;
	int offset = 0;
	D_imp_dbh_from_sth;
	
	++prep_stmt_id;
	digits = 0;
	i = prep_stmt_id;
	do {
		++digits;
		i /=10;
	} while (i>0);	 /* 12*/
	
	/* //PerlIO_printf(DBILOGFP, "Statement: %s \n", statement); */
	prescan_stmt(statement, &stmt_len, &place_holder_count);
	
	/* //PerlIO_printf(DBILOGFP, "Place holders: %i \n", place_holder_count); */
	/* add space for placeholders candidates */
	stmt_len += calc_ph_space(place_holder_count);
	
	
	offset += strlen ("PREPARE \"DBD::ChurlPg::cached_query \" (");
	offset += digits; /* number of digits in prep_statement_id */
	offset += place_holder_count*strlen("varchar, ");
	offset += strlen(") AS");
	
	stmt_len += offset;
	++stmt_len; /* for term \0 */
	
	/* //PerlIO_printf(DBILOGFP, "Smt len:%i Offset %i\n", stmt_len, offset); */
	
	Newc(0, imp_sth->statement, stmt_len, char, char);
	memset(imp_sth->statement, ' ', offset+1);
	if (place_holder_count) {
		/* +1 so we can use a 1 based idx (placeholders start from 1)*/
		Newc(0, imp_sth->place_holders, place_holder_count+1,
				 phs_t**, phs_t*);
	} else {
		imp_sth->place_holders = 0;
	}
	
	place_holder_count = rewrite_placeholders(imp_sth, statement, imp_sth->statement+offset,0);
	imp_sth->phc = place_holder_count;
	
	/* // PerlIO_printf(DBILOGFP, "Rewritten stmt: %s\n", imp_sth->statement+offset); */
	
	assert(strlen(imp_sth->statement)+1 <= stmt_len);
	/* if not dml, no need to continue, As we are not going to
		 server side prepare this statement TODO: remalloc*/
	if (!is_dml(imp_sth->statement+offset) || imp_dbh->version.ver < 7.3)
		return 1;
	
	/* 1 == PREPARE -- TODO: Fix ugly number thing*/
	build_preamble(imp_sth->statement, 1, place_holder_count, prep_stmt_id);
	
	/* //PerlIO_printf(DBILOGFP, "Rewritten stmt: %s\n", imp_sth->statement); */
	
	imp_sth->result = PQexec(imp_dbh->conn, imp_sth->statement);
	status = imp_sth->result ? PQresultStatus(imp_sth->result) : -1;
	if (status != PGRES_COMMAND_OK) {
		pg_error(sth,status, PQerrorMessage(imp_dbh->conn));
		return 0;
	}
	if (imp_sth->result)
		PQclear(imp_sth->result);
	
	/* 2 == EXECUTE -- TODO: Fix ugly number thing & remalloc*/
	build_preamble(imp_sth->statement, 2, place_holder_count, prep_stmt_id);
	/* //PerlIO_printf(DBILOGFP, "Rewritten stmt: %s\n", imp_sth->statement); */
	imp_sth->server_prepared = 1;
	
	assert(strlen(imp_sth->statement)+1 <= stmt_len);
	return 1;
}



int
deallocate_statement (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	int status, max_len;
	char *stmt, *dest, *start;
	PGresult *result;
	D_imp_dbh_from_sth;
	
	if (NULL == imp_dbh->conn)
		return 1;
	
	max_len = strlen(imp_sth->statement)+strlen("DEALLOCATE ")+2;
	Newc(0,stmt, max_len, char, char);
	
	start = strstr(imp_sth->statement, "\"DBD::ChurlPg::cached_query");
	
	if(!start) {
		pg_error(sth, -1, "Could not Deallocate statment... Preamble"
						 "not found");
		return -1;
	}
	
	sprintf(stmt, "DEALLOCATE ");
	
	dest = stmt+11;
	
	*dest++ = *start++;
	while ((*dest++ = *start++))
		if ('"' == *(dest-1))
			break;
	
	*dest = '\0';
	
	/* // PerlIO_printf(DBILOGFP, "Rewritten stmt: %s, Max Len: %i, Act Len:%i\n", stmt, max_len, strlen(stmt)); */
	
	result = PQexec(imp_dbh->conn, stmt);
	Safefree(stmt);
	
	status = result ? PQresultStatus(result) : -1;
	PQclear(result);
	
	if (PGRES_COMMAND_OK != status) {
		pg_error(sth,status, PQerrorMessage(imp_dbh->conn));
		return -1;
	}
	return 1;
	
}



/* TODO: break this sub up. */
int
dbd_bind_ph (sth, imp_sth, ph_namesv, newvalue, sql_type, attribs, is_inout, maxlen)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 SV *ph_namesv;
		 SV *newvalue;
		 IV sql_type;
		 SV *attribs;
		 int is_inout;
		 IV maxlen;
{
	SV **phs_svp;
	SV **svp;
	STRLEN name_len;
	char *name = Nullch;
	char namebuf[30];
	phs_t *phs;
	sql_type_info_t *sql_type_info;
	int pg_type, bind_type;
	char *value_string;
	STRLEN value_len;

	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_bind_ph\n"); }
	
	if (is_inout)
		croak("bind_inout not supported by this driver");
	
	
	/* check if placeholder was passed as a number		*/
	if (SvGMAGICAL(ph_namesv)) { /* eg if from tainted expression */
		mg_get(ph_namesv);
	}
	if (!SvNIOKp(ph_namesv)) {
		name = SvPV(ph_namesv, name_len);
	}
	if (SvNIOKp(ph_namesv) || (name && isDIGIT(name[0]))) {
		sprintf(namebuf, "$%d", (int)SvIV(ph_namesv));
		name = namebuf;
		name_len = strlen(name);
		assert(name_len < sizeof(namebuf));
	}
	assert(name != Nullch);
	
	if (SvTYPE(newvalue) > SVt_PVLV) { /* hook for later array logic	*/
		croak("Can't bind a non-scalar value (%s)", neatsvpv(newvalue,0));
	}
	if ((SvROK(newvalue) &&!IS_DBI_HANDLE(newvalue) &&!SvMAGIC(newvalue))) {
		/* dbi handle allowed for cursor variables */
		croak("Can't bind a reference (%s)", neatsvpv(newvalue,0));
	}

	if (SvTYPE(newvalue) == SVt_PVLV && is_inout) {	 /* may allow later */
		croak("Can't bind ``lvalue'' mode scalar as inout parameter (currently)");
	}
	
	if (dbis->debug >= 2) {
		PerlIO_printf(DBILOGFP, "		 bind %s <== %s (type %ld", name, neatsvpv(newvalue,0), (long)sql_type);
		if (is_inout) {
			PerlIO_printf(DBILOGFP, ", inout 0x%lx, maxlen %ld", (long)newvalue, (long)maxlen);
		}
		if (attribs) {
			PerlIO_printf(DBILOGFP, ", attribs: %s", neatsvpv(attribs,0));
		}
		PerlIO_printf(DBILOGFP, ")\n");
	}
	
	
  /* // XXX this is broken: bind_param(1,1,{TYPE=>SQL_INTEGER}); */
	if (attribs) {
		if (sql_type)
			croak ("Cannot specify both sql_type and pg_type");
		
		if ((svp = hv_fetch((HV*)SvRV(attribs),"pg_type", 7, 0))==NULL)
			croak("DBD::ChurlPg only knows about the pg_type attribute");
		
		pg_type = SvIV(*svp);
		
		
		if ((sql_type_info = pg_type_data(pg_type))) {
			if (!sql_type_info->bind_ok) {
				croak("Can't bind %s, pg_type %s not supported"
							"by DBD::ChurlPg",
							name, sql_type_info->type_name);
			}
		} else {
			croak("Cannot bind %s unknown sql_type %i",	name, sql_type);
		}
		bind_type = sql_type_info->type_id;
		
	} else if (sql_type) {
		
		if ((sql_type_info = sql_type_data(sql_type))) {
			/* always bind as pg_type, because we know we are inserting
				 into a pg database... It would make no sense to quote
				 something to sql semantics and break the insert.
			*/
			bind_type = sql_type_info->type.pg;
		} else {
			croak("Cannot bind %s unknown sql_type %i",	name, sql_type);
		}
		
	} else {
		sql_type_info = pg_type_data(VARCHAROID);
		if (!sql_type_info)
			croak("Default type is bad!!!!???");
		
		bind_type = sql_type_info->type_id;
	}

	
	/* get the place holder */
	phs_svp = hv_fetch(imp_sth->all_params_hv, name, name_len, 0);
	if (phs_svp == NULL) {
		croak("Can't bind unknown placeholder '%s' (%s)",
					name, neatsvpv(ph_namesv,0));
	}
	phs = (phs_t*)(void*)SvPVX(*phs_svp);
	
	
	if (phs->is_bound && phs->ftype != bind_type) {
		croak("Can't change TYPE of param %s to %d after initial bind",
					phs->name, sql_type);
	} else {
		phs->ftype = bind_type;
	}
	
	/* convert to a string ASAP */
	if (!SvPOK(newvalue) && SvOK(newvalue)) {
		sv_2pv(newvalue, &na);
	}
	/* phs->sv is copy of real variable, upgrade to at least string */
	(void)SvUPGRADE(newvalue, SVt_PV);
	

	if (!SvOK(newvalue)) {
		phs->quoted = strdup("NULL");
		if (NULL == phs->quoted)
			croak("No memory");
		phs->quoted_len = strlen(phs->quoted);
	} else {
		value_string = SvPV(newvalue, value_len);
		phs->quoted = sql_type_info->quote(value_string, value_len, &phs->quoted_len);
	}
	
	phs->is_bound = 1;
	return 1;
	
}


/*TODO: make smaller */
dbd_st_execute (sth, imp_sth)  /* <= -2:error, >=0:ok row count, (-1=unknown count) */
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	/* //dTHR; */
	
	D_imp_dbh_from_sth;
	ExecStatusType status = -1;
	char *cmdStatus;
	char *cmdTuples;
	char *statement;
	int ret = -2;
	int num_fields;
	int max_len =0;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_execute\n"); }
	
	if (NULL == imp_dbh->conn) {
		pg_error(sth, -1, "execute on disconnected handle");
		return -2;
	}
	
	if (! imp_sth->statement) {
		pg_error(sth, -1, "statement not prepared\n");
		return -2;
	}
	
	max_len = strlen(imp_sth->statement)+1;
	/* do we have input parameters ? */
	if ((int)DBIc_NUM_PARAMS(imp_sth) > 0) {
		/* How much do we need to malloc to hold resultant string */
		HV *hv = imp_sth->all_params_hv;
		SV *sv;
		char *key;
		I32 retlen;
		hv_iterinit(hv);
		/* //PerlIO_printf(DBILOGFP, "b4 max_len: %i\n", max_len); */
		while( (sv = hv_iternextsv(hv, &key, &retlen)) != NULL ) {
			if (sv != &sv_undef) {
				phs_t *phs_tpl = (phs_t*)(void*)SvPVX(sv);
				if (!phs_tpl->is_bound) {
					pg_error(sth, -1,
									 "Execute called with unbound placeholder");
					return -2;
				}
				max_len += phs_tpl->quoted_len * phs_tpl->count;
			}
		}
		
		Newc(0, statement, max_len, char, char);
		
		/* scan statement for '$1' style placeholders and replace with values*/
		if ((ret = rewrite_execute_stmt(sth, imp_sth, statement, sth)) < 0)
			return ret;
	} else {
		statement = imp_sth->statement;
	}
	
	assert(strlen(statement)+1 <= max_len);
	
	
	
	if (dbis->debug >= 2) { PerlIO_printf(DBILOGFP, "dbd_st_execute: statement = >%s<\n", statement); }
	
	/* clear old result (if any) */
	if (imp_sth->result) {
		PQclear(imp_sth->result);
	}
	
	/* execute statement */
	imp_sth->result = PQexec(imp_dbh->conn, statement);
	
	/* free statement string in case of input parameters */
	if ((int)DBIc_NUM_PARAMS(imp_sth) > 0) {
		Safefree(statement);
	}
	
	/* check status */
	status	= imp_sth->result ? PQresultStatus(imp_sth->result)	: -1;
	cmdStatus = imp_sth->result ? (char *)PQcmdStatus(imp_sth->result) : "";
	cmdTuples = imp_sth->result ? (char *)PQcmdTuples(imp_sth->result) : "";
	
	if (PGRES_TUPLES_OK == status) {
		/* select statement */
		num_fields = PQnfields(imp_sth->result);
		imp_sth->cur_tuple = 0;
		DBIc_NUM_FIELDS(imp_sth) = num_fields;
		DBIc_ACTIVE_on(imp_sth);
		ret = PQntuples(imp_sth->result);
	} else if (PGRES_COMMAND_OK == status) {
		/* non-select statement */
		if (! strncmp(cmdStatus, "DELETE", 6) || ! strncmp(cmdStatus, "INSERT", 6) || ! strncmp(cmdStatus, "UPDATE", 6)) {
			ret = atoi(cmdTuples);
		} else {
			ret = -1;
		}
	} else if (PGRES_COPY_OUT == status || PGRES_COPY_IN == status) {
		/* Copy Out/In data transfer in progress */
		ret = -1;
	} else {
		pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
		ret = -2;
	}
	
	/* store the number of affected rows */
	imp_sth->rows = ret;
	
	return ret;
}


/*TODO: pg_bool_tf */

AV *
dbd_st_fetch (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	sql_type_info_t *type_info;
	int num_fields;
	char *value;
	char *p;
	int i, pg_type, value_len, chopblanks, len;
	AV *av;
	D_imp_dbh_from_sth;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_fetch\n"); }
	
	/* Check that execute() was executed sucessfully */
	if ( !DBIc_ACTIVE(imp_sth) ) {
		pg_error(sth, 1, "no statement executing\n");	
		return Nullav;
	}
	
	if ( imp_sth->cur_tuple == PQntuples(imp_sth->result) ) {
		imp_sth->cur_tuple = 0;
		DBIc_ACTIVE_off(imp_sth);
		return Nullav; /* we reached the last tuple */
	}
	
	av = DBIS->get_fbav(imp_sth);
	num_fields = AvFILL(av)+1;
	
	chopblanks = DBIc_has(imp_sth, DBIcf_ChopBlanks);
	
	for(i = 0; i < num_fields; ++i) {
		
		SV *sv = AvARRAY(av)[i];
		if (PQgetisnull(imp_sth->result, imp_sth->cur_tuple, i)) {
			sv_setsv(sv, &sv_undef);
		} else {
			value = (char*)PQgetvalue(imp_sth->result, imp_sth->cur_tuple, i); 
			
			pg_type = PQftype(imp_sth->result, i);
			type_info = pg_type_data(pg_type);
			
			if (type_info)
				type_info->dequote(value, &value_len); /* dequote in place */
			else
				value_len = strlen(value);
			
			if (type_info && (type_info->type_id == BOOLOID) &&
					imp_dbh->pg_bool_tf)
				{
					*value = (*value == '1') ? 't' : 'f';
				}

			sv_setpvn(sv, value, value_len);
			
			if (type_info && (type_info->type_id == BPCHAROID) && 
				chopblanks)
			{
				p = SvEND(sv);
				len = SvCUR(sv);
				while(len && *--p == ' ')
					--len;
				if (len != SvCUR(sv)) {
					SvCUR_set(sv, len);
					*SvEND(sv) = '\0';
				}
			}
			
#ifdef is_utf8_string
			/* XXX Under what circumstances is type_info NULL? */
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
}

is_high_bit_set(val)
		 char *val;
{
	while (*val++)
		if (*val & 0x80) return 1;
	return 0;
}

/* TODO: test for rows and define rows so that this rows() will be used */
int
dbd_st_rows (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_rows\n"); }
	
	return imp_sth->rows;
}


int
dbd_st_finish (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	dTHR;
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_finish\n"); }
	
	if (DBIc_ACTIVE(imp_sth) && imp_sth->result) {
		PQclear(imp_sth->result);
		imp_sth->result = 0;
		imp_sth->rows = 0;
	}
	
	DBIc_ACTIVE_off(imp_sth);
	return 1;
}


void
dbd_st_destroy (sth, imp_sth)
		 SV *sth;
		 imp_sth_t *imp_sth;
{
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_destroy\n"); }
	
	/* Free off contents of imp_sth */
	
	if (imp_sth->server_prepared)
		if (deallocate_statement(sth, imp_sth) < 1)
			warn("Something Ugly Happened. And whatever it was, it caused"
					 "us not to be able to deallocate the prepared statement. "
					 "Prolly a tx went bad or something like that");
	
	Safefree(imp_sth->statement);
	if (imp_sth->place_holders)
		Safefree(imp_sth->place_holders);
	
	if (imp_sth->result) {
		PQclear(imp_sth->result);
		imp_sth->result = 0;
	}
	
	if (imp_sth->all_params_hv) {
		HV *hv = imp_sth->all_params_hv;
		SV *sv;
		char *key;
		I32 retlen;
		hv_iterinit(hv);
		while( (sv = hv_iternextsv(hv, &key, &retlen)) != NULL ) {
			if (sv != &sv_undef) {
				phs_t *phs_tpl = (phs_t*)(void*)SvPVX(sv);
				/* sv_free(phs_tpl->sv); */
				safefree(phs_tpl->quoted);
			}
		}
		sv_free((SV*)imp_sth->all_params_hv);
	}
	
	DBIc_IMPSET_off(imp_sth); /* let DBI know we've done it */
}


int
dbd_st_STORE_attrib (sth, imp_sth, keysv, valuesv)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 SV *keysv;
		 SV *valuesv;
{
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_STORE\n"); }
	
	return FALSE;
}


SV *
dbd_st_FETCH_attrib (sth, imp_sth, keysv)
		 SV *sth;
		 imp_sth_t *imp_sth;
		 SV *keysv;
{
	STRLEN kl;
	char *key = SvPV(keysv,kl);
	int i, sz;
	SV *retsv = Nullsv;
	char *type_name;
	sql_type_info_t *type_info;
	
	
	if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_FETCH\n"); }
	
	if (! imp_sth->result) {
		return Nullsv;
	}
	
	i = DBIc_NUM_FIELDS(imp_sth);
	
	if (kl == 4 && strEQ(key, "NAME")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			av_store(av, i, newSVpv(PQfname(imp_sth->result, i),0));
		}
	} else if ( kl== 4 && strEQ(key, "TYPE")) {
		/* Need to convert the Pg type to ANSI/SQL type. */
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			type_info = pg_type_data(PQftype(imp_sth->result, i));
			av_store(av, i, newSViv( type_info ? type_info->type.sql : 0 ) );
		}
	} else if (kl==9 && strEQ(key, "PRECISION")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			sz = PQfsize(imp_sth->result, i);
			av_store(av, i, sz > 0 ? newSViv(sz) : &sv_undef);
		}
	} else if (kl==5 && strEQ(key, "SCALE")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			av_store(av, i, &sv_undef);
		}
	} else if (kl==8 && strEQ(key, "NULLABLE")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			av_store(av, i, newSViv(2));
		}
	} else if (kl==10 && strEQ(key, "CursorName")) {
		retsv = &sv_undef;
	} else if (kl==11 && strEQ(key, "RowsInCache")) {
		retsv = &sv_undef;
	} else if (kl==7 && strEQ(key, "pg_size")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		while(--i >= 0) {
			av_store(av, i, newSViv(PQfsize(imp_sth->result, i)));
		}
	} else if (kl==7 && strEQ(key, "pg_type")) {
		AV *av = newAV();
		retsv = newRV(sv_2mortal((SV*)av));
		
		while(--i >= 0) {
			
			type_info = pg_type_data(PQftype(imp_sth->result,i));
			type_name = (type_info) ? type_info->type_name : "unknown";
			av_store(av, i, newSVpv(type_name, 0));
			
		}
	} else if (kl==13 && strEQ(key, "pg_oid_status")) {
		retsv = newSViv(PQoidValue(imp_sth->result));
	} else if (kl==13 && strEQ(key, "pg_cmd_status")) {
		retsv = newSVpv((char *)PQcmdStatus(imp_sth->result), 0);
	} else {
		return Nullsv;
	}
	
	return sv_2mortal(retsv);
}


/* end of dbdimp.c */
