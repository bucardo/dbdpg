/*
   $Id$

   Copyright (c) 2000-2004 PostgreSQL Global Development Group
   Copyright (c) 1997,1998,1999,2000 Edmund Mergl
   Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce

   You may distribute under the terms of either the GNU General Public
   License or the Artistic License, as specified in the Perl README file.

*/


#include "Pg.h"
#include "quote.h"
#include "types.h"
#include "pg_typeOID.h"

#ifdef _MSC_VER
#define strncasecmp(a,b,c) _strnicmp((a),(b),(c))
#endif


DBISTATE_DECLARE;

MODULE = DBD::Pg   PACKAGE = DBD::Pg


I32
constant(name=Nullch)
    char *name
    PROTOTYPE:
    ALIAS:
    PG_BOOL      = 16
    PG_BYTEA     = 17
    PG_CHAR      = 18
    PG_INT8      = 20
    PG_INT2      = 21
    PG_INT4      = 23
    PG_TEXT      = 25
    PG_OID       = 26
    PG_FLOAT4    = 700
    PG_FLOAT8    = 701
    PG_ABSTIME   = 702
    PG_RELTIME   = 703
    PG_TINTERVAL = 704
    PG_BPCHAR    = 1042
    PG_VARCHAR   = 1043
    PG_DATE      = 1082
    PG_TIME      = 1083
    PG_DATETIME  = 1184
    PG_TIMESPAN  = 1186
    PG_TIMESTAMP = 1296
    CODE:
    if (!ix) {
        if (!name) name = GvNAME(CvGV(cv));
            croak("Unknown DBD::Pg constant '%s'", name);
        }
    else RETVAL = ix;
    OUTPUT:
    RETVAL

INCLUDE: Pg.xsi


# ------------------------------------------------------------
# db functions
# ------------------------------------------------------------
MODULE=DBD::Pg     PACKAGE = DBD::Pg::db


#TODO: make quote(foo, {type=>SQL_INTEGER}) work  #rl
#TODO: make quote(foo, {pg_type=>DBD::Pg::PG_INTEGER}) work  #rl
SV*
quote(dbh, to_quote_sv, type_sv=Nullsv)
    SV* dbh
    SV* to_quote_sv
    SV* type_sv

    CODE:
    {
        char *to_quote;
        STRLEN len;
        STRLEN retlen=0;
        char *quoted;
        sql_type_info_t *type_info;


        if(type_sv && SvOK(type_sv)) {
                if SvMAGICAL(type_sv)
                        mg_get(type_sv);

                type_info = sql_type_data(SvIV(type_sv));
		if (!type_info) {
			warn("Unknown type %" IVdf ", "
			    "defaulting to VARCHAR",SvIV(type_sv));

			type_info = pg_type_data(VARCHAROID);
		}
        } else {
                /* default to varchar */
		 type_info = pg_type_data(VARCHAROID);
        }

        if (!SvOK(to_quote_sv))  {
                quoted = "NULL";
                len = 4;
                RETVAL = newSVpvn(quoted,len);
        } else {
                if (SvMAGICAL(to_quote_sv))
                        mg_get(to_quote_sv);

                to_quote = SvPV(to_quote_sv, len);
                quoted = type_info->quote(to_quote, len, &retlen);
                RETVAL = newSVpvn(quoted, retlen);
                Safefree (quoted);
        }
    }
    OUTPUT:
    	RETVAL

# ------------------------------------------------------------
# database level interface PG specific
# ------------------------------------------------------------
MODULE = DBD::Pg  PACKAGE = DBD::Pg::db

SV* state(dbh)
	SV *dbh
	CODE:
	D_imp_dbh(dbh);
	ST(0) = newSVpvn(imp_dbh->sqlstate, 5);

int
_ping(dbh)
    SV * dbh
    CODE:
    int ret;
    ret = dbd_db_ping(dbh);
    if (ret == 0) {
        XST_mUNDEF(0);
    }
    else {
        XST_mIV(0, ret);
    }

void
getfd(dbh)
    SV * dbh
    CODE:
    int ret;
    D_imp_dbh(dbh);

    ret = dbd_db_getfd(dbh, imp_dbh);
    ST(0) = sv_2mortal( newSViv( ret ) );

void
pg_notifies(dbh)
    SV * dbh
    CODE:
    D_imp_dbh(dbh);

    ST(0) = dbd_db_pg_notifies(dbh, imp_dbh);

void
lo_open(dbh, lobjId, mode)
    SV * dbh
    unsigned int lobjId
    int mode
    CODE:
        int ret = pg_db_lo_open(dbh, lobjId, mode);
        ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;

void
lo_close(dbh, fd)
    SV * dbh
    int fd
    CODE:
        ST(0) = (-1 != pg_db_lo_close(dbh, fd)) ? &sv_yes : &sv_no;


void
lo_read(dbh, fd, buf, len)
        SV * dbh
        int fd
        char * buf
        int len
    PREINIT:
        SV *bufsv = SvROK(ST(2)) ? SvRV(ST(2)) : ST(2);
        int ret;
    CODE:
        buf = SvGROW(bufsv, len + 1);
        ret = pg_db_lo_read(dbh, fd, buf, len);
        if (ret > 0) {
            SvCUR_set(bufsv, ret);
            *SvEND(bufsv) = '\0';
            sv_setpvn(ST(2), buf, ret);
            SvSETMAGIC(ST(2));
        }
        ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_write(dbh, fd, buf, len)
    SV * dbh
    int fd
    char * buf
    int len
    CODE:
        int ret = pg_db_lo_write(dbh, fd, buf, len);
        ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_lseek(dbh, fd, offset, whence)
    SV * dbh
    int fd
    int offset
    int whence
    CODE:
        int ret = pg_db_lo_lseek(dbh, fd, offset, whence);
        ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_creat(dbh, mode)
    SV * dbh
    int mode
    CODE:
        int ret = pg_db_lo_creat(dbh, mode);
        ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_tell(dbh, fd)
    SV * dbh
    int fd
    CODE:
        int ret = pg_db_lo_tell(dbh, fd);
        ST(0) = (-1 != ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_unlink(dbh, lobjId)
    SV * dbh
    unsigned int lobjId
    CODE:
        ST(0) = (-1 != pg_db_lo_unlink(dbh, lobjId)) ? &sv_yes : &sv_no;


void
lo_import(dbh, filename)
    SV * dbh
    char * filename
    CODE:
        unsigned int ret = pg_db_lo_import(dbh, filename);
        ST(0) = (ret) ? sv_2mortal(newSViv(ret)) : &sv_undef;


void
lo_export(dbh, lobjId, filename)
    SV * dbh
    unsigned int lobjId
    char * filename
    CODE:
        ST(0) = (-1 != pg_db_lo_export(dbh, lobjId, filename)) ? &sv_yes : &sv_no;


void
putline(dbh, buf)
    SV * dbh
    char * buf
    CODE:
        int ret = pg_db_putline(dbh, buf);
        ST(0) = (-1 != ret) ? &sv_yes : &sv_no;


void
getline(dbh, buf, len)
    PREINIT:
        SV *bufsv = SvROK(ST(1)) ? SvRV(ST(1)) : ST(1);
    INPUT:
        SV * dbh
        int len
        char * buf = sv_grow(bufsv, len);
    CODE:
        int ret = pg_db_getline(dbh, buf, len);
        if (*buf == '\\' && *(buf+1) == '.') {
            ret = -1;
        }
    sv_setpv((SV*)ST(1), buf);
    SvSETMAGIC(ST(1));
        ST(0) = (-1 != ret) ? &sv_yes : &sv_no;


void
endcopy(dbh)
    SV * dbh
    CODE:
        ST(0) = (-1 != pg_db_endcopy(dbh)) ? &sv_yes : &sv_no;

int
_pg_type_info (type_sv=Nullsv)
    SV* type_sv
    CODE:
    {
    	# int type_num = VARCHAROID;
    	int type_num = 0;
        sql_type_info_t *type_info;

        if(type_sv && SvOK(type_sv)) {
                if SvMAGICAL(type_sv)
                        mg_get(type_sv);

                type_info = pg_type_data(SvIV(type_sv));
                type_num = type_info ? type_info->type.sql : SQL_VARCHAR;
        } 
	RETVAL = type_num;
        XST_mIV(0, RETVAL);
    }
# ST(0) = (-1 != type_num) ? &sv_yes : &sv_no; */


# -- end of DBD::Pg::db


# ------------------------------------------------------------
# statement level interface PG specific
# ------------------------------------------------------------
MODULE = DBD::Pg  PACKAGE = DBD::Pg::st

SV* state(sth)
	SV *sth;
	CODE:
	D_imp_sth(sth);
	D_imp_dbh_from_sth;
	ST(0) = newSVpvn(imp_dbh->sqlstate, 5);

# end of Pg.xs
