/* begin large_object.c */

int
pg_db_lo_open (dbh, lobjId, mode)
		 SV *dbh;
		 unsigned int lobjId;
		 int mode;
{
	D_imp_dbh(dbh);
	return lo_open(imp_dbh->conn, lobjId, mode);
}


int
pg_db_lo_close (dbh, fd)
		SV *dbh;
		int fd;
{
		D_imp_dbh(dbh);
		return lo_close(imp_dbh->conn, fd);
}


int
pg_db_lo_read (dbh, fd, buf, len)
		SV *dbh;
		int fd;
		char *buf;
		int len;
{
		D_imp_dbh(dbh);
		return lo_read(imp_dbh->conn, fd, buf, len);
}


int
pg_db_lo_write (dbh, fd, buf, len)
		SV *dbh;
		int fd;
		char *buf;
		int len;
{
		D_imp_dbh(dbh);
		return lo_write(imp_dbh->conn, fd, buf, len);
}


int
pg_db_lo_lseek (dbh, fd, offset, whence)
		SV *dbh;
		int fd;
		int offset;
		int whence;
{
		D_imp_dbh(dbh);
		return lo_lseek(imp_dbh->conn, fd, offset, whence);
}


unsigned int
pg_db_lo_creat (dbh, mode)
		SV *dbh;
		int mode;
{
		D_imp_dbh(dbh);
		return lo_creat(imp_dbh->conn, mode);
}


int
pg_db_lo_tell (dbh, fd)
		SV *dbh;
		int fd;
{
		D_imp_dbh(dbh);
		return lo_tell(imp_dbh->conn, fd);
}


int
pg_db_lo_unlink (dbh, lobjId)
		SV *dbh;
		unsigned int lobjId;
{
		D_imp_dbh(dbh);
		return lo_unlink(imp_dbh->conn, lobjId);
}


unsigned int
pg_db_lo_import (dbh, filename)
		SV *dbh;
		char *filename;
{
		D_imp_dbh(dbh);
		return lo_import(imp_dbh->conn, filename);
}


int
pg_db_lo_export (dbh, lobjId, filename)
		SV *dbh;
		unsigned int lobjId;
		char *filename;
{
		D_imp_dbh(dbh);
		return lo_export(imp_dbh->conn, lobjId, filename);
}


int
pg_db_putline (dbh, buffer)
		SV *dbh;
		char *buffer;
{
		D_imp_dbh(dbh);
		return PQputline(imp_dbh->conn, buffer);
}


int
pg_db_getline (dbh, buffer, length)
		SV *dbh;
		char *buffer;
		int length;
{
		D_imp_dbh(dbh);
		return PQgetline(imp_dbh->conn, buffer, length);
}


int
pg_db_endcopy (dbh)
		SV *dbh;
{
		D_imp_dbh(dbh);
		return PQendcopy(imp_dbh->conn);
}


int
dbd_st_blob_read (sth, imp_sth, lobjId, offset, len, destrv, destoffset)
		SV *sth;
		imp_sth_t *imp_sth;
		int lobjId;
		long offset;
		long len;
		SV *destrv;
		long destoffset;
{
		D_imp_dbh_from_sth;
		int ret, lobj_fd, nbytes, nread;
		/* PGresult* result;
		ExecStatusType status; */
		SV *bufsv;
		char *tmp;

		if (dbis->debug >= 1) { PerlIO_printf(DBILOGFP, "dbd_st_blob_read\n"); }
		/* safety check */
		if (lobjId <= 0) {
				pg_error(sth, -1, "dbd_st_blob_read: lobjId <= 0");
				return 0;
		}
		if (offset < 0) {
				pg_error(sth, -1, "dbd_st_blob_read: offset < 0");
				return 0;
		}
		if (len < 0) {
				pg_error(sth, -1, "dbd_st_blob_read: len < 0");
				return 0;
		}
		if (! SvROK(destrv)) {
				pg_error(sth, -1, "dbd_st_blob_read: destrv not a reference");
				return 0;
		}
		if (destoffset < 0) {
				pg_error(sth, -1, "dbd_st_blob_read: destoffset < 0");
				return 0;
		}

		/* dereference destination and ensure it's writable string */
		bufsv = SvRV(destrv);
		if (! destoffset) {
				sv_setpvn(bufsv, "", 0);
		}

		/* execute begin
		result = PQexec(imp_dbh->conn, "begin");
		status = result ? PQresultStatus(result) : -1;
		PQclear(result);
		if (status != PGRES_COMMAND_OK) {
				pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
				return 0;
		}
		*/

		/* open large object */
		lobj_fd = lo_open(imp_dbh->conn, lobjId, INV_READ);
		if (lobj_fd < 0) {
				pg_error(sth, -1, PQerrorMessage(imp_dbh->conn));
				return 0;
		}

		/* seek on large object */
		if (offset > 0) {
				ret = lo_lseek(imp_dbh->conn, lobj_fd, offset, SEEK_SET);
				if (ret < 0) {
						pg_error(sth, -1, PQerrorMessage(imp_dbh->conn));
						return 0;
				}
		}

		/* read from large object */
		nread = 0;
		SvGROW(bufsv, destoffset + nread + BUFSIZ + 1);
		tmp = (SvPVX(bufsv)) + destoffset + nread;
		while ((nbytes = lo_read(imp_dbh->conn, lobj_fd, tmp, BUFSIZ)) > 0) {
				nread += nbytes;
				/* break if user wants only a specified chunk */
				if (len > 0 && nread > len) {
						nread = len;
						break;
				}
				SvGROW(bufsv, destoffset + nread + BUFSIZ + 1);
				tmp = (SvPVX(bufsv)) + destoffset + nread;
		}

		/* terminate string */
		SvCUR_set(bufsv, destoffset + nread);
		*SvEND(bufsv) = '\0';

		/* close large object */
		ret = lo_close(imp_dbh->conn, lobj_fd);
		if (ret < 0) {
				pg_error(sth, -1, PQerrorMessage(imp_dbh->conn));
				return 0;
		}

		/* execute end 
		result = PQexec(imp_dbh->conn, "end");
		status = result ? PQresultStatus(result) : -1;
		PQclear(result);
		if (status != PGRES_COMMAND_OK) {
				pg_error(sth, status, PQerrorMessage(imp_dbh->conn));
				return 0;
		}
		*/

		return nread;
}


/* end of large_object.c */

