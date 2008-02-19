# -*-cperl-*-
#  $Id$
#
#  Copyright (c) 2002-2008 Greg Sabino Mullane and others: see the Changes file
#  Portions Copyright (c) 2002 Jeffrey W. Baker
#  Portions Copyright (c) 1997-2001 Edmund Mergl
#  Portions Copyright (c) 1994-1997 Tim Bunce
#
#  You may distribute under the terms of either the GNU General Public
#  License or the Artistic License, as specified in the Perl README file.


use strict;
use warnings;
use 5.006001;

{
	package DBD::Pg;

	use version; our $VERSION = qv("2.1.0");

	use DBI ();
	use DynaLoader ();
	use Exporter ();
	use vars qw(@ISA %EXPORT_TAGS $err $errstr $sqlstate $drh $dbh $DBDPG_DEFAULT @EXPORT);
	@ISA = qw(DynaLoader Exporter);


	%EXPORT_TAGS =
		(
		 async => [qw(PG_ASYNC PG_OLDQUERY_CANCEL PG_OLDQUERY_WAIT)],
		 pg_types => [qw(
			PG_ABSTIME PG_ABSTIMEARRAY PG_ACLITEM PG_ACLITEMARRAY PG_ANY
			PG_ANYARRAY PG_ANYELEMENT PG_ANYENUM PG_ANYNONARRAY PG_BIT
			PG_BITARRAY PG_BOOL PG_BOOLARRAY PG_BOX PG_BOXARRAY
			PG_BPCHAR PG_BPCHARARRAY PG_BYTEA PG_BYTEAARRAY PG_CHAR
			PG_CHARARRAY PG_CID PG_CIDARRAY PG_CIDR PG_CIDRARRAY
			PG_CIRCLE PG_CIRCLEARRAY PG_CSTRING PG_CSTRINGARRAY PG_DATE
			PG_DATEARRAY PG_FLOAT4 PG_FLOAT4ARRAY PG_FLOAT8 PG_FLOAT8ARRAY
			PG_GTSVECTOR PG_GTSVECTORARRAY PG_INET PG_INETARRAY PG_INT2
			PG_INT2ARRAY PG_INT2VECTOR PG_INT2VECTORARRAY PG_INT4 PG_INT4ARRAY
			PG_INT8 PG_INT8ARRAY PG_INTERNAL PG_INTERVAL PG_INTERVALARRAY
			PG_LANGUAGE_HANDLER PG_LINE PG_LINEARRAY PG_LSEG PG_LSEGARRAY
			PG_MACADDR PG_MACADDRARRAY PG_MONEY PG_MONEYARRAY PG_NAME
			PG_NAMEARRAY PG_NUMERIC PG_NUMERICARRAY PG_OID PG_OIDARRAY
			PG_OIDVECTOR PG_OIDVECTORARRAY PG_OPAQUE PG_PATH PG_PATHARRAY
			PG_PG_ATTRIBUTE PG_PG_CLASS PG_PG_PROC PG_PG_TYPE PG_POINT
			PG_POINTARRAY PG_POLYGON PG_POLYGONARRAY PG_RECORD PG_REFCURSOR
			PG_REFCURSORARRAY PG_REGCLASS PG_REGCLASSARRAY PG_REGCONFIG PG_REGCONFIGARRAY
			PG_REGDICTIONARY PG_REGDICTIONARYARRAY PG_REGOPER PG_REGOPERARRAY PG_REGOPERATOR
			PG_REGOPERATORARRAY PG_REGPROC PG_REGPROCARRAY PG_REGPROCEDURE PG_REGPROCEDUREARRAY
			PG_REGTYPE PG_REGTYPEARRAY PG_RELTIME PG_RELTIMEARRAY PG_SMGR
			PG_TEXT PG_TEXTARRAY PG_TID PG_TIDARRAY PG_TIME
			PG_TIMEARRAY PG_TIMESTAMP PG_TIMESTAMPARRAY PG_TIMESTAMPTZ PG_TIMESTAMPTZARRAY
			PG_TIMETZ PG_TIMETZARRAY PG_TINTERVAL PG_TINTERVALARRAY PG_TRIGGER
			PG_TSQUERY PG_TSQUERYARRAY PG_TSVECTOR PG_TSVECTORARRAY PG_TXID_SNAPSHOT
			PG_TXID_SNAPSHOTARRAY PG_UNKNOWN PG_UUID PG_UUIDARRAY PG_VARBIT
			PG_VARBITARRAY PG_VARCHAR PG_VARCHARARRAY PG_VOID PG_XID
			PG_XIDARRAY PG_XML PG_XMLARRAY
		)]
	);

	{
		package DBD::Pg::DefaultValue;
		sub new { my $self = {}; return bless $self, shift; }
	}
	$DBDPG_DEFAULT = DBD::Pg::DefaultValue->new();
	Exporter::export_ok_tags('pg_types', 'async');
	@EXPORT = qw($DBDPG_DEFAULT PG_ASYNC PG_OLDQUERY_CANCEL PG_OLDQUERY_WAIT PG_BYTEA);

	require_version DBI 1.52;

	bootstrap DBD::Pg $VERSION;

	$err = 0;       # holds error code for DBI::err
	$errstr = "";   # holds error string for DBI::errstr
	$sqlstate = ""; # holds five character SQLSTATE code
	$drh = undef;   # holds driver handle once initialized

	sub CLONE {
		$drh = undef;
		return;
	}

	## Deprecated
	sub _pg_use_catalog {
		return 'pg_catalog.';
	}

	sub driver {
		return $drh if defined $drh;
		my($class, $attr) = @_;

		$class .= "::dr";

		$drh = DBI::_new_drh($class, {
			'Name'        => 'Pg',
			'Version'     => $VERSION,
			'Err'         => \$DBD::Pg::err,
			'Errstr'      => \$DBD::Pg::errstr,
			'State'       => \$DBD::Pg::sqlstate,
			'Attribution' => "DBD::Pg $VERSION by Greg Sabino Mullane and others",
		});


		DBD::Pg::db->install_method("pg_cancel");
		DBD::Pg::db->install_method("pg_endcopy");
		DBD::Pg::db->install_method("pg_getline");
		DBD::Pg::db->install_method("pg_getcopydata");
		DBD::Pg::db->install_method("pg_getcopydata_async");
		DBD::Pg::db->install_method("pg_putcopydata");
		DBD::Pg::db->install_method("pg_putcopyend");
		DBD::Pg::db->install_method("pg_ping");
		DBD::Pg::db->install_method("pg_putline");
		DBD::Pg::db->install_method("pg_ready");
		DBD::Pg::db->install_method("pg_release");
		DBD::Pg::db->install_method("pg_result");
		DBD::Pg::db->install_method("pg_rollback_to");
		DBD::Pg::db->install_method("pg_savepoint");
		DBD::Pg::db->install_method("pg_server_trace");
		DBD::Pg::db->install_method("pg_server_untrace");
		DBD::Pg::db->install_method("pg_type_info");

		DBD::Pg::st->install_method("pg_cancel");
		DBD::Pg::st->install_method("pg_result");
		DBD::Pg::st->install_method("pg_ready");

		return $drh;

	} ## end of driver


	1;

} ## end of package DBD::Pg


{
	package DBD::Pg::dr;

	use strict;

	## Returns an array of formatted database names from the pg_database table
	sub data_sources {

		my $drh = shift;
		my $attr = shift || '';
		## Future: connect to "postgres" when the minimum version we support is 8.0
		my $connstring = 'dbname=template1';
		if ($ENV{DBI_DSN}) {
			($connstring = $ENV{DBI_DSN}) =~ s/dbi:Pg://;
		}
		if (length $attr) {
			$connstring .= ";$attr";
		}
		my $dbh = DBD::Pg::dr::connect($drh, $connstring) or return undef;
		$dbh->{AutoCommit}=1;
		my $SQL = "SELECT pg_catalog.quote_ident(datname) FROM pg_catalog.pg_database ORDER BY 1";
		my $sth = $dbh->prepare($SQL);
		$sth->execute() or die $DBI::errstr;
		my @sources = map { "dbi:Pg:dbname=$_->[0]" } @{$sth->fetchall_arrayref()};
		$dbh->disconnect;
		return @sources;
	}


	sub connect { ## no critic (ProhibitBuiltinHomonyms)
		my ($drh, $dbname, $user, $pass)= @_;

		## Allow "db" and "database" as synonyms for "dbname"
		$dbname =~ s/\b(?:db|database)\s*=/dbname=/;

		my $name = $dbname;
		if ($dbname =~ m{dbname\s*=\s*[\"\']([^\"\']+)}) {
			$name = "'$1'";
			$dbname =~ s/\"/\'/g;
		}
		elsif ($dbname =~ m{dbname\s*=\s*([^;]+)}) {
			$name = $1;
		}

 		$user = defined($user) ? $user : defined $ENV{DBI_USER} ? $ENV{DBI_USER} : "";
		$pass = defined($pass) ? $pass : defined $ENV{DBI_PASS} ? $ENV{DBI_PASS} : "";

		my ($dbh) = DBI::_new_dbh($drh, {
			'Name'         => $dbname,
			'Username'     => $user,
			'CURRENT_USER' => $user,
		 });

		# Connect to the database..
		DBD::Pg::db::_login($dbh, $dbname, $user, $pass) or return undef;

		my $version = $dbh->{pg_server_version};
		$dbh->{private_dbdpg}{version} = $version;

		return $dbh;
	}

	sub private_attribute_info {
		return {
		};
	}

} ## end of package DBD::Pg::dr


{
	package DBD::Pg::db;

	use DBI qw(:sql_types);

	use strict;

	sub prepare {
		my($dbh, $statement, @attribs) = @_;

		return undef if ! defined $statement;

		# Create a 'blank' statement handle:
		my $sth = DBI::_new_sth($dbh, {
			'Statement' => $statement,
		});

		DBD::Pg::st::_prepare($sth, $statement, @attribs) || 0;

		return $sth;
	}

	sub last_insert_id {

		my ($dbh, $catalog, $schema, $table, $col, $attr) = @_;

		## Our ultimate goal is to get a sequence
		my ($sth, $count, $SQL, $sequence);

		## Cache all of our table lookups? Default is yes
		my $cache = 1;

		## Catalog and col are not used
		$schema = '' if ! defined $schema;
		$table = '' if ! defined $table;
		my $cachename = "lii$table$schema";

		if (defined $attr and length $attr) {
			## If not a hash, assume it is a sequence name
			if (! ref $attr) {
				$attr = {sequence => $attr};
			}
			elsif (ref $attr ne 'HASH') {
				$dbh->set_err(1, "last_insert_id must be passed a hashref as the final argument");
				return undef;
			}
			## Named sequence overrides any table or schema settings
			if (exists $attr->{sequence} and length $attr->{sequence}) {
				$sequence = $attr->{sequence};
			}
			if (exists $attr->{pg_cache}) {
				$cache = $attr->{pg_cache};
			}
		}

		if (! defined $sequence and exists $dbh->{private_dbdpg}{$cachename} and $cache) {
			$sequence = $dbh->{private_dbdpg}{$cachename};
		}
		elsif (! defined $sequence) {
			## At this point, we must have a valid table name
			if (! length $table) {
				$dbh->set_err(1, "last_insert_id needs at least a sequence or table name");
				return undef;
			}
			my @args = ($table);
			## Make sure the table in question exists and grab its oid
			my ($schemajoin,$schemawhere) = ('','');
			if (length $schema) {
				$schemajoin = "\n JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)";
				$schemawhere = "\n AND n.nspname = ?";
				push @args, $schema;
			}
			$SQL = "SELECT c.oid FROM pg_catalog.pg_class c $schemajoin\n WHERE relname = ?$schemawhere";
			if (! length $schema) {
				$SQL .= " AND pg_catalog.pg_table_is_visible(c.oid)";
			}
			$sth = $dbh->prepare_cached($SQL);
			$count = $sth->execute(@args);
			if (!defined $count or $count eq '0E0') {
				$sth->finish();
				my $message = qq{Could not find the table "$table"};
				length $schema and $message .= qq{ in the schema "$schema"};
				$dbh->set_err(1, $message);
				return undef;
			}
			my $oid = $sth->fetchall_arrayref()->[0][0];
			$oid =~ /(\d+)/ or die qq{OID was not numeric?!?\n};
			$oid = $1;
			## This table has a primary key. Is there a sequence associated with it via a unique, indexed column?
			$SQL = "SELECT a.attname, i.indisprimary, pg_catalog.pg_get_expr(adbin,adrelid)\n".
				"FROM pg_catalog.pg_index i, pg_catalog.pg_attribute a, pg_catalog.pg_attrdef d\n ".
				"WHERE i.indrelid = $oid AND d.adrelid=a.attrelid AND d.adnum=a.attnum\n".
				"  AND a.attrelid = $oid AND i.indisunique IS TRUE\n".
				"  AND a.atthasdef IS TRUE AND i.indkey[0]=a.attnum\n".
				" AND d.adsrc ~ '^nextval'";
			$sth = $dbh->prepare($SQL);
			$count = $sth->execute();
			if (!defined $count or $count eq '0E0') {
				$sth->finish();
				$dbh->set_err(1, qq{No suitable column found for last_insert_id of table "$table"});
				return undef;
			}
			my $info = $sth->fetchall_arrayref();

			## We have at least one with a default value. See if we can determine sequences
			my @def;
			for (@$info) {
				next unless $_->[2] =~ /^nextval\(+'([^']+)'::/o;
				push @$_, $1;
				push @def, $_;
			}
			if (!@def) {
				$dbh->set_err(1, qq{No suitable column found for last_insert_id of table "$table"\n});
			}
			## Tiebreaker goes to the primary keys
			if (@def > 1) {
				my @pri = grep { $_->[1] } @def;
				if (1 != @pri) {
					$dbh->set_err(1, qq{No suitable column found for last_insert_id of table "$table"\n});
				}
				@def = @pri;
			}
			$sequence = $def[0]->[3];
			## Cache this information for subsequent calls
			$dbh->{private_dbdpg}{$cachename} = $sequence;
		}

		$sth = $dbh->prepare_cached("SELECT currval(?)");
		$count = $sth->execute($sequence);
		return undef if ! defined $count;
		return $sth->fetchall_arrayref()->[0][0];

	} ## end of last_insert_id

	sub ping {
		my $dbh = shift;
		local $SIG{__WARN__} = sub { } if $dbh->{PrintError};
		local $dbh->{RaiseError} = 0 if $dbh->{RaiseError};
		my $ret = DBD::Pg::db::_ping($dbh);
		return $ret < 1 ? 0 : $ret;
	}

	sub pg_ping {
		my $dbh = shift;
		local $SIG{__WARN__} = sub { } if $dbh->{PrintError};
		local $dbh->{RaiseError} = 0 if $dbh->{RaiseError};
		return DBD::Pg::db::_ping($dbh);
	}

	sub pg_type_info {
		my($dbh,$pg_type) = @_;
		local $SIG{__WARN__} = sub { } if $dbh->{PrintError};
		local $dbh->{RaiseError} = 0 if $dbh->{RaiseError};
		my $ret = DBD::Pg::db::_pg_type_info($pg_type);
		return $ret;
	}

	# Column expected in statement handle returned.
	# table_cat, table_schem, table_name, column_name, data_type, type_name,
 	# column_size, buffer_length, DECIMAL_DIGITS, NUM_PREC_RADIX, NULLABLE,
	# REMARKS, COLUMN_DEF, SQL_DATA_TYPE, SQL_DATETIME_SUB, CHAR_OCTET_LENGTH,
	# ORDINAL_POSITION, IS_NULLABLE
	# The result set is ordered by TABLE_SCHEM, TABLE_NAME and ORDINAL_POSITION.

	sub column_info {
		my $dbh = shift;
		my ($catalog, $schema, $table, $column) = @_;

		my @search;
		## If the schema or table has an underscore or a %, use a LIKE comparison
		if (defined $schema and length $schema) {
			push @search, "n.nspname " . ($schema =~ /[_%]/ ? "LIKE " : "= ") .
				$dbh->quote($schema);
		}
		if (defined $table and length $table) {
			push @search, "c.relname " . ($table =~ /[_%]/ ? "LIKE " : "= ") .
				$dbh->quote($table);
		}
		if (defined $column and length $column) {
			push @search, "a.attname " . ($column =~ /[_%]/ ? "LIKE " : "= ") .
				$dbh->quote($column);
		}

		my $whereclause = join "\n\t\t\t\tAND ", "", @search;

		my $schemajoin = "JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)";

		my $remarks = "pg_catalog.col_description(a.attrelid, a.attnum)";

		my $col_info_sql = qq!
			SELECT
				NULL::text AS "TABLE_CAT"
				, quote_ident(n.nspname) AS "TABLE_SCHEM"
				, quote_ident(c.relname) AS "TABLE_NAME"
				, quote_ident(a.attname) AS "COLUMN_NAME"
				, a.atttypid AS "DATA_TYPE"
				, pg_catalog.format_type(a.atttypid, NULL) AS "TYPE_NAME"
				, a.attlen AS "COLUMN_SIZE"
				, NULL::text AS "BUFFER_LENGTH"
				, NULL::text AS "DECIMAL_DIGITS"
				, NULL::text AS "NUM_PREC_RADIX"
				, CASE a.attnotnull WHEN 't' THEN 0 ELSE 1 END AS "NULLABLE"
				, $remarks AS "REMARKS"
				, af.adsrc AS "COLUMN_DEF"
				, NULL::text AS "SQL_DATA_TYPE"
				, NULL::text AS "SQL_DATETIME_SUB"
				, NULL::text AS "CHAR_OCTET_LENGTH"
				, a.attnum AS "ORDINAL_POSITION"
				, CASE a.attnotnull WHEN 't' THEN 'NO' ELSE 'YES' END AS "IS_NULLABLE"
				, pg_catalog.format_type(a.atttypid, a.atttypmod) AS "pg_type"
				, '?' AS "pg_constraint"
				, n.nspname AS "pg_schema"
				, c.relname AS "pg_table"
				, a.attname AS "pg_column"
				, a.attrelid AS "pg_attrelid"
				, a.attnum AS "pg_attnum"
				, a.atttypmod AS "pg_atttypmod"
			FROM
				pg_catalog.pg_type t
				JOIN pg_catalog.pg_attribute a ON (t.oid = a.atttypid)
				JOIN pg_catalog.pg_class c ON (a.attrelid = c.oid)
				LEFT JOIN pg_catalog.pg_attrdef af ON (a.attnum = af.adnum AND a.attrelid = af.adrelid)
				$schemajoin
			WHERE
				a.attnum >= 0
				AND c.relkind IN ('r','v')
				$whereclause
			ORDER BY "TABLE_SCHEM", "TABLE_NAME", "ORDINAL_POSITION"
			!;

		my $data = $dbh->selectall_arrayref($col_info_sql) or return undef;

		# To turn the data back into a statement handle, we need 
		# to fetch the data as an array of arrays, and also have a
		# a matching array of all the column names
		my %col_map = (qw/
			TABLE_CAT             0
			TABLE_SCHEM           1
			TABLE_NAME            2
			COLUMN_NAME           3
			DATA_TYPE             4
			TYPE_NAME             5
			COLUMN_SIZE           6
			BUFFER_LENGTH         7
			DECIMAL_DIGITS        8
			NUM_PREC_RADIX        9
			NULLABLE             10
			REMARKS              11
			COLUMN_DEF           12
			SQL_DATA_TYPE        13
			SQL_DATETIME_SUB     14
			CHAR_OCTET_LENGTH    15
			ORDINAL_POSITION     16
			IS_NULLABLE          17
			pg_type              18
			pg_constraint        19
			pg_schema            20
			pg_table             21
			pg_column            22
			/);

		my $oldconstraint_sth;

		for my $row (@$data) {
			my $typmod = pop @$row;
			my $attnum = pop @$row;
			my $aid = pop @$row;

			$row->[$col_map{COLUMN_SIZE}] =
 				_calc_col_size($typmod,$row->[$col_map{COLUMN_SIZE}]);

			# Replace the Pg type with the SQL_ type
			my $w = $row->[$col_map{DATA_TYPE}];
			$row->[$col_map{DATA_TYPE}] = DBD::Pg::db::pg_type_info($dbh,$row->[$col_map{DATA_TYPE}]);
			$w = $row->[$col_map{DATA_TYPE}];

			# Add pg_constraint
			my $SQL = "SELECT consrc FROM pg_catalog.pg_constraint WHERE contype = 'c' AND ".
				"conrelid = $aid AND conkey = '{$attnum}'";
			my $info = $dbh->selectall_arrayref($SQL);
			if (@$info) {
				$row->[19] = $info->[0][0];
			}
			else {
				$row->[19] = undef;
			}
			$col_map{pg_constraint} = 19;
		}

		# get rid of atttypmod that we no longer need
		delete $col_map{pg_atttypmod};

		# Since we've processed the data in Perl, we have to jump through a hoop
		# To turn it back into a statement handle
		#
		return _prepare_from_data
			(
			 'column_info',
			 $data,
			 [ sort { $col_map{$a} <=> $col_map{$b} } keys %col_map]
			 );
	}

	sub _prepare_from_data {
		my ($statement, $data, $names, %attr) = @_;
		my $sponge = DBI->connect("dbi:Sponge:","","",{ RaiseError => 1 });
		my $sth = $sponge->prepare($statement, { rows=>$data, NAME=>$names, %attr });
		return $sth;
	}

	sub statistics_info {

		my $dbh = shift;
		my ($catalog, $schema, $table, $unique_only, $quick, $attr) = @_;

		## Catalog is ignored, but table is mandatory
		return undef unless defined $table and length $table;

		my $schema_where = '';
		my @exe_args = ($table);

		my $input_schema = (defined $schema and length $schema) ? 1 : 0;

		if ($input_schema) {
			$schema_where = 'AND n.nspname = ? AND n.oid = d.relnamespace';
			push(@exe_args, $schema);
		}
		else {
			$schema_where = 'AND n.oid = d.relnamespace';
		}

		my $table_stats_sql = qq{
			SELECT d.relpages, d.reltuples, n.nspname
			FROM   pg_catalog.pg_class d, pg_catalog.pg_namespace n
			WHERE  d.relname = ? $schema_where
		};

		my $colnames_sql = qq{
			SELECT
				a.attnum, a.attname
			FROM
				pg_catalog.pg_attribute a, pg_catalog.pg_class d, pg_catalog.pg_namespace n
			WHERE
				a.attrelid = d.oid AND d.relname = ? $schema_where
		};

		my $stats_sql = qq{
			SELECT
				c.relname, i.indkey, i.indisunique, i.indisclustered, a.amname,
				n.nspname, c.relpages, c.reltuples, i.indexprs,
				pg_get_expr(i.indpred,i.indrelid) as predicate
			FROM
				pg_catalog.pg_index i, pg_catalog.pg_class c,
				pg_catalog.pg_class d, pg_catalog.pg_am a,
				pg_catalog.pg_namespace n
			WHERE
				d.relname = ? $schema_where AND d.oid = i.indrelid
				AND i.indexrelid = c.oid AND c.relam = a.oid
			ORDER BY
				i.indisunique desc, a.amname, c.relname
		};

		my @output_rows;

		# Table-level stats
		if (!$unique_only) {
			my $table_stats_sth = $dbh->prepare($table_stats_sql);
			$table_stats_sth->execute(@exe_args) or return undef;
			my $tst = $table_stats_sth->fetchrow_hashref or return undef;
			push(@output_rows, [
				undef,            # TABLE_CAT
				$tst->{nspname},  # TABLE_SCHEM
				$table,           # TABLE_NAME
				undef,            # NON_UNIQUE
				undef,            # INDEX_QUALIFIER
				undef,            # INDEX_NAME
				'table',          # TYPE
				undef,            # ORDINAL_POSITION
				undef,            # COLUMN_NAME
				undef,            # ASC_OR_DESC
				$tst->{reltuples},# CARDINALITY
				$tst->{relpages}, # PAGES
				undef,            # FILTER_CONDITION
			]);
		}

		# Fetch the column names for later use
		my $colnames_sth = $dbh->prepare($colnames_sql);
		$colnames_sth->execute(@exe_args) or return undef;
		my $colnames = $colnames_sth->fetchall_hashref('attnum');

		# Fetch the index definitions
		my $sth = $dbh->prepare($stats_sql);
		$sth->execute(@exe_args) or return undef;

		STAT_ROW:
		while (my $row = $sth->fetchrow_hashref) {
			next if $row->{indexprs}; # We can't return these accurately via this interface ...
			next if $unique_only and !$row->{indisunique};

			my $indtype = $row->{indisclustered}
				? 'clustered'
				: ( $row->{amname} eq 'btree' )
					? 'btree'
					: ($row->{amname} eq 'hash' )
						? 'hashed' : 'other';

			my $nonunique = $row->{indisunique} ? 0 : 1;

			my @index_row = (
				undef,             # TABLE_CAT
				$row->{nspname},   # TABLE_SCHEM
				$table,            # TABLE_NAME
				$nonunique,        # NON_UNIQUE
				undef,             # INDEX_QUALIFIER
				$row->{relname},   # INDEX_NAME
				$indtype,          # TYPE
				undef,             # ORDINAL_POSITION
				undef,             # COLUMN_NAME
				'A',               # ASC_OR_DESC
				$row->{reltuples}, # CARDINALITY
				$row->{relpages},  # PAGES
				$row->{predicate}, # FILTER_CONDITION
			);

			my $col_nums = $row->{indkey};
			$col_nums =~ s/^\s+//;
			my @col_nums = split(/\s+/, $col_nums);

			my $ord_pos = 1;
			for my $col_num (@col_nums) {
				my @copy = @index_row;
				$copy[7] = $ord_pos++; # ORDINAL_POSITION
				$copy[8] = $colnames->{$col_num}->{attname}; # COLUMN_NAME
				push(@output_rows, \@copy);
			}
		}

		my @output_colnames = qw/ TABLE_CAT TABLE_SCHEM TABLE_NAME NON_UNIQUE INDEX_QUALIFIER
					INDEX_NAME TYPE ORDINAL_POSITION COLUMN_NAME ASC_OR_DESC
					CARDINALITY PAGES FILTER_CONDITION /;

		return _prepare_from_data('statistics_info', \@output_rows, \@output_colnames);
	}

	sub primary_key_info {

		my $dbh = shift;
		my ($catalog, $schema, $table, $attr) = @_;

		## Catalog is ignored, but table is mandatory
		return undef unless defined $table and length $table;

		my $whereclause = "AND c.relname = " . $dbh->quote($table);

		if (defined $schema and length $schema) {
			$whereclause .= "\n\t\t\tAND n.nspname = " . $dbh->quote($schema);
		}

		my $TSJOIN = 'pg_catalog.pg_tablespace t ON (t.oid = c.reltablespace)';
		if ($dbh->{private_dbdpg}{version} < 80000) {
			$TSJOIN = '(SELECT 0 AS oid, 0 AS spcname, 0 AS spclocation LIMIT 0) AS t ON (t.oid=1)';
		}

		my $pri_key_sql = qq{
			SELECT
				  c.oid
				, quote_ident(n.nspname)
				, quote_ident(c.relname)
				, quote_ident(c2.relname)
				, i.indkey, quote_ident(t.spcname), quote_ident(t.spclocation)
				, n.nspname, c.relname, c2.relname
			FROM
				pg_catalog.pg_class c
				JOIN pg_catalog.pg_index i ON (i.indrelid = c.oid)
				JOIN pg_catalog.pg_class c2 ON (c2.oid = i.indexrelid)
				LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
				LEFT JOIN $TSJOIN
			WHERE
				i.indisprimary IS TRUE
			$whereclause
		};

		my $sth = $dbh->prepare($pri_key_sql) or return undef;
		$sth->execute();
		my $info = $sth->fetchall_arrayref()->[0];
		return undef if ! defined $info;

		# Get the attribute information
		my $indkey = join ',', split /\s+/, $info->[4];
		my $sql = qq{
			SELECT a.attnum, pg_catalog.quote_ident(a.attname) AS colname,
				pg_catalog.quote_ident(t.typname) AS typename
			FROM pg_catalog.pg_attribute a, pg_catalog.pg_type t
			WHERE a.attrelid = '$info->[0]'
			AND a.atttypid = t.oid
			AND attnum IN ($indkey);
		};
		$sth = $dbh->prepare($sql) or return undef;
		$sth->execute();
		my $attribs = $sth->fetchall_hashref('attnum');

		my $pkinfo = [];

		## Normal way: complete "row" per column in the primary key
		if (!exists $attr->{'pg_onerow'}) {
			my $x=0;
			my @key_seq = split/\s+/, $info->[4];
			for (@key_seq) {
				# TABLE_CAT
				$pkinfo->[$x][0] = undef;
				# SCHEMA_NAME
				$pkinfo->[$x][1] = $info->[1];
				# TABLE_NAME
				$pkinfo->[$x][2] = $info->[2];
				# COLUMN_NAME
				$pkinfo->[$x][3] = $attribs->{$_}{colname};
				# KEY_SEQ
				$pkinfo->[$x][4] = $_;
				# PK_NAME
				$pkinfo->[$x][5] = $info->[3];
				# DATA_TYPE
				$pkinfo->[$x][6] = $attribs->{$_}{typename};
				$pkinfo->[$x][7] = $info->[5];
				$pkinfo->[$x][8] = $info->[6];
				$pkinfo->[$x][9] = $info->[7];
				$pkinfo->[$x][10] = $info->[8];
				$pkinfo->[$x][11] = $info->[9];
				$x++;
			}
		}
		else { ## Nicer way: return only one row

			# TABLE_CAT
			$info->[0] = undef;
			# TABLESPACES
			$info->[7] = $info->[5];
			$info->[8] = $info->[6];
			# Unquoted names
			$info->[9] = $info->[7];
			$info->[10] = $info->[8];
			$info->[11] = $info->[9];
			# PK_NAME
			$info->[5] = $info->[3];
			# COLUMN_NAME
			$info->[3] = 2==$attr->{'pg_onerow'} ?
				[ map { $attribs->{$_}{colname} } split /\s+/, $info->[4] ] :
					join ', ', map { $attribs->{$_}{colname} } split /\s+/, $info->[4];
			# DATA_TYPE
			$info->[6] = 2==$attr->{'pg_onerow'} ?
				[ map { $attribs->{$_}{typename} } split /\s+/, $info->[4] ] :
					join ', ', map { $attribs->{$_}{typename} } split /\s+/, $info->[4];
			# KEY_SEQ
			$info->[4] = 2==$attr->{'pg_onerow'} ?
				[ split /\s+/, $info->[4] ] :
					join ', ', split /\s+/, $info->[4];

			$pkinfo = [$info];
		}

		my @cols = (qw(TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME
									 KEY_SEQ PK_NAME DATA_TYPE));
		push @cols, 'pg_tablespace_name', 'pg_tablespace_location';
		push @cols, 'pg_schema', 'pg_table', 'pg_column';

		return _prepare_from_data('primary_key_info', $pkinfo, \@cols);

	}

	sub primary_key {
		my $sth = primary_key_info(@_[0..3], {pg_onerow => 2});
		return defined $sth ? @{$sth->fetchall_arrayref()->[0][3]} : ();
	}


	sub foreign_key_info {

		my $dbh = shift;

		## PK: catalog, schema, table, FK: catalog, schema, table, attr

		## Each of these may be undef or empty
		my $pschema = $_[1] || '';
		my $ptable = $_[2] || '';
		my $fschema = $_[4] || '';
		my $ftable = $_[5] || '';
		my $args = $_[6];

		## No way to currently specify it, but we are ready when there is
		my $odbc = 0;

		## Must have at least one named table
		return undef if !$ptable and !$ftable;

		## If only the primary table is given, we return only those columns
		## that are used as foreign keys, even if that means that we return
		## unique keys but not primary one. We also return all the foreign
		## tables/columns that are referencing them, of course.

		## The first step is to find the oid of each specific table in the args:
		## Return undef if no matching relation found
		my %oid;
		for ([$ptable, $pschema, 'P'], [$ftable, $fschema, 'F']) {
			if (length $_->[0]) {
				my $SQL = "SELECT c.oid AS schema FROM pg_catalog.pg_class c, pg_catalog.pg_namespace n\n".
					"WHERE c.relnamespace = n.oid AND c.relname = " . $dbh->quote($_->[0]);
				if (length $_->[1]) {
					$SQL .= " AND n.nspname = " . $dbh->quote($_->[1]);
				}
				my $info = $dbh->selectall_arrayref($SQL);
				return undef if ! @$info;
				$oid{$_->[2]} = $info->[0][0];
			}
		}

		## We now need information about each constraint we care about.
		## Foreign table: only 'f' / Primary table: only 'p' or 'u'
		my $WHERE = $odbc ? "((contype = 'p'" : "((contype IN ('p','u')";
		if (length $ptable) {
			$WHERE .= " AND conrelid=$oid{'P'}::oid";
		}
		else {
			$WHERE .= " AND conrelid IN (SELECT DISTINCT confrelid FROM pg_catalog.pg_constraint WHERE conrelid=$oid{'F'}::oid)";
			if (length $pschema) {
				$WHERE .= " AND n2.nspname = " . $dbh->quote($pschema);
			}
		}

		$WHERE .= ")\n \t\t\t\tOR \n \t\t\t\t(contype = 'f'";
		if (length $ftable) {
			$WHERE .= " AND conrelid=$oid{'F'}::oid";
			if (length $ptable) {
				$WHERE .= " AND confrelid=$oid{'P'}::oid";
			}
		}
		else {
			$WHERE .= " AND confrelid = $oid{'P'}::oid";
			if (length $fschema) {
				$WHERE .= " AND n2.nspname = " . $dbh->quote($fschema);
			}
		}
		$WHERE .= "))";

		## Grab everything except specific column names:
		my $fk_sql = qq{
		SELECT conrelid, confrelid, contype, conkey, confkey,
			pg_catalog.quote_ident(c.relname) AS t_name, pg_catalog.quote_ident(n2.nspname) AS t_schema,
			pg_catalog.quote_ident(n.nspname) AS c_schema, pg_catalog.quote_ident(conname) AS c_name,
			CASE
				WHEN confupdtype = 'c' THEN 0
				WHEN confupdtype = 'r' THEN 1
				WHEN confupdtype = 'n' THEN 2
				WHEN confupdtype = 'a' THEN 3
				WHEN confupdtype = 'd' THEN 4
				ELSE -1
			END AS update,
			CASE
				WHEN confdeltype = 'c' THEN 0
				WHEN confdeltype = 'r' THEN 1
				WHEN confdeltype = 'n' THEN 2
				WHEN confdeltype = 'a' THEN 3
				WHEN confdeltype = 'd' THEN 4
				ELSE -1
			END AS delete,
			CASE
				WHEN condeferrable = 'f' THEN 7
				WHEN condeferred = 't' THEN 6
				WHEN condeferred = 'f' THEN 5
				ELSE -1
			END AS defer
			FROM pg_catalog.pg_constraint k, pg_catalog.pg_class c, pg_catalog.pg_namespace n, pg_catalog.pg_namespace n2
			WHERE $WHERE
				AND k.connamespace = n.oid
				AND k.conrelid = c.oid
				AND c.relnamespace = n2.oid
				ORDER BY conrelid ASC
				};

		my $sth = $dbh->prepare($fk_sql);
		$sth->execute();
		my $info = $sth->fetchall_arrayref({});
		return undef if ! defined $info or ! @$info;

		## Return undef if just ptable given but no fk found
		return undef if ! length $ftable and ! grep { $_->{'contype'} eq 'f'} @$info;

		## Figure out which columns we need information about
		my %colnum;
		for my $row (@$info) {
			for (@{$row->{'conkey'}}) {
				$colnum{$row->{'conrelid'}}{$_}++;
			}
			if ($row->{'contype'} eq 'f') {
				for (@{$row->{'confkey'}}) {
					$colnum{$row->{'confrelid'}}{$_}++;
				}
			}
		}
		## Get the information about the columns computed above
		my $SQL = qq{
			SELECT a.attrelid, a.attnum, pg_catalog.quote_ident(a.attname) AS colname, 
				pg_catalog.quote_ident(t.typname) AS typename
			FROM pg_catalog.pg_attribute a, pg_catalog.pg_type t
			WHERE a.atttypid = t.oid
			AND (\n};

		$SQL .= join "\n\t\t\t\tOR\n" => map {
			my $cols = join ',' => keys %{$colnum{$_}};
			"\t\t\t\t( a.attrelid = '$_' AND a.attnum IN ($cols) )"
		} sort keys %colnum;

		$sth = $dbh->prepare(qq{$SQL \)});
		$sth->execute();
		my $attribs = $sth->fetchall_arrayref({});

		## Make a lookup hash
		my %attinfo;
		for (@$attribs) {
			$attinfo{"$_->{'attrelid'}"}{"$_->{'attnum'}"} = $_;
		}

		## This is an array in case we have identical oid/column combos. Lowest oid wins
		my %ukey;
		for my $c (grep { $_->{'contype'} ne 'f' } @$info) {
			## Munge multi-column keys into sequential order
			my $multi = join ' ' => sort @{$c->{'conkey'}};
			push @{$ukey{$c->{'conrelid'}}{$multi}}, $c;
		}

		## Finally, return as a SQL/CLI structure:
		my $fkinfo = [];
		my $x=0;
		for my $t (sort { $a->{'c_name'} cmp $b->{'c_name'} } grep { $_->{'contype'} eq 'f' } @$info) {
			## We need to find which constraint row (if any) matches our confrelid-confkey combo
			## by checking out ukey hash. We sort for proper matching of { 1 2 } vs. { 2 1 }
			## No match means we have a pure index constraint
			my $u;
			my $multi = join ' ' => sort @{$t->{'confkey'}};
			if (exists $ukey{$t->{'confrelid'}}{$multi}) {
				$u = $ukey{$t->{'confrelid'}}{$multi}->[0];
			}
			else {
				## Mark this as an index so we can fudge things later on
				$multi = "index";
				## Grab the first one found, modify later on as needed
				$u = ((values %{$ukey{$t->{'confrelid'}}})[0]||[])->[0];
				## Bail in case there was no match
				next if ! ref $u;
			}

			## ODBC is primary keys only
			next if $odbc and ($u->{'contype'} ne 'p' or $multi eq 'index');

			my $conkey = $t->{'conkey'};
			my $confkey = $t->{'confkey'};
			for (my $y=0; $conkey->[$y]; $y++) {
				# UK_TABLE_CAT
				$fkinfo->[$x][0] = undef;
				# UK_TABLE_SCHEM
				$fkinfo->[$x][1] = $u->{'t_schema'};
				# UK_TABLE_NAME
				$fkinfo->[$x][2] = $u->{'t_name'};
				# UK_COLUMN_NAME
				$fkinfo->[$x][3] = $attinfo{$t->{'confrelid'}}{$confkey->[$y]}{'colname'};
				# FK_TABLE_CAT
				$fkinfo->[$x][4] = undef;
				# FK_TABLE_SCHEM
				$fkinfo->[$x][5] = $t->{'t_schema'};
				# FK_TABLE_NAME
				$fkinfo->[$x][6] = $t->{'t_name'};
				# FK_COLUMN_NAME
				$fkinfo->[$x][7] = $attinfo{$t->{'conrelid'}}{$conkey->[$y]}{'colname'};
				# ORDINAL_POSITION
				$fkinfo->[$x][8] = $conkey->[$y];
				# UPDATE_RULE
				$fkinfo->[$x][9] = "$t->{'update'}";
				# DELETE_RULE
				$fkinfo->[$x][10] = "$t->{'delete'}";
				# FK_NAME
				$fkinfo->[$x][11] = $t->{'c_name'};
				# UK_NAME (may be undef if an index with no named constraint)
				$fkinfo->[$x][12] = $multi eq 'index' ? undef : $u->{'c_name'};
				# DEFERRABILITY
				$fkinfo->[$x][13] = "$t->{'defer'}";
				# UNIQUE_OR_PRIMARY
				$fkinfo->[$x][14] = ($u->{'contype'} eq 'p' and $multi ne 'index') ? 'PRIMARY' : 'UNIQUE';
				# UK_DATA_TYPE
				$fkinfo->[$x][15] = $attinfo{$t->{'confrelid'}}{$confkey->[$y]}{'typename'};
				# FK_DATA_TYPE
				$fkinfo->[$x][16] = $attinfo{$t->{'conrelid'}}{$conkey->[$y]}{'typename'};
				$x++;
			} ## End each column in this foreign key
		} ## End each foreign key

		my @CLI_cols = (qw(
			UK_TABLE_CAT UK_TABLE_SCHEM UK_TABLE_NAME UK_COLUMN_NAME
			FK_TABLE_CAT FK_TABLE_SCHEM FK_TABLE_NAME FK_COLUMN_NAME
			ORDINAL_POSITION UPDATE_RULE DELETE_RULE FK_NAME UK_NAME
			DEFERABILITY UNIQUE_OR_PRIMARY UK_DATA_TYPE FK_DATA_TYPE
		));

		my @ODBC_cols = (qw(
			PKTABLE_CAT PKTABLE_SCHEM PKTABLE_NAME PKCOLUMN_NAME
			FKTABLE_CAT FKTABLE_SCHEM FKTABLE_NAME FKCOLUMN_NAME
			KEY_SEQ UPDATE_RULE DELETE_RULE FK_NAME PK_NAME
			DEFERABILITY UNIQUE_OR_PRIMARY PK_DATA_TYPE FKDATA_TYPE
		));
		return _prepare_from_data('foreign_key_info', $fkinfo, $odbc ? \@ODBC_cols : \@CLI_cols);

	}


	sub table_info {

		my $dbh = shift;
		my ($catalog, $schema, $table, $type) = @_;

		my $tbl_sql = ();

		my $extracols = q{,NULL::text AS pg_schema, NULL::text AS pg_table};
		if ( # Rule 19a
				(defined $catalog and $catalog eq '%')
				and (defined $schema and $schema eq '')
				and (defined $table and $table eq '')
			 ) {
			$tbl_sql = qq{
					SELECT
						 NULL::text AS "TABLE_CAT"
					 , NULL::text AS "TABLE_SCHEM"
					 , NULL::text AS "TABLE_NAME"
					 , NULL::text AS "TABLE_TYPE"
					 , NULL::text AS "REMARKS" $extracols
					};
		}
		elsif (# Rule 19b
					 (defined $catalog and $catalog eq '')
					 and (defined $schema and $schema eq '%')
					 and (defined $table and $table eq '')
					) {
			$extracols = q{,n.nspname AS pg_schema, NULL::text AS pg_table};
			$tbl_sql = qq{SELECT
						 NULL::text AS "TABLE_CAT"
					 , quote_ident(n.nspname) AS "TABLE_SCHEM"
					 , NULL::text AS "TABLE_NAME"
					 , NULL::text AS "TABLE_TYPE"
					 , CASE WHEN n.nspname ~ '^pg_' THEN 'system schema' ELSE 'owned by ' || pg_get_userbyid(n.nspowner) END AS "REMARKS" $extracols
					FROM pg_catalog.pg_namespace n
					ORDER BY "TABLE_SCHEM"
					};
		}
		elsif (# Rule 19c
					 (defined $catalog and $catalog eq '')
					 and (defined $schema and $schema eq '')
					 and (defined $table and $table eq '')
					 and (defined $type and $type eq '%')
					) {
			$tbl_sql = qq{
					SELECT
					   NULL::text AS "TABLE_CAT"
					 , NULL::text AS "TABLE_SCHEM"
					 , NULL::text AS "TABLE_NAME"
					 , 'TABLE'    AS "TABLE_TYPE"
					 , 'relkind: r' AS "REMARKS" $extracols
					UNION
					SELECT
					   NULL::text AS "TABLE_CAT"
					 , NULL::text AS "TABLE_SCHEM"
					 , NULL::text AS "TABLE_NAME"
					 , 'VIEW'     AS "TABLE_TYPE"
					 , 'relkind: v' AS "REMARKS" $extracols
				};
		}
		else {
			# Default SQL
			$extracols = q{,n.nspname AS pg_schema, c.relname AS pg_table};
			my @search;
			my $showtablespace = ', quote_ident(t.spcname) AS "pg_tablespace_name", quote_ident(t.spclocation) AS "pg_tablespace_location"';

			## If the schema or table has an underscore or a %, use a LIKE comparison
			if (defined $schema and length $schema) {
					push @search, "n.nspname " . ($schema =~ /[_%]/ ? "LIKE " : "= ") . $dbh->quote($schema);
			}
			if (defined $table and length $table) {
					push @search, "c.relname " . ($table =~ /[_%]/ ? "LIKE " : "= ") . $dbh->quote($table);
			}
			## All we can see is "table" or "view". Default is both
			my $typesearch = "IN ('r','v')";
			if (defined $type and length $type) {
				if ($type =~ /\btable\b/i and $type !~ /\bview\b/i) {
					$typesearch = "= 'r'";
				}
				elsif ($type =~ /\bview\b/i and $type !~ /\btable\b/i) {
					$typesearch = "= 'v'";
				}
			}
			push @search, "c.relkind $typesearch";

			my $TSJOIN = 'pg_catalog.pg_tablespace t ON (t.oid = c.reltablespace)';
			if ($dbh->{private_dbdpg}{version} < 80000) {
				$TSJOIN = '(SELECT 0 AS oid, 0 AS spcname, 0 AS spclocation LIMIT 0) AS t ON (t.oid=1)';
			}
			my $whereclause = join "\n\t\t\t\t\t AND " => @search;
			$tbl_sql = qq{
				SELECT NULL::text AS "TABLE_CAT"
					 , quote_ident(n.nspname) AS "TABLE_SCHEM"
					 , quote_ident(c.relname) AS "TABLE_NAME"
					 , CASE
					 		WHEN c.relkind = 'v' THEN
								CASE WHEN quote_ident(n.nspname) ~ '^pg_' THEN 'SYSTEM VIEW' ELSE 'VIEW' END
							ELSE
								CASE WHEN quote_ident(n.nspname) ~ '^pg_' THEN 'SYSTEM TABLE' ELSE 'TABLE' END
						END AS "TABLE_TYPE"
					 , d.description AS "REMARKS" $showtablespace $extracols
				FROM pg_catalog.pg_class AS c
					LEFT JOIN pg_catalog.pg_description AS d
						ON (c.oid = d.objoid AND c.tableoid = d.classoid AND d.objsubid = 0)
					LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)
					LEFT JOIN $TSJOIN
				WHERE $whereclause
				ORDER BY "TABLE_TYPE", "TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME"
				};
		}
		my $sth = $dbh->prepare( $tbl_sql ) or return undef;
		$sth->execute();

		return $sth;
	}

	sub tables {
			my ($dbh, @args) = @_;
			my $attr = $args[4];
			my $sth = $dbh->table_info(@args) or return;
			my $tables = $sth->fetchall_arrayref() or return;
			my @tables = map { (! (ref $attr eq "HASH" and $attr->{pg_noprefix})) ?
						"$_->[1].$_->[2]" : $_->[2] } @$tables;
			return @tables;
	}

	sub table_attributes {
		my ($dbh, $table) = @_;

		my $sth = $dbh->column_info(undef,undef,$table,undef);

		my %convert = (
			COLUMN_NAME   => 'NAME',
			DATA_TYPE     => 'TYPE',
			COLUMN_SIZE   => 'SIZE',
			NULLABLE      => 'NOTNULL',
			REMARKS       => 'REMARKS',
			COLUMN_DEF    => 'DEFAULT',
			pg_constraint => 'CONSTRAINT',
		);

		my $attrs = $sth->fetchall_arrayref(\%convert);

		for my $row (@$attrs) {
			# switch the column names
			for my $name (keys %$row) {
				$row->{ $convert{$name} } = $row->{$name};

				## Keep some original columns
				delete $row->{$name} unless ($name eq 'REMARKS' or $name eq 'NULLABLE');

			}
			# Moved check outside of loop as it was inverting the NOTNULL value for
			# attribute.
			# NOTNULL inverts the sense of NULLABLE
			$row->{NOTNULL} = ($row->{NOTNULL} ? 0 : 1);

			my @pri_keys = ();
			@pri_keys = $dbh->primary_key( undef, undef, $table );
			$row->{PRIMARY_KEY} = scalar(grep { /^$row->{NAME}$/i } @pri_keys) ? 1 : 0;
		}

		return $attrs;

	}

	sub _calc_col_size {

		my $mod = shift;
		my $size = shift;


		if ((defined $size) and ($size > 0)) {
			return $size;
		} elsif ($mod > 0xffff) {
			my $prec = ($mod & 0xffff) - 4;
			$mod >>= 16;
			my $dig = $mod;
			return "$prec,$dig";
		} elsif ($mod >= 4) {
			return $mod - 4;
		} # else {
			# $rtn = $mod;
			# $rtn = undef;
		# }

		return;
	}


	sub type_info_all {
		my ($dbh) = @_;

		my $names =
			{
			 TYPE_NAME          => 0,
			 DATA_TYPE          => 1,
			 COLUMN_SIZE        => 2,
			 LITERAL_PREFIX     => 3,
			 LITERAL_SUFFIX     => 4,
			 CREATE_PARAMS      => 5,
			 NULLABLE           => 6,
			 CASE_SENSITIVE     => 7,
			 SEARCHABLE         => 8,
			 UNSIGNED_ATTRIBUTE => 9,
			 FIXED_PREC_SCALE   => 10,
			 AUTO_UNIQUE_VALUE  => 11,
			 LOCAL_TYPE_NAME    => 12,
			 MINIMUM_SCALE      => 13,
			 MAXIMUM_SCALE      => 14,
			 SQL_DATA_TYPE      => 15,
			 SQL_DATETIME_SUB   => 16,
			 NUM_PREC_RADIX     => 17,
			 INTERVAL_PRECISION => 18,
			};

		## This list is derived from dbi_sql.h in DBI, from types.c and types.h, and from the PG docs

		## Aids to make the list more readable:
		my $GIG = 1073741824;
		my $PS = 'precision/scale';
		my $LEN = 'length';
		my $UN = undef;
		my $ti =
			[
			 $names,
# name     sql_type          size   pfx/sfx crt   n/c/s    +-/P/I   local       min max  sub rdx itvl

['unknown',  SQL_UNKNOWN_TYPE,  0,    $UN,$UN, $UN,  1,0,0, $UN,0,0, 'UNKNOWN',   $UN,$UN,
             SQL_UNKNOWN_TYPE,                                                             $UN, $UN, $UN ],
['bytea',    SQL_VARBINARY,     $GIG, "'","'", $UN,  1,0,3, $UN,0,0, 'BYTEA',     $UN,$UN,
             SQL_VARBINARY,                                                                $UN, $UN, $UN ],
['bpchar',   SQL_CHAR,          $GIG, "'","'", $LEN, 1,1,3, $UN,0,0, 'CHARACTER', $UN,$UN,
             SQL_CHAR,                                                                     $UN, $UN, $UN ],
['numeric',  SQL_DECIMAL,       1000, $UN,$UN, $PS,  1,0,2, 0,0,0, '  FLOAT',     0,1000,
             SQL_DECIMAL,                                                                  $UN, $UN, $UN ],
['numeric',  SQL_NUMERIC,       1000, $UN,$UN, $PS,  1,0,2, 0,0,0,   'FLOAT',     0,1000,
             SQL_NUMERIC,                                                                  $UN, $UN, $UN ],
['int4',     SQL_INTEGER,       10,   $UN,$UN, $UN,  1,0,2, 0,0,0,   'INTEGER',   0,0,
             SQL_INTEGER,                                                                  $UN, $UN, $UN ],
['int2',     SQL_SMALLINT,      5,    $UN,$UN, $UN,  1,0,2, 0,0,0,   'SMALLINT',  0,0,
             SQL_SMALLINT,                                                                 $UN, $UN, $UN ],
['float4',   SQL_FLOAT,         6,    $UN,$UN, $PS,  1,0,2, 0,0,0,   'FLOAT',     0,6,
             SQL_FLOAT,                                                                    $UN, $UN, $UN ],
['float8',   SQL_REAL,          15,   $UN,$UN, $PS,  1,0,2, 0,0,0,   'REAL',      0,15,
             SQL_REAL,                                                                     $UN, $UN, $UN ],
['int8',     SQL_DOUBLE,        20,   $UN,$UN, $UN,  1,0,2, 0,0,0,   'LONGINT',   0,0,
             SQL_DOUBLE,                                                                   $UN, $UN, $UN ],
['date',     SQL_DATE,          10,   "'","'", $UN,  1,0,2, $UN,0,0, 'DATE',      0,0,
             SQL_DATE,                                                                     $UN, $UN, $UN ],
['tinterval',SQL_TIME,          18,   "'","'", $UN,  1,0,2, $UN,0,0, 'TINTERVAL', 0,6,
             SQL_TIME,                                                                     $UN, $UN, $UN ],
['timestamp',SQL_TIMESTAMP,     29,   "'","'", $UN,  1,0,2, $UN,0,0, 'TIMESTAMP', 0,6,
             SQL_TIMESTAMP,                                                                $UN, $UN, $UN ],
['text',     SQL_VARCHAR,       $GIG, "'","'", $LEN, 1,1,3, $UN,0,0, 'TEXT',      $UN,$UN,
             SQL_VARCHAR,                                                                  $UN, $UN, $UN ],
['bool',     SQL_BOOLEAN,       1,    "'","'", $UN,  1,0,2, $UN,0,0, 'BOOLEAN',   $UN,$UN,
             SQL_BOOLEAN,                                                                  $UN, $UN, $UN ],
['array',    SQL_ARRAY,         1,    "'","'", $UN,  1,0,2, $UN,0,0, 'ARRAY',     $UN,$UN,
             SQL_ARRAY,                                                                    $UN, $UN, $UN ],
['date',     SQL_TYPE_DATE,     10,   "'","'", $UN,  1,0,2, $UN,0,0, 'DATE',      0,0,
             SQL_TYPE_DATE,                                                                $UN, $UN, $UN ],
['time',     SQL_TYPE_TIME,     18,   "'","'", $UN,  1,0,2, $UN,0,0, 'TIME',      0,6,
             SQL_TYPE_TIME,                                                                $UN, $UN, $UN ],
['timestamp',SQL_TYPE_TIMESTAMP,29,   "'","'", $UN,  1,0,2, $UN,0,0, 'TIMESTAMP', 0,6,
             SQL_TYPE_TIMESTAMP,                                                           $UN, $UN, $UN ],
['timetz',   SQL_TYPE_TIME_WITH_TIMEZONE,
                                29,   "'","'", $UN,  1,0,2, $UN,0,0, 'TIMETZ',    0,6,
             SQL_TYPE_TIME_WITH_TIMEZONE,                                                  $UN, $UN, $UN ],
['timestamptz',SQL_TYPE_TIMESTAMP_WITH_TIMEZONE,
                                29,   "'","'", $UN,  1,0,2, $UN,0,0, 'TIMESTAMPTZ',0,6,
             SQL_TYPE_TIMESTAMP_WITH_TIMEZONE,                                             $UN, $UN, $UN ],
		#
		# intentionally omitted: char, all geometric types, internal types
	];
	return $ti;
	}


	# Characters that need to be escaped by quote().
	my %esc = (
		"'"  => '\\047', # '\\' . sprintf("%03o", ord("'")), # ISO SQL 2
		'\\' => '\\134', # '\\' . sprintf("%03o", ord("\\")),
	);

	# Set up lookup for SQL types we don't want to escape.
	my %no_escape = map { $_ => 1 }
		DBI::SQL_INTEGER, DBI::SQL_SMALLINT, DBI::SQL_DECIMAL,
		DBI::SQL_FLOAT, DBI::SQL_REAL, DBI::SQL_DOUBLE, DBI::SQL_NUMERIC;

	sub get_info {

		my ($dbh,$type) = @_;

		return undef unless defined $type and length $type;

		my %type = (

## Driver information:

     116 => ["SQL_ACTIVE_ENVIRONMENTS",             0                         ],
   10021 => ["SQL_ASYNC_MODE",                      0                         ],
     120 => ["SQL_BATCH_ROW_COUNT",                 2                         ],
     121 => ["SQL_BATCH_SUPPORT",                   3                         ], ## ??
       2 => ["SQL_DATA_SOURCE_NAME",                "dbi:Pg:$dbh->{Name}"     ],
       3 => ["SQL_DRIVER_HDBC",                     0                         ], ## ??
     135 => ["SQL_DRIVER_HDESC",                    0                         ],
       4 => ["SQL_DRIVER_HENV",                     0                         ],
      76 => ["SQL_DRIVER_HLIB",                     0                         ],
       5 => ["SQL_DRIVER_HSTMT",                    0                         ],
       6 => ["SQL_DRIVER_NAME",                     'DBD/Pg.pm'               ],
      77 => ["SQL_DRIVER_ODBC_VERSION",             '03.00'                   ], ## ??
       7 => ["SQL_DRIVER_VER",                      'DBDVERSION'              ],
     144 => ["SQL_DYNAMIC_CURSOR_ATTRIBUTES1",      0                         ], ## ?? 519
     145 => ["SQL_DYNAMIC_CURSOR_ATTRIBUTES2",      0                         ], ## ?? 5209
      84 => ["SQL_FILE_USAGE",                      0                         ],
     146 => ["SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES1", 519                       ], ## ??
     147 => ["SQL_FORWARD_ONLY_CURSOR_ATTRIBUTES2", 5209                      ], ## ??
      81 => ["SQL_GETDATA_EXTENSIONS",              15                        ],
     149 => ["SQL_INFO_SCHEMA_VIEWS",               3932149                   ], # not: assert, charset, collat, trans
     150 => ["SQL_KEYSET_CURSOR_ATTRIBUTES1",       0                         ],
     151 => ["SQL_KEYSET_CURSOR_ATTRIBUTES2",       0                         ],
   10022 => ["SQL_MAX_ASYNC_CONCURRENT_STATEMENTS", 0                         ],
       0 => ["SQL_MAX_DRIVER_CONNECTIONS",          'MAXCONNECTIONS'          ],
     152 => ["SQL_ODBC_INTERFACE_CONFORMANCE",      1                         ], ## ??
      10 => ["SQL_ODBC_VER",                        '03.00.0000'              ], ## ??
     153 => ["SQL_PARAM_ARRAY_ROW_COUNTS",          2                         ],
     154 => ["SQL_PARAM_ARRAY_SELECTS",             3                         ],
      11 => ["SQL_ROW_UPDATES",                     'N'                       ],
      14 => ["SQL_SEARCH_PATTERN_ESCAPE",           '\\'                      ],
      13 => ["SQL_SERVER_NAME",                     'CURRENTDB'               ],
     166 => ["SQL_STANDARD_CLI_CONFORMANCE",        2                         ], ## ??
     167 => ["SQL_STATIC_CURSOR_ATTRIBUTES1",       519                       ], ## ??
     168 => ["SQL_STATIC_CURSOR_ATTRIBUTES2",       5209                      ], ## ??

## DBMS Information

      16 => ["SQL_DATABASE_NAME",                   'CURRENTDB'               ],
      17 => ["SQL_DBMS_NAME",                       'PostgreSQL'              ],
      18 => ["SQL_DBMS_VERSION",                    'ODBCVERSION'             ],

## Data source information

      20 => ["SQL_ACCESSIBLE_PROCEDURES",           "Y"                       ],
      19 => ["SQL_ACCESSIBLE_TABLES",               "Y"                       ],
      82 => ["SQL_BOOKMARK_PERSISTENCE",            0                         ],
      42 => ["SQL_CATALOG_TERM",                    ''                        ],
   10004 => ["SQL_COLLATION_SEQ",                   'ENCODING'                ], ## ??
      22 => ["SQL_CONCAT_NULL_BEHAVIOR",            0                         ],
      23 => ["SQL_CURSOR_COMMIT_BEHAVIOR",          1                         ],
      24 => ["SQL_CURSOR_ROLLBACK_BEHAVIOR",        1                         ],
   10001 => ["SQL_CURSOR_SENSITIVITY",              1                         ],
      25 => ["SQL_DATA_SOURCE_READ_ONLY",           "N"                       ],
      26 => ["SQL_DEFAULT_TXN_ISOLATION",           8                         ],
   10002 => ["SQL_DESCRIBE_PARAMETER",              "Y"                       ],
      36 => ["SQL_MULT_RESULT_SETS",                "Y"                       ],
      37 => ["SQL_MULTIPLE_ACTIVE_TXN",             "Y"                       ],
     111 => ["SQL_NEED_LONG_DATA_LEN",              "N"                       ],
      85 => ["SQL_NULL_COLLATION",                  0                         ],
      40 => ["SQL_PROCEDURE_TERM",                  "function"                ], ## for now
      39 => ["SQL_SCHEMA_TERM",                     "schema"                  ],
      44 => ["SQL_SCROLL_OPTIONS",                  8                         ], ## ??
      45 => ["SQL_TABLE_TERM",                      "table"                   ],
      46 => ["SQL_TXN_CAPABLE",                     2                         ],
      72 => ["SQL_TXN_ISOLATION_OPTION",            15                        ],
      47  => ["SQL_USER_NAME",                      $dbh->{CURRENT_USER}      ],

## Supported SQL

     169  => ["SQL_AGGREGATE_FUNCTIONS",            127                       ],
     117  => ["SQL_ALTER_DOMAIN",                   31                        ],
      86  => ["SQL_ALTER_TABLE",                    32639                     ], ## no collate
     114  => ["SQL_CATALOG_LOCATION",               0                         ],
   10003  => ["SQL_CATALOG_NAME",                   "N"                       ],
      41  => ["SQL_CATALOG_NAME_SEPARATOR",         ""                        ],
      92  => ["SQL_CATALOG_USAGE",                  0                         ],
      87  => ["SQL_COLUMN_ALIAS",                   "Y"                       ],
      74  => ["SQL_CORRELATION_NAME",               2                         ],
     127  => ["SQL_CREATE_ASSERTION",               0                         ],
     128  => ["SQL_CREATE_CHARACTER_SET",           0                         ],
     129  => ["SQL_CREATE_COLLATION",               0                         ],
     130  => ["SQL_CREATE_DOMAIN",                  23                        ],
     131  => ["SQL_CREATE_SCHEMA",                  3                         ],
     132  => ["SQL_CREATE_TABLE",                   13845                     ],
     133  => ["SQL_CREATE_TRANSLATION",             0                         ],
     134  => ["SQL_CREATE_VIEW",                    9                         ],
     119  => ["SQL_DATETIME_LITERALS",              65535                     ],
     170  => ["SQL_DDL_INDEX",                      3                         ],
     136  => ["SQL_DROP_ASSERTION",                 0                         ],
     137  => ["SQL_DROP_CHARACTER_SET",             0                         ],
     138  => ["SQL_DROP_COLLATION",                 0                         ],
     139  => ["SQL_DROP_DOMAIN",                    7                         ],
     140  => ["SQL_DROP_SCHEMA",                    7                         ],
     141  => ["SQL_DROP_TABLE",                     7                         ],
     142  => ["SQL_DROP_TRANSLATION",               0                         ],
     143  => ["SQL_DROP_VIEW",                      7                         ],
      27  => ["SQL_EXPRESSIONS_IN_ORDERBY",         "Y"                       ],
      88  => ["SQL_GROUP_BY",                       2                         ],
      28  => ["SQL_IDENTIFIER_CASE",                2                         ], ## kinda
      29  => ["SQL_IDENTIFIER_QUOTE_CHAR",          '"'                       ],
     148  => ["SQL_INDEX_KEYWORDS",                 0                         ],
     172  => ["SQL_INSERT_STATEMENT",               7                         ],
      73  => ["SQL_INTEGERITY",                     "Y"                       ], ## e.g. ON DELETE CASCADE?
      89  => ["SQL_KEYWORDS",                       'KEYWORDS'                ],
     113  => ["SQL_LIKE_ESCAPE_CLAUSE",             "Y"                       ],
      75  => ["SQL_NON_NULLABLE_COLUMNS",           1                         ],
     115  => ["SQL_OJ_CAPABILITIES",                127                       ],
      90  => ["SQL_ORDER_BY_COLUMNS_IN_SELECT",     "N"                       ],
      38  => ["SQL_OUTER_JOINS",                    "Y"                       ],
      21  => ["SQL_PROCEDURES",                     "Y"                       ],
      93  => ["SQL_QUOTED_IDENTIFIER_CASE",         3                         ],
      91  => ["SQL_SCHEMA_USAGE",                   31                        ],
      94  => ["SQL_SPECIAL_CHARACTERS",             '$'                       ],
     118  => ["SQL_SQL_CONFORMANCE",                4                         ], ## ??
      95  => ["SQL_SUBQUERIES",                     31                        ],
      96  => ["SQL_UNION",                          3                         ],

## SQL limits

     112  => ["SQL_MAX_BINARY_LITERAL_LEN",         0                         ],
      34  => ["SQL_MAX_CATALOG_NAME_LEN",           0                         ],
     108  => ["SQL_MAX_CHAR_LITERAL_LEN",           0                         ],
      30  => ["SQL_MAX_COLUMN_NAME_LEN",            'NAMEDATALEN'             ],
      97  => ["SQL_MAX_COLUMNS_IN_GROUP_BY",        0                         ],
      98  => ["SQL_MAX_COLUMNS_IN_INDEX",           0                         ],
      99  => ["SQL_MAX_COLUMNS_IN_ORDER_BY",        0                         ],
     100  => ["SQL_MAX_COLUMNS_IN_SELECT",          0                         ],
     101  => ["SQL_MAX_COLUMNS_IN_TABLE",           1600                      ], ## depends on column types
      31  => ["SQL_MAX_CURSOR_NAME_LEN",            'NAMEDATALEN'             ],
   10005  => ["SQL_MAX_IDENTIFIER_LEN",             'NAMEDATALEN'             ],
     102  => ["SQL_MAX_INDEX_SIZE",                 0                         ],
     102  => ["SQL_MAX_PROCEDURE_NAME_LEN",         'NAMEDATALEN'             ],
     104  => ["SQL_MAX_ROW_SIZE",                   0                         ], ## actually 1.6 TB, but too big to represent here
     103  => ["SQL_MAX_ROW_SIZE_INCLUDES_LONG",     "Y"                       ],
      32  => ["SQL_MAX_SCHEMA_NAME_LEN",            'NAMEDATALEN'             ],
     105  => ["SQL_MAX_STATEMENT_LEN",              0                         ],
      35  => ["SQL_MAX_TABLE_NAME_LEN",             'NAMEDATALEN'             ],
     106  => ["SQL_MAX_TABLES_IN_SELECT",           0                         ],
     107  => ["SQL_MAX_USER_NAME_LEN",              'NAMEDATALEN'             ],

## Scalar function information

      48  => ["SQL_CONVERT_FUNCTIONS",              2                         ], ## ??
      49  => ["SQL_NUMERIC_FUNCTIONS",              16777215                  ], ## ?? all but some naming clashes: rand(om), trunc(ate), log10=ln, etc.
      50  => ["SQL_STRING_FUNCTIONS",               16280984                  ], ## ??
      51  => ["SQL_SYSTEM_FUNCTIONS",               0                         ], ## ??
     109  => ["SQL_TIMEDATE_ADD_INTERVALS",         0                         ], ## ?? no explicit timestampadd?
     110  => ["SQL_TIMEDATE_DIFF_INTERVALS",        0                         ], ## ??
      52  => ["SQL_TIMEDATE_FUNCTIONS",             1966083                   ],

## Conversion information - all but BIT, LONGVARBINARY, and LONGVARCHAR

      53  => ["SQL_CONVERT_BIGINT",                 1830399                    ],
      54  => ["SQL_CONVERT_BINARY",                 1830399                    ],
      55  => ["SQL_CONVERT_BIT",                    0                          ],
      56  => ["SQL_CONVERT_CHAR",                   1830399                    ],
      57  => ["SQL_CONVERT_DATE",                   1830399                    ],
      58  => ["SQL_CONVERT_DECIMAL",                1830399                    ],
      59  => ["SQL_CONVERT_DOUBLE",                 1830399                    ],
      60  => ["SQL_CONVERT_FLOAT",                  1830399                    ],
      61  => ["SQL_CONVERT_INTEGER",                1830399                    ],
     123  => ["SQL_CONVERT_INTERVAL_DAY_TIME",      1830399                    ],
     124  => ["SQL_CONVERT_INTERVAL_YEAR_MONTH",    1830399                    ],
      71  => ["SQL_CONVERT_LONGVARBINARY",          0                          ],
      62  => ["SQL_CONVERT_LONGVARCHAR",            0                          ],
      63  => ["SQL_CONVERT_NUMERIC",                1830399                    ],
      64  => ["SQL_CONVERT_REAL",                   1830399                    ],
      65  => ["SQL_CONVERT_SMALLINT",               1830399                    ],
      66  => ["SQL_CONVERT_TIME",                   1830399                    ],
      67  => ["SQL_CONVERT_TIMESTAMP",              1830399                    ],
      68  => ["SQL_CONVERT_TINYINT",                1830399                    ],
      69  => ["SQL_CONVERT_VARBINARY",              0                          ],
      70  => ["SQL_CONVERT_VARCHAR",                1830399                    ],
     122  => ["SQL_CONVERT_WCHAR",                  0                          ],
     125  => ["SQL_CONVERT_WLONGVARCHAR",           0                          ],
     126  => ["SQL_CONVERT_WVARCHAR",               0                          ],

		); ## end of %type

		## Put both numbers and names into a hash
		my %t;
		for (keys %type) {
			$t{$_} = $type{$_}->[1];
			$t{$type{$_}->[0]} = $type{$_}->[1];
		}

		return undef unless exists $t{$type};

		my $ans = $t{$type};

		if ($ans eq 'NAMEDATALEN') {
			return $dbh->selectall_arrayref("show max_identifier_length")->[0][0];
		}
		elsif ($ans eq 'ODBCVERSION') {
			my $version = $dbh->{private_dbdpg}{version};
			return "00.00.0000" unless $version =~ /^(\d\d?)(\d\d)(\d\d)$/o;
			return sprintf "%02d.%02d.%.2d00", $1,$2,$3;
		}
		elsif ($ans eq 'DBDVERSION') {
			my $simpleversion = $DBD::Pg::VERSION;
			$simpleversion =~ s/_/./g;
			return sprintf "%02d.%02d.%1d%1d%1d%1d", split (/\./, "$simpleversion.0.0.0.0.0.0");
		}
		 elsif ($ans eq 'MAXCONNECTIONS') {
			 return $dbh->selectall_arrayref("show max_connections")->[0][0];
		 }
		 elsif ($ans eq 'ENCODING') {
			 return $dbh->selectall_arrayref("show server_encoding")->[0][0];
		 }
		 elsif ($ans eq 'KEYWORDS') {
			## http://www.postgresql.org/docs/current/static/sql-keywords-appendix.html
			## Basically, we want ones that are 'reserved' for PostgreSQL but not 'reserved' in SQL:2003
			return join "," => (qw(ANALYSE ANALYZE ASC DEFERRABLE DESC DO FREEZE ILIKE INITIALLY ISNULL LIMIT NOTNULL OFF OFFSET PLACING RETURNING VERBOSE));
		 }
		 elsif ($ans eq 'CURRENTDB') {
			 return $dbh->selectall_arrayref("select pg_catalog.current_database()")->[0][0];
		 }

		 return $ans;
	} # end of get_info

	sub private_attribute_info {
		return {
				pg_bool_tf                => undef,
				pg_enable_utf8            => undef,
				pg_errorlevel             => undef,
				pg_prepare_now            => undef,
				pg_placeholder_dollaronly => undef,
				pg_server_prepare         => undef,
		};
	}
}


{
	package DBD::Pg::st;

	sub bind_param_array {

		## The DBI version is broken, so we implement a near-copy here
		my $sth = shift;
		my ($p_id, $value_array, $attr) = @_;

		return $sth->set_err(1, "Value for parameter $p_id must be a scalar or an arrayref, not a ".ref($value_array))
			if defined $value_array and ref $value_array and ref $value_array ne 'ARRAY';

		return $sth->set_err(1, "Can't use named placeholders for non-driver supported bind_param_array")
			unless DBI::looks_like_number($p_id); # because we rely on execute(@ary) here

		# get/create arrayref to hold params
		my $hash_of_arrays = $sth->{ParamArrays} ||= { };

		if (ref $value_array eq 'ARRAY') {
			# check that input has same length as existing
			# find first arrayref entry (if any)
			for (keys %$hash_of_arrays) {
				my $v = $$hash_of_arrays{$_};
				next unless ref $v eq 'ARRAY';
				return $sth->set_err
					(1,"Arrayref for parameter $p_id has ".@$value_array." elements"
					 ." but parameter $_ has ".@$v)
					if @$value_array != @$v;
			}
		}

		$$hash_of_arrays{$p_id} = $value_array;
		return $sth->bind_param($p_id, '', $attr) if $attr; ## This is the big change so -w does not complain
		return 1;
	} ## end bind_param_array

	sub private_attribute_info {
		return {
				pg_bool_tf                => undef,
				pg_db                     => undef,
				pg_default_port           => undef,
				pg_enable_utf8            => undef,
				pg_errorlevel             => undef,
				pg_host                   => undef,
				pg_INV_READ               => undef,
				pg_INV_WRITE              => undef,
				pg_lib_version            => undef,
				pg_options                => undef,
				pg_pass                   => undef,
				pg_pid                    => undef,
				pg_pid_number             => undef,
				pg_placeholder_dollaronly => undef,
				pg_port                   => undef,
				pg_prepare_now            => undef,
				pg_protocol               => undef,
				pg_server_prepare         => undef,
				pg_server_version         => undef,
				pg_socket                 => undef,
				pg_user                   => undef,
		};
    }

} ## end st section

1;

__END__

=head1 NAME

DBD::Pg - PostgreSQL database driver for the DBI module

=head1 VERSION

This documents version 2.1.0 of the DBD::Pg module

=head1 SYNOPSIS

  use DBI;

  $dbh = DBI->connect("dbi:Pg:dbname=$dbname", "", "", {AutoCommit => 0});

  # For some advanced uses you may need PostgreSQL type values:
  use DBD::Pg qw(:pg_types);

  # See the DBI module documentation for full details

=head1 DESCRIPTION

DBD::Pg is a Perl module that works with the DBI module to provide access to
PostgreSQL databases.

=head1 MODULE DOCUMENTATION

This documentation describes driver specific behavior and restrictions. It is
not supposed to be used as the only reference for the user. In any case
consult the L<DBI|DBI> documentation first!

=head1 THE DBI CLASS

=head2 DBI Class Methods

=over 4

=item B<connect>

To connect to a database with a minimum of parameters, use the following
syntax:

  $dbh = DBI->connect("dbi:Pg:dbname=$dbname", "", "");

This connects to the database $dbname on the default port (usually 5432) without 
any user authentication.

The following connect statement shows almost all possible parameters:

  $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$host;port=$port;" .
                      "options=$options", "$username", "$password",
                      {AutoCommit => 0});

If a parameter is not given, the PostgreSQL server will first look for 
specific environment variables, and then use hard-coded defaults:

  parameter  environment variable  hard coded default
  --------------------------------------------------
  host       PGHOST                local domain socket
  hostaddr   PGHOSTADDR            local domain socket
  port       PGPORT                5432
  dbname*    PGDATABASE            current userid
  username   PGUSER                current userid
  password   PGPASSWORD            (none)
  options    PGOPTIONS             (none)
  service    PGSERVICE             (none)
  sslmode    PGSSLMODE             (none)

* Can also use "db" or "database"

The options parameter specifies runtime options for the Postgres
backend. Common usage is to increase the number of buffers with the C<-B>
option. Also important is the C<-F> option, which disables automatic fsync()
call after each transaction. For further details please refer to the
PostgreSQL documentation at L<http://www.postgresql.org/docs/>.

For authentication with username and password, appropriate entries have to be
made in the F<pg_hba.conf> file. If the username and password entries passed 
via connect() are undefined (as opposed to being empty), DBI will use the 
environment variables C<DBI_USER> and C<DBI_PASS> if they exists.

You can also connect by using a service connection file, which is named 
"pg_service.conf." The location of this file can be controlled by 
setting the C<PGSYSCONFDIR> environment variable. To use one of the named 
services within the file, set the name by using either the "service" parameter 
or the environment variable C<PGSERVICE>. Note that when connecting this way, 
only the minimum parameters should be used. For example, to connect to a 
service named "zephyr", you could use:

  $dbh = DBI->connect("dbi:Pg:service=zephyr", "", "");

You could also set $ENV{PGSERVICE} to "zephyr" and connect like this:

  $dbh = DBI->connect("dbi:Pg:", "", "");

The format of the pg_service.conf file is simply a bracketed service 
name, followed by one parameter per line in the format name=value.
For example:

  [zephyr]
  dbname=winds
  user=wisp
  password=W$2Hc00YSgP
  port=6543

There are four valid arguments to the "sslmode" parameter, which controls 
whether to use SSL to connect to the database:

=over 4

=item disable - SSL connections are never used

=item allow - try non-SSL, then SSL

=item prefer - try SSL, then non-SSL

=item require - connect only with SSL

=back

=item B<connect_cached>

Implemented by DBI, no driver-specific impact.

=item B<installed_drivers>

Implemented by DBI, no driver-specific impact.

=item B<installed_versions>

Implemented by DBI, no driver-specific impact.

=item B<available_drivers>

  @driver_names = DBI->available_drivers;

Implemented by DBI, no driver-specific impact.

=item B<data_sources>

  @data_sources = DBI->data_sources('Pg');

This driver supports this method. Note that the necessary database connection
to the database "template1" will be made on the localhost without any user
authentication. Other preferences can be set with the environment
variables C<PGHOST>, C<PGPORT>, C<DBI_USER>, C<DBI_PASS>, and C<PGSERVICE>.

You can also pass in options to add to the connection string as the second argument 
to data_sources. For example, to specify an alternate port and host:

  @data_sources = DBI->data_sources('Pg', 'port=5824;host=example.com');

=back

=head1 METHODS COMMON TO ALL HANDLES

=over 4

=item B<err>

  $rv = $h->err;

Supported by this driver as proposed by DBI. For the connect method it returns
C<PQstatus>. In all other cases it returns C<PQresultStatus> of the current
handle.

=item B<errstr>

  $str = $h->errstr;

Supported by this driver as proposed by DBI. It returns the C<PQerrorMessage>
related to the current handle.

=item B<state>

  $str = $h->state;

Supported by this driver. Returns a five-character "SQLSTATE" code.
Success is indicated by a "00000" code, which gets mapped to an 
empty string by DBI. A code of S8006 indicates a connection failure, 
usually because the connection to the PostgreSQL server has been lost.

Note that state can be called as either $sth->state or $dbh->state.

The list of codes used by PostgreSQL can be found at:
L<http://www.postgresql.org/docs/current/static/errcodes-appendix.html>

Note that these codes are part of the SQL standard and only a small number 
of them will be used by PostgreSQL.

Common ones to note:

  00000 Successful completion
  25P01 No active SQL transaction
  25P02 In failed SQL transaction

=item B<trace>

  $h->trace($trace_level);
  $h->trace($trace_level, $trace_filename);

Implemented by DBI, no driver-specific impact.

=item B<trace_msg>

  $h->trace_msg($message_text);
  $h->trace_msg($message_text, $min_level);

Implemented by DBI, no driver-specific impact.

=item B<func>

This driver supports a variety of driver specific functions accessible via the
C<func> method. Note that the name of the function comes last, after the arguments.

=over

=item table_attributes

  $attrs = $dbh->func($table, 'table_attributes');

The C<table_attributes> function is no longer recommended. Instead,
you can use the more portable C<column_info> and C<primary_key> methods
to access the same information.

The C<table_attributes> method returns, for the given table argument, a
reference to an array of hashes, each of which contains the following keys:

  NAME        attribute name
  TYPE        attribute type
  SIZE        attribute size (-1 for variable size)
  NULLABLE    flag nullable
  DEFAULT     default value
  CONSTRAINT  constraint
  PRIMARY_KEY flag is_primary_key
  REMARKS     attribute description

=item lo_creat

  $lobjId = $dbh->func($mode, 'lo_creat');

Creates a new large object and returns the object-id. $mode is a bitmask
describing different attributes of the new object. Use the following
constants:

  $dbh->{pg_INV_WRITE}
  $dbh->{pg_INV_READ}

Upon failure it returns C<undef>.

=item lo_open

  $lobj_fd = $dbh->func($lobjId, $mode, 'lo_open');

Opens an existing large object and returns an object-descriptor for use in
subsequent C<lo_*> calls. For the mode bits see C<lo_creat>. Returns C<undef>
upon failure. Note that 0 is a perfectly correct object descriptor!

=item lo_write

  $nbytes = $dbh->func($lobj_fd, $buf, $len, 'lo_write');

Writes $len bytes of $buf into the large object $lobj_fd. Returns the number
of bytes written and C<undef> upon failure.

=item lo_read

  $nbytes = $dbh->func($lobj_fd, $buf, $len, 'lo_read');

Reads $len bytes into $buf from large object $lobj_fd. Returns the number of
bytes read and C<undef> upon failure.

=item lo_lseek

  $loc = $dbh->func($lobj_fd, $offset, $whence, 'lo_lseek');

Changes the current read or write location on the large object
$obj_id. Currently $whence can only be 0 (C<L_SET>). Returns the current
location and C<undef> upon failure.

=item lo_tell

  $loc = $dbh->func($lobj_fd, 'lo_tell');

Returns the current read or write location on the large object $lobj_fd and
C<undef> upon failure.

=item lo_close

  $lobj_fd = $dbh->func($lobj_fd, 'lo_close');

Closes an existing large object. Returns true upon success and false upon
failure.

=item lo_unlink

  $ret = $dbh->func($lobjId, 'lo_unlink');

Deletes an existing large object. Returns true upon success and false upon
failure.

=item lo_import

  $lobjId = $dbh->func($filename, 'lo_import');

Imports a Unix file as large object and returns the object id of the new
object or C<undef> upon failure.

=item lo_export

  $ret = $dbh->func($lobjId, $filename, 'lo_export');

Exports a large object into a Unix file. Returns false upon failure, true
otherwise.

=item pg_notifies

  $ret = $dbh->func('pg_notifies');

Returns either C<undef> or a reference to two-element array [ $table,
$backend_pid ] of asynchronous notifications received. Note that this does
not check if the connection to the database is still valid - for that, 
use the c<ping> method. Also note that you may need to commit if not in 
autocommit mode - new notices will not be picked up while in the middle of 
a transaction. An example:

  $dbh->do("LISTEN abc");
  $dbh->do("LISTEN def");

  ## Hang around until we get the message we want
  LISTENLOOP: {
    while (my $notify = $dbh->func('pg_notifies')) {
      my ($name, $pid) = @$notify;
      print qq{I received notice "$name" from PID $pid\n};
      ## Do something based on the notice received
    }
    $dbh->ping() or die qq{Ping failed!};
    $dbh->commit();
    sleep(5);
    redo;
  }

=item getfd

  $fd = $dbh->func('getfd');

Returns fd of the actual connection to server. Can be used with select() and
func('pg_notifies'). Deprecated in favor of C<< $dbh->{pg_socket} >>.

=back

=item private_attribute_info

  $hashref = $dbh->private_attribute_info();
  $hashref = $sth->private_attribute_info();

Supported by this driver as proposed by DBI.


=back

=head1 ATTRIBUTES COMMON TO ALL HANDLES

=over 4

=item B<Warn> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<Active> (boolean, read-only)

Supported by this driver as proposed by DBI. A database handle is active while
it is connected and statement handle is active until it is finished.

=item B<Executed> (boolean, read-only)

Implemented by DBI, no driver-specific impact. Requires DBI 1.41 or greater.

=item B<Kids> (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<ActiveKids> (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<CachedKids> (hash ref)

Implemented by DBI, no driver-specific impact.

=item B<Type> (scalar)

Implemented by DBI, no driver-specific impact.

=item B<ChildHandles> (array ref)

Implemented by DBI, no driver-specific impact.

=item B<CompatMode> (boolean, inherited)

Not used by this driver.

=item B<InactiveDestroy> (boolean)

Implemented by DBI, no driver-specific impact. If set to true, then 
the disconnect() method will not be automatically called when the 
database handle goes out of scope (e.g. when exiting after a fork).

=item B<PrintWarn> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<PrintError> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<RaiseError> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<HandleError> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<HandleSetErr> (code ref, inherited)

Implemented by DBI, no driver-specific impact.

=item B<ErrCount> (unsigned integer)

Implemented by DBI, no driver-specific impact.

=item B<ShowErrorStatement> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<TraceLevel> (integer, inherited)

Implemented by DBI, no driver-specific impact.

=item B<FetchHashKeyName> (string, inherited)

Implemented by DBI, no driver-specific impact.

=item B<ChopBlanks> (boolean, inherited)

Supported by this driver as proposed by DBI. This method is similar to the
SQL function C<RTRIM>.

=item B<LongReadLen> (integer, inherited)

Not used by this driver.

=item B<LongTruncOk> (boolean, inherited)

Not used by this driver.

=item B<Taint> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<TaintIn> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<TaintOut> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<Profile> (inherited)

Implemented by DBI, no driver-specific impact.

=back

=head1 DBI DATABASE HANDLE OBJECTS

=head2 Database Handle Methods

=over 4

=item B<selectrow_array>

  @row_ary = $dbh->selectrow_array($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact.

=item B<selectrow_arrayref>

  $ary_ref = $dbh->selectrow_arrayref($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact.

=item B<selectrow_hashref>

  $hash_ref = $dbh->selectrow_hashref($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact.

=item B<selectall_arrayref>

  $ary_ref = $dbh->selectall_arrayref($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact.

=item B<selectall_hashref>

  $hash_ref = $dbh->selectall_hashref($statement, $key_field);

Implemented by DBI, no driver-specific impact.

=item B<selectcol_arrayref>

  $ary_ref = $dbh->selectcol_arrayref($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact.

=item B<prepare>

  $sth = $dbh->prepare($statement, \%attr);

WARNING: DBD::Pg now uses true prepared statements by sending them 
to the backend to be prepared by the PostgreSQL server. Statements 
that were legal before may no longer work. See below for details.

Prepares a statement for later execution. PostgreSQL supports prepared
statements, which enables DBD::Pg to only send the query once, and
simply send the arguments for every subsequent call to execute().
DBD::Pg can use these server-side prepared statements, or it can
just send the entire query to the server each time. The best way
is automatically chosen for each query. This will be sufficient for
most users: keep reading for a more detailed explanation and some
optional flags.

Statements that do not begin with the word "SELECT", "INSERT", 
"UPDATE", or "DELETE" will not be sent to be server-side prepared.

Deciding whether or not to use prepared statements depends on many factors, 
but you can force them to be used or not used by passing the 
C<pg_server_prepare> attribute to prepare(). A "0" means to never use 
prepared statements. Setting C<pg_server_prepare> to "1" means that prepared 
statements should be used whenever possible. This is the default for servers
version 8.0 or higher. Servers that are version 7.4 get a special default 
value of "2", because server-side statements were only partially supported 
in that version. In this case, it only uses server-side prepares if all 
parameters are specifically bound. 

The pg_server_prepare attribute can also be set at connection time like so:

  $dbh = DBI->connect($DBNAME, $DBUSER, $DBPASS,
                      { AutoCommit => 0,
                        RaiseError => 1,
                        pg_server_prepare => 0 });

or you may set it after your database handle is created:

  $dbh->{pg_server_prepare} = 1;

To enable it for just one particular statement:

  $sth = $dbh->prepare("SELECT id FROM mytable WHERE val = ?",
                       { pg_server_prepare => 1 });

You can even toggle between the two as you go:

  $sth->{pg_server_prepare} = 1;
  $sth->execute(22);
  $sth->{pg_server_prepare} = 0;
  $sth->execute(44);
  $sth->{pg_server_prepare} = 1;
  $sth->execute(66);

In the above example, the first execute will use the previously prepared statement.
The second execute will not, but will build the query into a single string and send
it to the server. The third one will act like the first and only send the arguments.
Even if you toggle back and forth, a statement is only prepared once.

Using prepared statements is in theory quite a bit faster: not only does the
PostgreSQL backend only have to prepare the query only once, but DBD::Pg no
longer has to worry about quoting each value before sending it to the server.

However, there are some drawbacks. The server cannot always choose the ideal
parse plan because it will not know the arguments before hand. But for most
situations in which you will be executing similar data many times, the default
plan will probably work out well. Programs such as PgBouncer which cache connections 
at a low level should not use prepared statements via DBD::Pg, or must take 
extra care in the application to account for the fact that prepared statements 
are not shared across database connections. Further discussion on this subject is beyond
the scope of this documentation: please consult the pgsql-performance mailing
list, L<http://archives.postgresql.org/pgsql-performance/>

Only certain commands will be sent to a server-side prepare: currently these
include C<SELECT>, C<INSERT>, C<UPDATE>, and C<DELETE>. DBD::Pg uses a simple
naming scheme for the prepared statements: C<dbdpg_#>, where "#" starts at 1 and
increases. This number is tracked at the database handle level, so multiple
statement handles will not collide. If you use your own prepare statements, do
not name them "dbdpg_"!

You cannot send more than one command at a time in the same prepare command, 
by separating them with semi-colons, when using server-side prepares.

The actual C<PREPARE> is not performed until the first execute is called, due
to the fact that information on the data types (provided by C<bind_param>) may
be given after the prepare but before the execute.

A server-side prepare can also happen before the first execute. If the server can
handle the server-side prepare and the statement has no placeholders, it will
be prepared right away. It will also be prepared if the C<pg_prepare_now> attribute
is passed. Similarly, the <pg_prepare_now> attribute can be set to 0 to ensure that
the statement is B<not> prepared immediately, although cases in which you would
want this may be rare. Finally, you can set the default behavior of all prepare
statements by setting the C<pg_prepare_now> attribute on the database handle:

  $dbh->{pg_prepare_now} = 1;

The following two examples will be prepared right away:

  $sth->prepare("SELECT 123"); ## no placeholders

  $sth->prepare("SELECT 123, ?", {pg_prepare_now => 1});

The following two examples will NOT be prepared right away:

  $sth->prepare("SELECT 123, ?"); ## has a placeholder

  $sth->prepare("SELECT 123", {pg_prepare_now => 0});

There are times when you may want to prepare a statement yourself. To do this,
simply send the C<PREPARE> statement directly to the server (e.g. with
"do"). Create a statement handle and set the prepared name via
C<pg_prepare_name> attribute. The statement handle can be created with a dummy
statement, as it will not be executed. However, it should have the same
number of placeholders as your prepared statement. Example:

  $dbh->do("PREPARE mystat AS SELECT COUNT(*) FROM pg_class WHERE reltuples < ?");
  $sth = $dbh->prepare("SELECT ?");
  $sth->bind_param(1, 1, SQL_INTEGER);
  $sth->{pg_prepare_name} = "mystat";
  $sth->execute(123);

The above will run this query:

  SELECT COUNT(*) FROM pg_class WHERE reltuples < 123;

Note: DBD::Pg will not escape your custom prepared statement name, so don't
use a name that needs escaping! DBD::Pg uses the prepare names C<dbdpg_#>
internally, so please do not use those either.

You can force DBD::Pg to send your query directly to the server by adding
the C<pg_direct> attribute to your prepare call. This is not recommended,
but is added just in case you need it.

=item B<Placeholders>

There are three types of placeholders that can be used in DBD::Pg. The first is
the question mark method, in which each placeholder is represented by a single
question mark. This is the method recommended by the DBI specs and is the most
portable. Each question mark is replaced by a "dollar sign number" in the order
in which they appear in the query (important when using C<bind_param>).

The second method is to use "dollar sign numbers" directly. This is the method
that PostgreSQL uses internally and is overall probably the best method to use
if you do not need compatibility with other database systems. DBD::Pg, like
PostgreSQL, allows the same number to be used more than once in the query.
Numbers must start with "1" and increment by one value. If the same number
appears more than once in a query, it is treated as a single parameter and all
instances are replaced at once. Examples:

Not legal:

  $SQL = 'SELECT count(*) FROM pg_class WHERE relpages > $2';

  $SQL = 'SELECT count(*) FROM pg_class WHERE relpages BETWEEN $1 AND $3';

Legal:

  $SQL = 'SELECT count(*) FROM pg_class WHERE relpages > $1';

  $SQL = 'SELECT count(*) FROM pg_class WHERE relpages BETWEEN $1 AND $2';

  $SQL = 'SELECT count(*) FROM pg_class WHERE relpages BETWEEN $1 AND $2 AND reltuples > $1';

  $SQL = 'SELECT count(*) FROM pg_class WHERE relpages > $1 AND reltuples > $1';

In the final statement above, DBI thinks there is only one placeholder, so this
statement will replace both placeholders:

  $sth->bind_param(1, 2045);

While execute requires only a single argument as well:

  $sth->execute(2045);

The final placeholder method is the named parameters in the format ":foo". While this
syntax is supported by DBD::Pg, its use is highly discouraged.

The different types of placeholders cannot be mixed within a statement, but you may
use different ones for each statement handle you have. Again, this is not encouraged.

If your queries use operators that contain question marks (e.g. some of the native 
Postgres geometric operators) or array slices (e.g. data[100:300]), you can tell 
DBD::Pg to ignore any non-dollar sign placeholders by setting the 
"pg_placeholder_dollaronly" attribute at either the database handle or the statement 
handle level. Examples:

  $dbh->{pg_placeholder_dollaronly} = 1;
  $sth = $dbh->prepare(q{SELECT * FROM mytable WHERE lseg1 ?# lseg2 AND name = $1});
  $sth->execute('segname');

Alternatively, you can set it at prepare time:

  $sth = $dbh->prepare(q{SELECT * FROM mytable WHERE lseg1 ?-| lseg2 AND name = $1},
    {pg_placeholder_dollaronly = 1});
  $sth->execute('segname');

=item B<prepare_cached>

  $sth = $dbh->prepare_cached($statement, \%attr);

Implemented by DBI, no driver-specific impact. This method is most useful
when using a server that supports server-side prepares, and you have asked
the prepare to happen immediately via the C<pg_prepare_now> attribute.

=item B<do>

  $rv  = $dbh->do($statement, \%attr, @bind_values);

Prepare and execute a single statement. Note that an empty statement 
(string with no length) will not be passed to the server; if you 
want a simple test, use "SELECT 123" or the ping() function. If 
neither attr nor bind_values is given, the query will be sent directly 
to the server without the overhead of creating a statement handle and 
running prepare and execute.


=item B<last_insert_id>

  $rv = $dbh->last_insert_id($catalog, $schema, $table, $field);
  $rv = $dbh->last_insert_id($catalog, $schema, $table, $field, \%attr);

Attempts to return the id of the last value to be inserted into a table.
You can either provide a sequence name (preferred) or provide a table
name with optional schema. The $catalog and $field arguments are always ignored.
The current value of the sequence is returned by a call to the
C<CURRVAL()> PostgreSQL function. This will fail if the sequence has not yet
been used in the current database connection.

If you do not know the name of the sequence, you can provide a table name and
DBD::Pg will attempt to return the correct value. To do this, there must be at
least one column in the table with a C<NOT NULL> constraint, that has a unique
constraint, and which uses a sequence as a default value. If more than one column
meets these conditions, the primary key will be used. This involves some
looking up of things in the system table, so DBD::Pg will cache the sequence
name for subsequent calls. If you need to disable this caching for some reason,
(such as the sequence name changing), you can control it via the C<pg_cache> 
attribute.

Please keep in mind that this method is far from foolproof, so make your
script use it properly. Specifically, make sure that it is called
immediately after the insert, and that the insert does not add a value
to the column that is using the sequence as a default value.

Some examples:

  $dbh->do("CREATE SEQUENCE lii_seq START 1");
  $dbh->do("CREATE TABLE lii (
    foobar INTEGER NOT NULL UNIQUE DEFAULT nextval('lii_seq'),
    baz VARCHAR)");
  $SQL = "INSERT INTO lii(baz) VALUES (?)";
  $sth = $dbh->prepare($SQL);
  for (qw(uno dos tres cuatro)) {
    $sth->execute($_);
    my $newid = $dbh->last_insert_id(C<undef>,undef,undef,undef,{sequence=>'lii_seq'});
    print "Last insert id was $newid\n";
  }

If you did not want to worry about the sequence name:

  $dbh->do("CREATE TABLE lii2 (
    foobar SERIAL UNIQUE,
    baz VARCHAR)");
  $SQL = "INSERT INTO lii2(baz) VALUES (?)";
  $sth = $dbh->prepare($SQL);
  for (qw(uno dos tres cuatro)) {
    $sth->execute($_);
    my $newid = $dbh->last_insert_id(undef,undef,"lii2",undef);
    print "Last insert id was $newid\n";
  }

=item B<commit>

  $rc  = $dbh->commit;

Supported by this driver as proposed by DBI. See also the notes about
B<Transactions> elsewhere in this document.

=item B<rollback>

  $rc  = $dbh->rollback;

Supported by this driver as proposed by DBI. See also the notes about
B<Transactions> elsewhere in this document.

=item B<begin_work>

Supported by this driver as proposed by DBI. Note that this will not 
issue a "begin" until immediately before the next given command.

=item B<disconnect>

  $rc  = $dbh->disconnect;

Supported by this driver as proposed by DBI.

=item B<ping>

  $rc = $dbh->ping;

This driver supports the C<ping> method, which can be used to check the validity
of a database handle. The value returned is either 0, indicating that the 
connection is no longer valid, or a positive integer, indicating the following:

  Value    Meaning
  --------------------------------------------------
    1      Database is idle (not in a transaction)
    2      Database is active, there is a command in progress (usually seen after a COPY command)
    3      Database is idle within a transaction
    4      Database is idle, within a failed transaction

Additional information on why a handle is not valid can be obtained by using the 
C<pg_ping> method.

=item B<pg_ping>

  $rc = $dbh->pg_ping;

This is a DBD::Pg-specific extension to the C<ping> command. This will check the 
validity of a database handle in exactly the same way as C<ping>, but instead of 
returning a 0 for an invalid connection, it will return a negative number. So in 
addition to returning the positive numbers documented for C<ping>, it may also 
return the following:

  Value    Meaning
  --------------------------------------------------
   -1      There is no connection to the database at all (e.g. after C<disconnect>)
   -2      An unknown transaction status was returned (e.g. after forking)
   -3      The handle exists, but no data was returned from a test query.

In practice, you should only ever see -1 and -2.

=item B<get_info>

  $value = $dbh->get_info($info_type);

Supports a very large set (> 250) of the information types, including the minimum 
recommended by DBI.

=item B<table_info>

  $sth = $dbh->table_info( $catalog, $schema, $table, $type );

Supported by this driver as proposed by DBI. This method returns all tables
and views visible to the current user. The $catalog argument is currently
unused. The schema and table arguments will do a C<LIKE> search if a percent
sign (C<%>) or an underscore (C<_>) is detected in the argument. The $type
argument accepts a value of either "TABLE" or "VIEW" (using both is the
default action).

The TABLE_CAT field will always return NULL (C<undef>).

If your database supports tablespaces (version 8.0 or greater), two additional
columns are returned, "pg_tablespace_name" and "pg_tablespace_location",
that contain the name and location of the tablespace associated with
this table. Tables that have not been assigned to a particular tablespace
will return NULL (C<undef>) for both of these columns.

Three additional fields are returned:

  pg_schema - the unquoted name of the schema
  pg_table - the unquoted name of the table
  pg_column - the unquoted name of the column

=item B<column_info>

  $sth = $dbh->column_info( $catalog, $schema, $table, $column );

Supported by this driver as proposed by DBI with the follow exceptions.
These fields are currently always returned with NULL (C<undef>) values:

   TABLE_CAT
   BUFFER_LENGTH
   DECIMAL_DIGITS
   NUM_PREC_RADIX
   SQL_DATA_TYPE
   SQL_DATETIME_SUB
   CHAR_OCTET_LENGTH

Also, five additional non-standard fields are returned:

  pg_type - data type with additional info i.e. "character varying(20)"
  pg_constraint - holds column constraint definition
  pg_schema - the unquoted name of the schema
  pg_table - the unquoted name of the table
  pg_column - the unquoted name of the column

Note that the TABLE_SCHEM, TABLE_NAME, and COLUMN_NAME fields all return 
output wrapped in quote_ident(). If you need the unquoted version, use 
the pg_ fields above.

=item B<primary_key_info>

  $sth = $dbh->primary_key_info( $catalog, $schema, $table, \%attr );

Supported by this driver as proposed by DBI. The $catalog argument is
currently unused. There are no search patterns allowed, but leaving the 
$schema argument blank will cause the first table found in the schema 
search path to be used. An additional field, "DATA_TYPE", is returned and 
shows the data type for each of the arguments in the "COLUMN_NAME" field.

This method will also return tablespace information for servers that support
tablespaces. See the C<table_info> entry for more information.

The five additional custom fields returned are:

  pg_tablespace_name - Name of the tablespace, if any
  pg_tablespace_location - Location of the tablespace
  pg_schema - the unquoted name of the schema
  pg_table - the unquoted name of the table
  pg_column - the unquoted name of the column

In addition to the standard format of returning one row for each column
found for the primary key, you can pass the C<pg_onerow> attribute to force
a single row to be used. If the primary key has multiple columns, the
"KEY_SEQ", "COLUMN_NAME", and "DATA_TYPE" fields will return a comma-delimited
string. If the C<pg_onerow> attribute is set to "2", the fields will be
returned as an arrayref, which can be useful when multiple columns are
involved:

  $sth = $dbh->primary_key_info('', '', 'dbd_pg_test', {pg_onerow => 2});
  if (defined $sth) {
    my $pk = $sth->fetchall_arrayref()->[0];
    print "Table $pk->[2] has a primary key on these columns:\n";
    for (my $x=0; defined $pk->[3][$x]; $x++) {
      print "Column: $pk->[3][$x]  (data type: $pk->[6][$x])\n";
    }
  }

=item B<primary_key>

Supported by this driver as proposed by DBI.

=item B<foreign_key_info>

  $sth = $dbh->foreign_key_info( $pk_catalog, $pk_schema, $pk_table,
                                 $fk_catalog, $fk_schema, $fk_table );

Supported by this driver as proposed by DBI, using the SQL/CLI variant.
There are no search patterns allowed, but leaving the $schema argument
blank will cause the first table found in the schema search path to be
used. Two additional fields, "UK_DATA_TYPE" and "FK_DATA_TYPE", are returned
to show the data type for the unique and foreign key columns. Foreign
keys that have no named constraint (where the referenced column only has
an unique index) will return C<undef> for the "UK_NAME" field.

=item B<tables>

  @names = $dbh->tables( $catalog, $schema, $table, $type, \%attr );

Supported by this driver as proposed by DBI. This method returns all tables
and/or views which are visible to the current user: see C<table_info()>
for more information about the arguments. The name of the schema appears 
before the table or view name. This can be turned off by adding in the 
C<pg_noprefix> attribute:

  my @tables = $dbh->tables( '', '', 'dbd_pg_test', '', {pg_noprefix => 1} );

=item B<type_info_all>

  $type_info_all = $dbh->type_info_all;

Supported by this driver as proposed by DBI. Information is only provided for
SQL datatypes and for frequently used datatypes. The mapping between the
PostgreSQL typename and the SQL92 datatype (if possible) has been done
according to the following table:

  +---------------+------------------------------------+
  | typname       | SQL92                              |
  |---------------+------------------------------------|
  | bool          | BOOL                               |
  | text          | /                                  |
  | bpchar        | CHAR(n)                            |
  | varchar       | VARCHAR(n)                         |
  | int2          | SMALLINT                           |
  | int4          | INT                                |
  | int8          | /                                  |
  | money         | /                                  |
  | float4        | FLOAT(p)   p<7=float4, p<16=float8 |
  | float8        | REAL                               |
  | abstime       | /                                  |
  | reltime       | /                                  |
  | tinterval     | /                                  |
  | date          | /                                  |
  | time          | /                                  |
  | datetime      | /                                  |
  | timespan      | TINTERVAL                          |
  | timestamp     | TIMESTAMP                          |
  +---------------+------------------------------------+

For further details concerning the PostgreSQL specific datatypes please read
L<pgbuiltin|pgbuiltin>.

=item B<type_info>

  @type_info = $dbh->type_info($data_type);

Implemented by DBI, no driver-specific impact.

=item B<quote>

  $rv = $dbh->quote($value, $data_type);

This module implements its own C<quote> method. In addition to the DBI method it
also doubles the backslash, because PostgreSQL treats a backslash as an escape
character. You may also quote arrayrefs and received a string suitable for 
passing into Postgres array columns.

B<NOTE:> The undocumented (and invalid) support for the C<SQL_BINARY> data
type is officially deprecated. Use C<PG_BYTEA> with C<bind_param()> instead:

  $rv = $sth->bind_param($param_num, $bind_value,
                         { pg_type => PG_BYTEA });

=item B<quote_identifier>

Implemented by DBI, no driver-specific impact.

=item B<pg_server_trace>

  $dbh->pg_server_trace($filehandle);

Writes debugging information from the PostgreSQL backend to a file. This is
not the same as the trace() method and you should not use this method unless
you know what you are doing. If you do enable this, be aware that the file
will grow very large, very quick. To stop logging to the file, use the
C<pg_server_untrace> function. The first argument must be a file handle, not
a filename. Example:

  my $pid = $dbh->{pg_pid};
  my $file = "pgbackend.$pid.debug.log";
  open(my $fh, ">$file") or die qq{Could not open "$file": $!\n};
  $dbh->pg_server_trace($fh);
  ## Run code you want to trace here
  $dbh->pg_server_untrace;
  close($fh);

=item B<pg_server_untrace>

  $dbh->pg_server_untrace

Stop server logging to a previously opened file.

=back

=head2 Database Handle Attributes

=over 4

=item B<AutoCommit>  (boolean)

Supported by this driver as proposed by DBI. According to the classification of
DBI, PostgreSQL is a database in which a transaction must be explicitly
started. Without starting a transaction, every change to the database becomes
immediately permanent. The default of AutoCommit is on, but this may change
in the future, so it is highly recommended that you explicitly set it when
calling C<connect()>. For details see the notes about B<Transactions>
elsewhere in this document.

=item B<pg_bool_tf> (boolean)

PostgreSQL specific attribute. If true, boolean values will be returned
as the characters 't' and 'f' instead of '1' and '0'.

=item B<Driver>  (handle)

Implemented by DBI, no driver-specific impact.

=item B<Name>  (string, read-only)

The default DBI method is overridden by a driver specific method that returns
only the database name. Anything else from the connection string is stripped
off. Note that, in contrast to the DBI specs, the DBD::Pg implementation for
this method is read-only.

=item B<RowCacheSize>  (integer)

Implemented by DBI, not used by this driver.

=item B<Username>  (string, read-only)

Supported by this driver as proposed by DBI.

=item B<pg_auto_escape> (boolean)

PostgreSQL specific attribute. If true, then quotes and backslashes in all
parameters will be escaped in the following way:

  escape quote with a quote (SQL)
  escape backslash with a backslash

The default is on. Note that PostgreSQL also accepts quotes that are
escaped by a backslash. Any other ASCII character can be used directly in a
string constant.

=item B<pg_enable_utf8> (boolean)

PostgreSQL specific attribute. If true, then the C<utf8> flag will be turned
for returned character data (if the data is valid UTF-8). For details about
the C<utf8> flag, see L<Encode|Encode>. This attribute is only relevant under
perl 5.8 and later.

B<NB>: This attribute is experimental and may be subject to change.

=item B<pg_INV_READ> (integer, read-only)

Constant to be used for the mode in C<lo_creat> and C<lo_open>.

=item B<pg_INV_WRITE> (integer, read-only)

Constant to be used for the mode in C<lo_creat> and C<lo_open>.

=item B<pg_errorlevel> (integer)

PostgreSQL specific attribute. Sets the amount of information returned by the server's 
error messages. Valid entries are 0, 1, and 2. Any other number will be forced to the 
default value of 1.

A value of 0 ("TERSE") will show severity, primary text, and position only
and will usually fit on a single line. A value of 1 ("DEFAULT") will also
show any detail, hint, or context fields. A value of 2 ("VERBOSE") will
show all available information.

=item B<pg_protocol> (integer, read-only)

PostgreSQL specific attribute. Returns the version of the PostgreSQL server.
If DBD::Pg is unable to figure out the version, it will return a "0". Otherwise,
a "3" is returned.

=item B<pg_lib_version> (integer, read-only)

PostgreSQL specific attribute. Indicates which version of PostgreSQL that 
DBD::Pg was compiled against. In other words, which libraries were used. 
Returns a number with major, minor, and revision together; version 8.1.4 
would be returned as 80104.

=item B<pg_server_version> (integer, read-only)

PostgreSQL specific attribute. Indicates which version of PostgreSQL that 
the current database handle is connected to. Returns a number with major, 
minor, and revision together; version 8.0.1 would be 80001.

=item B<pg_db> (string, read-only)

PostgreSQL specific attribute. Returns the name of the current database.

=item B<pg_user> (string, read-only)

PostgreSQL specific attribute. Returns the name of the user that
connected to the server.

=item B<pg_pass> (string, read-only)

PostgreSQL specific attribute. Returns the password used to connect
to the server.

=item B<pg_host> (string, read-only)

PostgreSQL specific attribute. Returns the host of the current
server connection. Locally connected hosts will return an empty
string.

=item B<pg_port> (integer, read-only)

PostgreSQL specific attribute. Returns the port of the connection to
the server.

=item B<pg_default_port> (integer, read-only)

PostgreSQL specific attribute. Returns the default port used if none is
specifically given.

=item B<pg_options> (string, read-only)

PostgreSQL specific attribute. Returns the command-line options passed
to the server. May be an empty string.

=item B<pg_socket> (number, read-only)

PostgreSQL specific attribute. Returns the file description number of
the connection socket to the server.

=item B<pg_pid> (number, read-only)

PostgreSQL specific attribute. Returns the process id (PID) of the
backend server process handling the connection.

=back

=head1 DBI STATEMENT HANDLE OBJECTS

=head2 Statement Handle Methods

=over 4

=item B<bind_param>

  $rv = $sth->bind_param($param_num, $bind_value, \%attr);

Allows the user to bind a value and/or a data type to a placeholder. This is
especially important when using server-side prepares. See the 
C<prepare()> method for more information.

The value of $param_num is a number if using the '?' or '$1' style
placeholders. If using ":foo" style placeholders, the complete name
(e.g. ":foo") must be given. For numeric values, you can either use a
number or use a literal '$1'. See the examples below.

The $bind_value argument is fairly self-explanatory. A value of C<undef> will
bind a C<NULL> to the placeholder. Using C<undef> is useful when you want
to change just the type and will be overwriting the value later.
(Any value is actually usable, but C<undef> is easy and efficient).

The %attr hash is used to indicate the data type of the placeholder.
The default value is "varchar". If you need something else, you must
use one of the values provided by DBI or by DBD::Pg. To use a SQL value,
modify your "use DBI" statement at the top of your script as follows:

  use DBI qw(:sql_types);

This will import some constants into your script. You can plug those
directly into the C<bind_param> call. Some common ones that you will
encounter are:

  SQL_INTEGER

To use PostgreSQL data types, import the list of values like this:

  use DBD::Pg qw(:pg_types);

You can then set the data types by setting the value of the C<pg_type>
key in the hash passed to C<bind_param>. 
The current list of Postgres data types exported is:

PG_ABSTIME PG_ABSTIMEARRAY PG_ACLITEM PG_ACLITEMARRAY PG_ANY PG_ANYARRAY
PG_ANYELEMENT PG_ANYENUM PG_ANYNONARRAY PG_BIT PG_BITARRAY PG_BOOL
PG_BOOLARRAY PG_BOX PG_BOXARRAY PG_BPCHAR PG_BPCHARARRAY PG_BYTEA
PG_BYTEAARRAY PG_CHAR PG_CHARARRAY PG_CID PG_CIDARRAY PG_CIDR
PG_CIDRARRAY PG_CIRCLE PG_CIRCLEARRAY PG_CSTRING PG_CSTRINGARRAY PG_DATE
PG_DATEARRAY PG_FLOAT4 PG_FLOAT4ARRAY PG_FLOAT8 PG_FLOAT8ARRAY PG_GTSVECTOR
PG_GTSVECTORARRAY PG_INET PG_INETARRAY PG_INT2 PG_INT2ARRAY PG_INT2VECTOR
PG_INT2VECTORARRAY PG_INT4 PG_INT4ARRAY PG_INT8 PG_INT8ARRAY PG_INTERNAL
PG_INTERVAL PG_INTERVALARRAY PG_LANGUAGE_HANDLER PG_LINE PG_LINEARRAY PG_LSEG
PG_LSEGARRAY PG_MACADDR PG_MACADDRARRAY PG_MONEY PG_MONEYARRAY PG_NAME
PG_NAMEARRAY PG_NUMERIC PG_NUMERICARRAY PG_OID PG_OIDARRAY PG_OIDVECTOR
PG_OIDVECTORARRAY PG_OPAQUE PG_PATH PG_PATHARRAY PG_PG_ATTRIBUTE PG_PG_CLASS
PG_PG_PROC PG_PG_TYPE PG_POINT PG_POINTARRAY PG_POLYGON PG_POLYGONARRAY
PG_RECORD PG_REFCURSOR PG_REFCURSORARRAY PG_REGCLASS PG_REGCLASSARRAY PG_REGCONFIG
PG_REGCONFIGARRAY PG_REGDICTIONARY PG_REGDICTIONARYARRAY PG_REGOPER PG_REGOPERARRAY PG_REGOPERATOR
PG_REGOPERATORARRAY PG_REGPROC PG_REGPROCARRAY PG_REGPROCEDURE PG_REGPROCEDUREARRAY PG_REGTYPE
PG_REGTYPEARRAY PG_RELTIME PG_RELTIMEARRAY PG_SMGR PG_TEXT PG_TEXTARRAY
PG_TID PG_TIDARRAY PG_TIME PG_TIMEARRAY PG_TIMESTAMP PG_TIMESTAMPARRAY
PG_TIMESTAMPTZ PG_TIMESTAMPTZARRAY PG_TIMETZ PG_TIMETZARRAY PG_TINTERVAL PG_TINTERVALARRAY
PG_TRIGGER PG_TSQUERY PG_TSQUERYARRAY PG_TSVECTOR PG_TSVECTORARRAY PG_TXID_SNAPSHOT
PG_TXID_SNAPSHOTARRAY PG_UNKNOWN PG_UUID PG_UUIDARRAY PG_VARBIT PG_VARBITARRAY
PG_VARCHAR PG_VARCHARARRAY PG_VOID PG_XID PG_XIDARRAY PG_XML
PG_XMLARRAY

Data types are "sticky," in that once a data type is set to a certain placeholder,
it will remain for that placeholder, unless it is explicitly set to something
else afterwards. If the statement has already been prepared, and you switch the
data type to something else, DBD::Pg will re-prepare the statement for you before
doing the next execute.

Examples:

  use DBI qw(:sql_types);
  use DBD::Pg qw(:pg_types);

  $SQL = "SELECT id FROM ptable WHERE size > ? AND title = ?";
  $sth = $dbh->prepare($SQL);

  ## Both arguments below are bound to placeholders as "varchar"
  $sth->execute(123, "Merk");

  ## Reset the datatype for the first placeholder to an integer
  $sth->bind_param(1, undef, SQL_INTEGER);

  ## The "undef" bound above is not used, since we supply params to execute
  $sth->execute(123, "Merk");

  ## Set the first placeholder's value and data type
  $sth->bind_param(1, 234, { pg_type => PG_TIMESTAMP });

  ## Set the second placeholder's value and data type.
  ## We don't send a third argument, so the default "varchar" is used
  $sth->bind_param('$2', "Zool");

  ## We realize that the wrong data type was set above, so we change it:
  $sth->bind_param('$1', 234, { pg_type => SQL_INTEGER });

  ## We also got the wrong value, so we change that as well.
  ## Because the data type is sticky, we don't need to change it
  $sth->bind_param(1, 567);

  ## This executes the statement with 567 (integer) and "Zool" (varchar)
  $sth->execute();

=item B<bind_param_inout>

Experimental support for this feature is provided. The first argument to 
bind_param_inout should be a placeholder number. The second argument 
should be a reference to a scalar variable in your script. The third argument 
is not used and should simply be set to 0. Note that what this really does is 
assign a returned column to the variable, in the order in which the column 
appears. For example:

  my $foo = 123;
  $sth = $dbh->prepare("SELECT 1+?::int");
  $sth->bind_param_inout(1, \$foo, 0);
  $foo = 222;
  $sth->execute(444);
  $sth->fetch;

The above will cause $foo to have a new value of "223" after the final fetch.
Note that the variables bound in this manner are very sticky, and will trump any 
values passed in to execute. This is because the binding is done as late as possible, 
at the execute() stage, allowing the value to be changed between the time it was bound 
and the time the query is executed. Thus, the above execute is the same as:

  $sth->execute();

=item B<bind_param_array>

Supported by this driver as proposed by DBI.

=item B<execute>

  $rv = $sth->execute(@bind_values);

Executes a previously prepared statement. In addition to C<UPDATE>, C<DELETE>,
C<INSERT> statements, for which it returns always the number of affected rows,
the C<execute> method can also be used for C<SELECT ... INTO table> statements.

The "prepare/bind/execute" process has changed significantly for PostgreSQL
servers 7.4 and later: please see the C<prepare()> and C<bind_param()> entries for
much more information.

Setting one of the bind_values to "undef" is the equivalent of setting the value 
to NULL in the database. Setting the bind_value to $DBDPG_DEFAULT is equivalent 
to sending the literal string 'DEFAULT' to the backend. Note that using this 
option will force server-side prepares off until such time as PostgreSQL 
supports using DEFAULT in prepared statements.

DBD::Pg also supports passing in arrays to execute: simply pass in an arrayref, 
and DBD::Pg will flatten it into a string suitable for input on the backend.

=item B<execute_array>

Supported by this driver as proposed by DBI.

=item B<execute_for_fetch>

Supported by this driver as proposed by DBI.

=item B<fetchrow_arrayref>

  $ary_ref = $sth->fetchrow_arrayref;

Supported by this driver as proposed by DBI.

=item B<fetchrow_array>

  @ary = $sth->fetchrow_array;

Supported by this driver as proposed by DBI.

=item B<fetchrow_hashref>

  $hash_ref = $sth->fetchrow_hashref;

Supported by this driver as proposed by DBI.

=item B<fetchall_arrayref>

  $tbl_ary_ref = $sth->fetchall_arrayref;

Implemented by DBI, no driver-specific impact.

=item B<finish>

  $rc = $sth->finish;

Supported by this driver as proposed by DBI.

=item B<rows>

  $rv = $sth->rows;

Supported by this driver as proposed by DBI. In contrast to many other drivers
the number of rows is available immediately after executing the statement.

=item B<bind_col>

  $rc = $sth->bind_col($column_number, \$var_to_bind, \%attr);

Supported by this driver as proposed by DBI.

=item B<bind_columns>

  $rc = $sth->bind_columns(\%attr, @list_of_refs_to_vars_to_bind);

Supported by this driver as proposed by DBI.

=item B<dump_results>

  $rows = $sth->dump_results($maxlen, $lsep, $fsep, $fh);

Implemented by DBI, no driver-specific impact.

=item B<blob_read>

  $blob = $sth->blob_read($id, $offset, $len);

Supported by this driver as proposed by DBI. Implemented by DBI but not
documented, so this method might change.

This method seems to be heavily influenced by the current implementation of
blobs in Oracle. Nevertheless we try to be as compatible as possible. Whereas
Oracle suffers from the limitation that blobs are related to tables and every
table can have only one blob (datatype LONG), PostgreSQL handles its blobs
independent of any table by using so-called object identifiers. This explains
why the C<blob_read> method is blessed into the STATEMENT package and not part of
the DATABASE package. Here the field parameter has been used to handle this
object identifier. The offset and len parameters may be set to zero, in which
case the driver fetches the whole blob at once.

Starting with PostgreSQL 6.5, every access to a blob has to be put into a
transaction. This holds even for a read-only access.

See also the PostgreSQL-specific functions concerning blobs, which are
available via the C<func> interface.

For further information and examples about blobs, please read the chapter
about Large Objects in the PostgreSQL Programmer's Guide at
L<http://www.postgresql.org/docs/current/static/largeobjects.html>.

=back

=head2 Statement Handle Attributes

=over 4

=item B<NUM_OF_FIELDS>  (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<NUM_OF_PARAMS>  (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<NAME>  (array-ref, read-only)

Supported by this driver as proposed by DBI.

=item B<NAME_lc>  (array-ref, read-only)

Implemented by DBI, no driver-specific impact.

=item B<NAME_uc>  (array-ref, read-only)

Implemented by DBI, no driver-specific impact.

=item B<NAME_hash>  (hash-ref, read-only)

Implemented by DBI, no driver-specific impact.

=item B<NAME_lc_hash>  (hash-ref, read-only)

Implemented by DBI, no driver-specific impact.

=item B<NAME_uc_hash>  (hash-ref, read-only)

Implemented by DBI, no driver-specific impact.

=item B<TYPE>  (array-ref, read-only)

Supported by this driver as proposed by DBI

=item B<PRECISION>  (array-ref, read-only)

Supported by this driver. C<NUMERIC> types will return the precision. Types of
C<CHAR> and C<VARCHAR> will return their size (number of characters). Other
types will return the number of I<bytes>.

=item B<SCALE>  (array-ref, read-only)

Supported by this driver as proposed by DBI. The only type
that will return a value currently is C<NUMERIC>.

=item B<NULLABLE>  (array-ref, read-only)

Supported by this driver as proposed by DBI.

=item B<CursorName>  (string, read-only)

Not supported by this driver. See the note about B<Cursors> elsewhere in this
document.

=item C<Database>  (dbh, read-only)

Implemented by DBI, no driver-specific impact.

=item C<ParamValues>  (hash ref, read-only)

Supported by this driver as proposed by DBI. If called before C<execute>, the
literal values passed in are returned. If called after C<execute>, then
the quoted versions of the values are shown.

=item C<ParamTypes>  (hash ref, read-only)

Returns a hash of all current placeholders. The keys are the names of the placeholders, 
and the values are the types that have been bound to each one. Placeholders that 
have not yet been bound will return undef as the value.

=item B<Statement>  (string, read-only)

Supported by this driver as proposed by DBI.

=item B<RowCache>  (integer, read-only)

Not supported by this driver.

=item B<pg_size>  (array-ref, read-only)

PostgreSQL specific attribute. It returns a reference to an array of integer
values for each column. The integer shows the size of the column in
bytes. Variable length columns are indicated by -1.

=item B<pg_type>  (array-ref, read-only)

PostgreSQL specific attribute. It returns a reference to an array of strings
for each column. The string shows the name of the data_type.

=item B<pg_oid_status> (integer, read-only)

PostgreSQL specific attribute. It returns the OID of the last INSERT command.

=item B<pg_cmd_status> (integer, read-only)

PostgreSQL specific attribute. It returns the type of the last
command. Possible types are: "INSERT", "DELETE", "UPDATE", "SELECT".

=back

=head1 FURTHER INFORMATION

=head2 Transactions

Transaction behavior is controlled via the C<AutoCommit> attribute. For a
complete definition of C<AutoCommit> please refer to the DBI documentation.

According to the DBI specification the default for C<AutoCommit> is a true
value. In this mode, any change to the database becomes valid immediately. Any
C<BEGIN>, C<COMMIT> or C<ROLLBACK> statements will be rejected. DBD::Pg
implements C<AutoCommit> by issuing a C<BEGIN> statement immediately before
executing a statement, and a C<COMMIT> afterwards.

=head2 Savepoints

PostgreSQL version 8.0 introduced the concept of savepoints, which allows 
transactions to be rolled back to a certain point without affecting the 
rest of the transaction. DBD::Pg encourages using the following methods to 
control savepoints:

=over 4

=item B<pg_savepoint>

Creates a savepoint. This will fail unless you are inside of a transaction. The 
only argument is the name of the savepoint. Note that PostgreSQL DOES allow 
multiple savepoints with the same name to exist.

  $dbh->pg_savepoint("mysavepoint");

=item B<pg_rollback_to>

Rolls the database back to a named savepoint, discarding any work performed after 
that point. If more than one savepoint with that name exists, rolls back to the 
most recently created one.

  $dbh->pg_rollback_to("mysavepoint");

=item B<pg_release>

Releases (or removes) a named savepoint. If more than one savepoint with that name 
exists, it will only destroy the most recently created one. Note that all savepoints 
created after the one being released are also destroyed.

  $dbh->pg_release("mysavepoint");

=back

=head2 Asynchronous Queries

It is possible to send a query to the backend and have your script do other work while the query is 
running on the backend. Both queries sent by the do() method, and by the execute() method can be 
sent asynchronously. (NOTE: This will only work if DBD::Pg has been compiled against Postgres libraries 
of version 8.0 or greater) The basic usage is as follows:

  use DBD::Pg ':async';

  print "Async do() example:\n";
  $dbh->do("SELECT long_running_query()", {pg_async => PG_ASYNC});
  do_something_else();
  {
    if ($dbh->pg_ready()) {
      $res = $pg_result();
      print "Result of do(): $res\n";
    }
    print "Query is still running...\n";
    if (cancel_request_received) {
      $dbh->pg_cancel();
    }
    sleep 1;
    redo;
  }

  print "Async prepare/execute example:\n";
  $sth = $dbh->prepare("SELECT long_running_query(1)", {pg_async => PG_ASYNC});
  $sth->execute();

  ## Changed our mind, cancel and run again:
  $sth = $dbh->prepare("SELECT 678", {pg_async => PG_ASYNC + PG_OLDQUERY_CANCEL});
  $sth->execute();

  do_something_else();

  if (!$sth->pg_ready) {
    do_another_thing();
  }

  ## We wait until it is done, and get the result:
  $res = $dbh->pg_result();

=head3 Asynchronous Constants

There are currently three asynchronous constants exported by DBD::Pg. You can import all of them by putting 
either of these at the top of your script:

  use DBD::Pg;

  use DBD::Pg ':async';

You may also use the numbers instead of the constants, but using the constants is recommended as it 
makes your script more readable.

=over 4

=item PG_ASYNC

This is a constant for the number 1. It is passed to either the do() or the prepare() method as a value 
to the pg_async key and indicates that the query should be sent asynchronously.

=item PG_OLDQUERY_CANCEL

This is a constant for the number 2. When passed to either the do() or the prepare method(), it causes any 
currently running asynchronous query to be cancelled and rolled back. It has no effect if no asynchronous 
query is currently running.

=item PG_OLDQUERY_WAIT

This is a constant for the number 4. When passed to either the do() or the prepare method(), it waits for any 
currently running asynchronous query to complete. It has no effect if there is no asynchronous query currently running.

=back

=head3 Asynchronous Methods

=over 4

=item pg_cancel

This database-level method attempts to cancel any currently running asynchronous query. It returns true if 
the cancel succeeded, and false otherwise. Note that a query that has finished before this method is executed 
will also return false. B<WARNING>: a successful cancellation will leave the database in an unusable state, 
so DBD::Pg will automatically clear out the error message and issue a ROLLBACK.

  $result = $dbh->pg_cancel();

=item pg_ready

This method can be called as a database handle method or (for convenience) as a statement handle method. Both simply 
see if a previously issued asynchronous query has completed yet. It returns true if the statement has finished, in which 
case you should then call the pg_result() method. Calls to pg_ready() should only be used when you have other 
things to do while the query is running. If you simply want to wait until the query is done, do not call pg_ready()
over and over, but simply call the pg_result() method.

  my $time = 0;
  while (!$dbh->pg_ready) {
    print "Query is still running. Seconds: $time\n";
    $time++;
    sleep 1;
  }
  $result = $dbh->pg_result;

=item pg_result

This database handle method returns the results of a previously issued asynchronous query. If the query is still 
running, this method will wait until it has finished. The result returned is the number of rows: the same thing 
that would have been returned by the asynchronous do() or execute() if it had been called without an asynchronous flag.

  $result = $dbh->pg_result;

=back

=head3 Asynchronous Examples

Here are some working examples of asynchronous queries. Note that we'll use the pg_sleep function to emulate a 
long-running query.

  use strict;
  use warnings;
  use Time::HiRes 'sleep';
  use DBD::Pg ':async';

  my $dbh = DBI->connect('dbi:Pg:dbname=postgres', 'postgres', '', {AutoCommit=>0,RaiseError=>1});

  ## Kick off a long running query on the first database:
  my $sth = $dbh->prepare("SELECT pg_sleep(?)", {pg_async => PG_ASYNC});
  $sth->execute(5);

  ## While that is running, do some other things
  print "Your query is processing. Thanks for waiting\n";
  check_on_the_kids(); ## Expensive sub, takes at least three seconds.

  while (!$dbh->pg_ready) {
    check_on_the_kids();
    ## If the above function returns quickly for some reason, we add a small sleep
    sleep 0.1;
  }

  print "The query has finished. Gathering results\n";
  my $result = $sth->pg_result;
  print "Result: $result\n";
  my $info = $sth->fetchall_arrayref();

Without asynchronous queries, the above script would take about 8 seconds to run: five seconds waiting 
for the execute to finish, then three for the check_on_the_kids() function to return. With asynchronous 
queries, the script takes about 6 seconds to run, and gets in two iterations of check_on_the_kids in 
the process.

Here's an example showing the ability to cancel a long-running query. Imagine two slave databases in 
different geographic locations over a slow network. You need information as quickly as possible, so 
you query both at once. When you get an answer, you tell the other one to stop working on your query, 
as you don't need it anymore.

  use strict;
  use warnings;
  use Time::HiRes 'sleep';
  use DBD::Pg ':async';

  my $dbhslave1 = DBI->connect('dbi:Pg:dbname=postgres;host=slave1', 'postgres', '', {AutoCommit=>0,RaiseError=>1});
  my $dbhslave2 = DBI->connect('dbi:Pg:dbname=postgres;host=slave2', 'postgres', '', {AutoCommit=>0,RaiseError=>1});

  $SQL = "SELECT count(*) FROM largetable WHERE flavor='blueberry'";

  my $sth1 = $dbhslave1->prepare($SQL, {pg_async => PG_ASYNC});
  my $sth2 = $dbhslave2->prepare($SQL, {pg_async => PG_ASYNC});

  $sth1->execute();
  $sth2->execute();

  my $winner;
  while (!defined $winner) {
    if ($sth1->pg_ready) {
      $winner = 1;
    }
    elsif ($sth2->pg_ready) {
      $winner = 2;
    }
    Time::HiRes::sleep 0.05;
  }

  my $count;
  if ($winner == 1) {
    $sth2->pg_cancel();
    $sth1->pg_result();
    $count = $sth1->fetchall_arrayref()->[0][0];
  }
  else {
    $sth1->pg_cancel();
    $sth2->pg_result();
    $count = $sth2->fetchall_arrayref()->[0][0];
  }

=head2 Array support

DBD::Pg allows arrays (as arrayrefs) to be passed in to both 
the quote() and the execute() functions. In both cases, the array is 
flattened into a string representing a Postgres array.

When fetching rows from a table that contains a column with an 
array type, the result will be passed back to your script as an arrayref.

To turn off the automatic parsing of returned arrays into arrayrefs, 
you can set the variable "pg_expand_array", which is true by default.

  $dbh->{pg_expand_array} = 0;


=head2 COPY support

DBD::Pg allows for the quick (bulk) reading and storing of data by using 
the COPY command. The basic process is to use $dbh->do to issue a 
COPY command, and then to either add rows using pg_putcopydata, or to 
read them by using pg_getcopydata.

The first step is to put the server into "COPY" mode. This is done by 
sending a complete COPY command to the server, by using the do() method. 
For example:

  $dbh->do("COPY foobar FROM STDIN");

This would tell the server to enter a COPY OUT state. It is now ready to 
receive information via the pg_putcopydata method. The complete syntax of the 
COPY command is more complex and not documented here: the canonical 
PostgreSQL documentation for COPY can be found at:

http://www.postgresql.org/docs/current/static/sql-copy.html

Once the COPY command has been issued, no other SQL commands are allowed 
until pg_putcopyend() has been issued, or the final pg_getcopydata has 
been called.

Note: All other COPY methods (pg_putline, pg_getline, etc.) are now 
deprecated in favor of the pg_getcopydata, pg_putcopydata, and 
pg_putcopyend methods.

=over 4

=item B<pg_getcopydata>

Used to retrieve data from a table after the server has been put into COPY OUT 
mode by calling "COPY tablename TO STDOUT". The first argument to pg_getcopydata 
is the variable into which the data will be stored (this variable should not 
be undefined, or it may throw a warning, although it may be a reference). This 
argument returns a number greater than 1 indicating the new size of the variable, 
or a -1 when the COPY has finished. Once a -1 has been returned, no other action is 
necessary, as COPY mode will have already terminated. Example:

  $dbh->do("COPY mytable TO STDOUT");
  my @data;
  my $x=0;
  1 while $dbh->pg_getcopydata($data[$x++]) > 0;

There is also a variation of this function called pg_getcopydata_async, which, 
as the name suggests, returns immediately. The only difference from the original 
function is that this version may return a 0, indicating that the row is not 
ready to be delivered yet. When this happens, the variable has not been changed, 
and you will need to call the function again until you get a non-zero result.

=item B<pg_putcopydata>

Used to put data into a table after the server has been put into COPY IN mode 
by calling "COPY tablename FROM STDIN". The only argument is the data you want 
inserted. Issue a pg_putcopyend() when you have added all your rows.

The default delimiter is a tab character, but this can be changed in 
the COPY statement. Returns a 1 on successful input. Examples:

  ## Simple example:
  $dbh->do("COPY mytable FROM STDIN");
  $dbh->pg_putcopydata("123\tPepperoni\t3\n");
  $dbh->pg_putcopydata("314\tMushroom\t8\n");
  $dbh->pg_putcopydata("6\tAnchovies\t100\n");
  $dbh->pg_putcopyend();

  ## This example uses explicit columns and a custom delimiter
  $dbh->do("COPY mytable(flavor, slices) FROM STDIN WITH DELIMITER '~'");
  $dbh->pg_putcopydata("Pepperoni~123\n");
  $dbh->pg_putcopydata("Mushroom~314\n");
  $dbh->pg_putcopydata("Anchovies~6\n");
  $dbh->pg_putcopyend();

=item B<pg_putcopyend>

When you are finished with pg_putcopydata, call pg_putcopyend to let the server know 
that you are done, and it will return to a normal, non-COPY state. Returns a 1 on 
success. This method will fail if called when not in a COPY IN or COPY OUT state. 

=back

=head2 Large Objects

This driver supports all largeobject functions provided by libpq via the
C<func> method. Please note that, starting with PostgreSQL 6.5, any access to
a large object -- even read-only large objects -- must be put into a
transaction!

=head2 Cursors

Although PostgreSQL supports cursors, they have not been used in the current
implementation. When DBD::Pg was defined, cursors in PostgreSQL could only be
used inside a transaction block. Because only one transaction block at a time
is allowed, this would have implied the restriction not to use any nested
C<SELECT> statements. Therefore the C<execute> method fetches all data at
once into data structures located in the front-end application. This fact
must to be considered when selecting large amounts of data!

You can use cursors in your application, but you'll need to do a little
work.  First you must declare your cursor.  Now you can issue queries against
the cursor, then select against your queries.  This typically results in a
double loop, like this:

  # WITH HOLD is not needed if AutoCommit is off
  $dbh->do("DECLARE csr CURSOR WITH HOLD FOR $sql");
  while (1) {
    my $sth = $dbh->prepare("fetch 1000 from csr");
    $sth->execute;
    last if 0 == $sth->rows;

    while (my $row = $sth->fetchrow_hashref) {
      # Do something with the data.
    }
  }
  $dbh->do("CLOSE csr");

=head2 Datatype bool

The current implementation of PostgreSQL returns 't' for true and 'f' for
false. From the Perl point of view, this is a rather unfortunate
choice. DBD::Pg therefore translates the result for the C<BOOL> data type in a
Perlish manner: 'f' -> '0' and 't' -> '1'. This way the application does
not have to check the database-specific returned values for the data-type
C<BOOL> because Perl treats '0' as false and '1' as true. You may set the
C<pg_bool_tf> attribute to a true value to change the values back to 't' and
'f' if you wish.

Boolean values can be passed to PostgreSQL as TRUE, 't', 'true', 'y', 'yes' or
'1' for true and FALSE, 'f', 'false', 'n', 'no' or '0' for false.

=head2 Schema support

The PostgreSQL schema concept may differ from those of other databases. In a nutshell, 
a schema is a named collection of objects within a single database. Please refer to the
PostgreSQL documentation for more details.

DBD::Pg does not provide explicit support for PostgreSQL schemas.
However, schema functionality may be used without any restrictions by
explicitly addressing schema objects, e.g.

  my $res = $dbh->selectall_arrayref("SELECT * FROM my_schema.my_table");

or by manipulating the schema search path with C<SET search_path>, e.g.

  $dbh->do("SET search_path TO my_schema, public");

=head1 SEE ALSO

L<DBI>

=head1 BUGS

Quoting will not work properly if standard_conforming_strings is set to 'on', 
so it is recommended that this be set to 'off' until full support for it can 
be built into DBD::Pg.

=head1 AUTHORS

DBI and DBD-Oracle by Tim Bunce (Tim.Bunce@ig.co.uk)

DBD-Pg by Edmund Mergl (E.Mergl@bawue.de) and Jeffrey W. Baker
(jwbaker@acm.org). By David Wheeler <david@justatheory.com>, Jason
Stewart <jason@openinformatics.com>, Bruce Momjian <pgman@candle.pha.pa.us>, 
Greg Sabino Mullane <greg@turnstep.com>, and others after v1.13.

Parts of this package have been copied from DBI and DBD-Oracle.

B<Mailing List>

The current maintainers may be reached through the 'dbd-pg' mailing list:
<dbd-pg@perl.org>

B<Bug Reports>

If you feel certain you have found a bug, you can report it by sending
an email to <bug-dbd-pg@rt.cpan.org>.

=head1 COPYRIGHT

The DBD::Pg module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 ACKNOWLEDGMENTS

See also B<DBI/ACKNOWLEDGMENTS>.

=cut

