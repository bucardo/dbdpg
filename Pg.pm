# -*-cperl-*-
#  $Id$
#
#  Copyright (c) 2002-2005 PostgreSQL Global Development Group
#  Portions Copyright (c) 2002 Jeffrey W. Baker
#  Portions Copyright (c) 1997-2001 Edmund Mergl
#  Portions Copyright (c) 1994-1997 Tim Bunce
#
#  You may distribute under the terms of either the GNU General Public
#  License or the Artistic License, as specified in the Perl README file.


use 5.006001;



{ package DBD::Pg;

	our $VERSION = '1.43_1';

	use DBI ();
	use DynaLoader ();
	use Exporter ();
	use vars qw(@ISA %EXPORT_TAGS $err $errstr $sqlstate $drh $dbh);
	@ISA = qw(DynaLoader Exporter);

	%EXPORT_TAGS = 
		(
		 pg_types => [qw(
			PG_BOOL PG_BYTEA PG_CHAR PG_INT8 PG_INT2 PG_INT4 PG_TEXT PG_OID
			PG_FLOAT4 PG_FLOAT8 PG_ABSTIME PG_RELTIME PG_TINTERVAL PG_BPCHAR
			PG_VARCHAR PG_DATE PG_TIME PG_DATETIME PG_TIMESPAN PG_TIMESTAMP
		)]
	);

	Exporter::export_ok_tags('pg_types');

	require_version DBI 1.38;

	bootstrap DBD::Pg $VERSION;

	$err = 0;		    # holds error code for DBI::err
	$errstr = "";	  # holds error string for DBI::errstr
	$sqlstate = ""; # holds five character SQLSTATE code
	$drh = undef;	  # holds driver handle once initialized

	sub CLONE {
		$drh = undef;
	}


	sub driver {
		return $drh if defined $drh;
		my($class, $attr) = @_;

		$class .= "::dr";

		# not a 'my' since we use it above to prevent multiple drivers

		$drh = DBI::_new_drh($class, {
			'Name' => 'Pg',
			'Version' => $VERSION,
			'Err' => \$DBD::Pg::err,
			'Errstr' => \$DBD::Pg::errstr,
			'State' => \$DBD::Pg::sqlstate,
			'Attribution' => 'PostgreSQL DBD by Edmund Mergl',
		});


		DBD::Pg::db->install_method("pg_endcopy");
		DBD::Pg::db->install_method("pg_getline");
		DBD::Pg::db->install_method("pg_ping");
		DBD::Pg::db->install_method("pg_putline");
		DBD::Pg::db->install_method("pg_release");
		DBD::Pg::db->install_method("pg_rollback_to");
		DBD::Pg::db->install_method("pg_savepoint");
		DBD::Pg::db->install_method("pg_server_trace");
		DBD::Pg::db->install_method("pg_server_untrace");
		DBD::Pg::db->install_method("pg_type_info");

		$drh;

	}


 	## Deprecated: use $dbh->{pg_server_version} if possible instead
 	sub _pg_use_catalog {
		my $dbh = shift;
 		return $dbh->{private_dbdpg}{pg_use_catalog} if defined $dbh->{private_dbdpg}{pg_use_catalog};
 		$dbh->{private_dbdpg}{pg_use_catalog} = $dbh->{private_dbdpg}{version} >= 70300 ? 'pg_catalog.' : '';
 	}


	1;
}


{ package DBD::Pg::dr;

	use strict;

	our $CATALOG = 123; ## Set later on, this is to catch seriously misplaced code


	## Returns an array of formatted database names from the pg_database table
	sub data_sources {
		my $drh = shift;
		my $dbh = DBD::Pg::dr::connect($drh, 'dbname=template1') or return undef;
		$dbh->{AutoCommit}=1;
		my $SQL = "SELECT ${CATALOG}quote_ident(datname) FROM ${CATALOG}pg_database ORDER BY 1";
		my $sth = $dbh->prepare($SQL);
		$sth->execute() or die $DBI::errstr;
		my @sources = map { "dbi:Pg:dbname=$_->[0]" } @{$sth->fetchall_arrayref()};
		$dbh->disconnect;
		return @sources;
	}


	sub connect {
		my($drh, $dbname, $user, $pass)= @_;

		## Allow "db" and "database" as synonyms for "dbname"
		$dbname =~ s/\b(?:db|database)\s*=/dbname=/;
	
		my $Name = $dbname;
		if ($dbname =~ m#dbname\s*=\s*[\"\']([^\"\']+)#) {
			$Name = "'$1'";
			$dbname =~ s/"/'/g;
		}
		elsif ($dbname =~ m#dbname\s*=\s*([^;]+)#) {
			$Name = $1;
		}
	
 		$user = "" unless defined($user);
		$pass = "" unless defined($pass);

		$user = $ENV{DBI_USER} if $user eq "";
		$pass = $ENV{DBI_PASS} if $pass eq "";

		$user = "" unless defined($user);
		$pass = "" unless defined($pass);

		my ($dbh) = DBI::_new_dbh($drh, {
			'Name' => $Name,
			'User' => $user, 'CURRENT_USER' => $user,
		});

		# Connect to the database..
		DBD::Pg::db::_login($dbh, $dbname, $user, $pass) or return undef;

		my $version = $dbh->{pg_server_version};
		$dbh->{private_dbdpg}{version} = $version;

		## If the version is 7.3 or later, fully qualify the system relations
		$CATALOG = $version >= 70300 ? 'pg_catalog.' : '';

		$dbh;
	}

}


{ package DBD::Pg::db;

	use strict;


	sub prepare {
		my($dbh, $statement, @attribs) = @_;

		# Create a 'blank' statement handle:
		my $sth = DBI::_new_sth($dbh, {
			'Statement' => $statement,
		});

		my $ph = DBD::Pg::st::_prepare($sth, $statement, @attribs) || 0;

		if ($ph < 0) {
			return undef;
		}

		if (@attribs and ref $attribs[0] and ref $attribs[0] eq 'HASH') {
			# Feel ambitious? Move all this to dbdimp.c! :)
			if (exists $attribs[0]->{bind_types}) {
				my $bind = $attribs[0]->{bind_types};
				## Until we are allowed to set just the type, we use a null
				$sth->bind_param("$1",undef,"foo");
			}
		}

		$sth;
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
				return $dbh->set_err(1, "last_insert_id must be passed a hashref as the final argument");
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
				return $dbh->set_err(1, "last_insert_id needs at least a sequence or table name");
			}
			my @args = ($table);

			## Only 7.3 and up can use schemas
			$schema = '' if $dbh->{private_dbdpg}{version} < 70300;

			## Make sure the table in question exists and grab its oid
			my ($schemajoin,$schemawhere) = ('','');
			if (length $schema) {
				$schemajoin = "\n JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)";
				$schemawhere = "\n AND n.nspname = ?";
				push @args, $schema;
			}
			$SQL = "SELECT c.oid FROM ${DBD::Pg::dr::CATALOG}pg_class c $schemajoin\n WHERE relname = ?$schemawhere";
			$sth = $dbh->prepare($SQL);
			$count = $sth->execute(@args);
			if (!defined $count or $count eq '0E0') {
				$sth->finish();
				my $message = qq{Could not find the table "$table"};
				length $schema and $message .= qq{ in the schema "$schema"};
				return $dbh->set_err(1, $message);
			}
			my $oid = $sth->fetchall_arrayref()->[0][0];
			## This table has a primary key. Is there a sequence associated with it via a unique, indexed column?
			$SQL = "SELECT a.attname, i.indisprimary, substring(d.adsrc for 128) AS def\n".
				"FROM ${DBD::Pg::dr::CATALOG}pg_index i, ${DBD::Pg::dr::CATALOG}pg_attribute a, ${DBD::Pg::dr::CATALOG}pg_attrdef d\n ".
					"WHERE i.indrelid = $oid AND d.adrelid=a.attrelid AND d.adnum=a.attnum\n".
						"  AND a.attrelid=$oid AND i.indisunique IS TRUE\n".
							"  AND a.atthasdef IS TRUE AND i.indkey[0]=a.attnum\n".
								" AND d.adsrc ~ '^nextval'";
			$sth = $dbh->prepare($SQL);
			$count = $sth->execute();
			if (!defined $count or $count eq '0E0') {
				$sth->finish();
				$dbh->set_err(1, qq{No suitable column found for last_insert_id of table "$table"});
			}
			my $info = $sth->fetchall_arrayref();

			## We have at least one with a default value. See if we can determine sequences
			my @def;
			for (@$info) {
				next unless $_->[2] =~ /^nextval\('([^']+)'::/o;
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
					$dbh->set_Err(1, qq{No suitable column found for last_insert_id of table "$table"\n});
				}
				@def = @pri;
			}
			$sequence = $def[0]->[3];
			## Cache this information for subsequent calls
			$dbh->{private_dbdpg}{$cachename} = $sequence;
		}

		$sth = $dbh->prepare("SELECT currval(?)");
		$sth->execute($sequence);
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

		my $version = $dbh->{private_dbdpg}{version};

		my @search;
		## If the schema or table has an underscore or a %, use a LIKE comparison
		if (defined $schema and length $schema and $version >= 70300) {
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

		my $showschema = $version >= 70300 ? "quote_ident(n.nspname)" : "NULL::text";

		my $schemajoin = $version >= 70300 ?
			"JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)" : "";

		# col_description is not available for Pg < 7.2
		my $remarks = $version > 70200 ? 
			"${DBD::Pg::dr::CATALOG}col_description(a.attrelid, a.attnum)" : "NULL::text";

		my $col_info_sql = qq!
			SELECT
				NULL::text AS "TABLE_CAT"
				, $showschema AS "TABLE_SCHEM"
				, quote_ident(c.relname) AS "TABLE_NAME"
				, quote_ident(a.attname) AS "COLUMN_NAME"
				, a.atttypid AS "DATA_TYPE"
				, ${DBD::Pg::dr::CATALOG}format_type(a.atttypid, NULL) AS "TYPE_NAME"
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
				, ${DBD::Pg::dr::CATALOG}format_type(a.atttypid, a.atttypmod) AS "pg_type"
				, a.attrelid AS "pg_attrelid"
				, a.attnum AS "pg_attnum"
				, a.atttypmod AS "pg_atttypmod"
			FROM
				${DBD::Pg::dr::CATALOG}pg_type t
				JOIN ${DBD::Pg::dr::CATALOG}pg_attribute a ON (t.oid = a.atttypid)
				JOIN ${DBD::Pg::dr::CATALOG}pg_class c ON (a.attrelid = c.oid)
				LEFT JOIN ${DBD::Pg::dr::CATALOG}pg_attrdef af ON (a.attnum = af.adnum AND a.attrelid = af.adrelid)
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
			/);

		my $oldconstraint_sth;
		if ($version < 70300) {
			my $constraint_query = "SELECT rcsrc FROM pg_relcheck WHERE rcname = ?";
			$oldconstraint_sth = $dbh->prepare($constraint_query);
		}

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
			if ($version >= 70300) {
				my $SQL = "SELECT consrc FROM pg_catalog.pg_constraint WHERE contype = 'c' AND ".
					"conrelid = $aid AND conkey = '{$attnum}'";
				my $info = $dbh->selectall_arrayref($SQL);
				if (@$info) {
					$row->[19] = $info->[0][0];
				}
				else {
					$row->[19] = undef;
				}
			}
			else {
				$oldconstraint_sth->execute("$row->[$col_map{TABLE_NAME}]_$row->[$col_map{COLUMN_NAME}]");
				($row->[19]) = $oldconstraint_sth->fetchrow_array;
			}
			$col_map{pg_constraint} = 19;
		}

		# get rid of atttypmod that we no longer need
		delete $col_map{pg_atttypmod};

		# Since we've processed the data in Perl, we have to jump through a hoop
		# To turn it back into a statement handle
		#
		my $sth = _prepare_from_data(
			'column_info',
			$data,
				[ sort { $col_map{$a} <=> $col_map{$b} } keys %col_map]);
	}

	sub _prepare_from_data {
		my ($statement, $data, $names, %attr) = @_;
		my $sponge = DBI->connect("dbi:Sponge:","","",{ RaiseError => 1 });
		my $sth = $sponge->prepare($statement, { rows=>$data, NAME=>$names, %attr });
		return $sth;
	}

	sub primary_key_info {

		my $dbh = shift;
		my ($catalog, $schema, $table, $attr) = @_;

		## Catalog is ignored, but table is mandatory
		return undef unless defined $table and length $table;

		my $version = $dbh->{private_dbdpg}{version};
		my $whereclause = "AND c.relname = " . $dbh->quote($table);

		my $gotschema = $version >= 70300 ? 1 : 0;
		if (defined $schema and length $schema and $gotschema) {
			$whereclause .= "\n\t\t\tAND n.nspname = " . $dbh->quote($schema);
		}
		my $showschema = $gotschema ? "quote_ident(n.nspname)" : "NULL::text";
		my $schemajoin = $gotschema ? 
			"LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)" : "";
		my $showtablespace = '';
		my $tablespacejoin = '';
		if ($version >= 70500) {
			$tablespacejoin = 'LEFT JOIN pg_catalog.pg_tablespace t ON (t.oid = c.reltablespace)';
			$showtablespace = ', quote_ident(t.spcname), quote_ident(t.spclocation)';
		}
		my $pri_key_sql = qq{
			SELECT
				  c.oid
				, $showschema
				, quote_ident(c.relname)
				, quote_ident(c2.relname)
				, i.indkey $showtablespace
			FROM
				${DBD::Pg::dr::CATALOG}pg_class c
				JOIN ${DBD::Pg::dr::CATALOG}pg_index i ON (i.indrelid = c.oid)
				JOIN ${DBD::Pg::dr::CATALOG}pg_class c2 ON (c2.oid = i.indexrelid)
				$schemajoin $tablespacejoin
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
			SELECT a.attnum, ${DBD::Pg::dr::CATALOG}quote_ident(a.attname) AS colname,
				${DBD::Pg::dr::CATALOG}quote_ident(t.typname) AS typename
			FROM ${DBD::Pg::dr::CATALOG}pg_attribute a, ${DBD::Pg::dr::CATALOG}pg_type t
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
				if ($tablespacejoin) {
					$pkinfo->[$x][7] = $info->[5];
					$pkinfo->[$x][8] = $info->[6];
				}
				$x++;
			}
		}
		else { ## Nicer way: return only one row

			# TABLE_CAT
			$info->[0] = undef;
			# TABLESPACES
			if ($tablespacejoin) {
				$info->[7] = $info->[5];
				$info->[8] = $info->[6];
			}
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
		push @cols, 'pg_tablespace_name', 'pg_tablespace_location' if $tablespacejoin;

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

		## Versions 7.2 or less have no pg_constraint table, so we cannot support
		my $version = $dbh->{private_dbdpg}{version};
		return undef unless $version >= 70300;

		my $C = 'pg_catalog.';

		## If only the primary table is given, we return only those columns
		## that are used as foreign keys, even if that means that we return
		## unique keys but not primary one. We also return all the foreign
		## tables/columns that are referencing them, of course.

		## The first step is to find the oid of each specific table in the args:
		## Return undef if no matching relation found
		my %oid;
		for ([$ptable, $pschema, 'P'], [$ftable, $fschema, 'F']) {
			if (length $_->[0]) {
				my $SQL = "SELECT c.oid AS schema FROM ${C}pg_class c, ${C}pg_namespace n\n".
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
			$WHERE .= " AND conrelid IN (SELECT DISTINCT confrelid FROM ${C}pg_constraint WHERE conrelid=$oid{'F'}::oid)";
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
			${C}quote_ident(c.relname) AS t_name, ${C}quote_ident(n2.nspname) AS t_schema,
			${C}quote_ident(n.nspname) AS c_schema, ${C}quote_ident(conname) AS c_name,
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
			FROM ${C}pg_constraint k, ${C}pg_class c, ${C}pg_namespace n, ${C}pg_namespace n2
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
		for (@$info) {
			$colnum{$_->{'conrelid'}}{$1}++ while $_->{'conkey'} =~ /(\d+)/go;
			if ($_->{'contype'} eq 'f') {
				$colnum{$_->{'confrelid'}}{$1}++ while $_->{'confkey'} =~ /(\d+)/go;
			}
		}

		## Get the information about the columns computed above
		my $SQL = qq{
			SELECT a.attrelid, a.attnum, ${C}quote_ident(a.attname) AS colname, 
				${C}quote_ident(t.typname) AS typename
			FROM ${C}pg_attribute a, ${C}pg_type t
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
			my $multi = join ' ' => sort split/\s*/, $c->{'conkey'};
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
			my $multi = join ' ' => sort split/\s*/, $t->{'confkey'};
			if (exists $ukey{$t->{'confrelid'}}{$multi}) {
				$u = $ukey{$t->{'confrelid'}}{$multi}->[0];
			}
			else {
				## Mark this as an index so we can fudge things later on
				$multi = "index";
				## Grab the first one found, modify later on as needed
				$u = (values %{$ukey{$t->{'confrelid'}}})[0]->[0];
			}

			## ODBC is primary keys only
			next if $odbc and ($u->{'contype'} ne 'p' or $multi eq 'index');

			my (@conkey, @confkey);
			push (@conkey, $1) while $t->{'conkey'} =~ /(\d+)/go;
			push (@confkey, $1) while $t->{'confkey'} =~ /(\d+)/go;
			for (my $y=0; $conkey[$y]; $y++) {
				# UK_TABLE_CAT
				$fkinfo->[$x][0] = undef;
				# UK_TABLE_SCHEM
				$fkinfo->[$x][1] = $u->{'t_schema'};
				# UK_TABLE_NAME
				$fkinfo->[$x][2] = $u->{'t_name'};
				# UK_COLUMN_NAME
				$fkinfo->[$x][3] = $attinfo{$t->{'confrelid'}}{$confkey[$y]}{'colname'};
				# FK_TABLE_CAT
				$fkinfo->[$x][4] = undef;
				# FK_TABLE_SCHEM
				$fkinfo->[$x][5] = $t->{'t_schema'};
				# FK_TABLE_NAME
				$fkinfo->[$x][6] = $t->{'t_name'};
				# FK_COLUMN_NAME
				$fkinfo->[$x][7] = $attinfo{$t->{'conrelid'}}{$conkey[$y]}{'colname'};
				# ORDINAL_POSITION
				$fkinfo->[$x][8] = $conkey[$y];
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
				$fkinfo->[$x][15] = $attinfo{$t->{'confrelid'}}{$confkey[$y]}{'typename'};
				# FK_DATA_TYPE
				$fkinfo->[$x][16] = $attinfo{$t->{'conrelid'}}{$conkey[$y]}{'typename'};
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

		my $version = $dbh->{private_dbdpg}{version};

		if ( # Rule 19a
				(defined $catalog and $catalog eq '%')
				and (defined $schema and $schema eq '')
				and (defined $table and $table eq '')
			 ) {
			$tbl_sql = q{
					SELECT
						 NULL::text AS "TABLE_CAT"
					 , NULL::text AS "TABLE_SCHEM"
					 , NULL::text AS "TABLE_NAME"
					 , NULL::text AS "TABLE_TYPE"
					 , NULL::text AS "REMARKS"
					};
		}
		elsif (# Rule 19b
					 (defined $catalog and $catalog eq '')
					 and (defined $schema and $schema eq '%')
					 and (defined $table and $table eq '')
					) {
			$tbl_sql = $version >= 70300 ?
				q{SELECT
						 NULL::text AS "TABLE_CAT"
					 , quote_ident(n.nspname) AS "TABLE_SCHEM"
					 , NULL::text AS "TABLE_NAME"
					 , NULL::text AS "TABLE_TYPE"
					 , CASE WHEN n.nspname ~ '^pg_' THEN 'system schema' ELSE 'owned by ' || pg_get_userbyid(n.nspowner) END AS "REMARKS"
					FROM pg_catalog.pg_namespace n
					ORDER BY "TABLE_SCHEM"
					} :
						q{SELECT
						 NULL::text AS "TABLE_CAT"
					 , NULL::text AS "TABLE_SCHEM"
					 , NULL::text AS "TABLE_NAME"
					 , NULL::text AS "TABLE_TYPE"
					 , NULL::text AS "REMARKS"
				};
		}
		elsif (# Rule 19c
					 (defined $catalog and $catalog eq '')
					 and (defined $schema and $schema eq '')
					 and (defined $table and $table eq '')
					 and (defined $type and $type eq '%')
					) {
			$tbl_sql = q{
					SELECT
					   NULL::text AS "TABLE_CAT"
					 , NULL::text AS "TABLE_SCHEM"
					 , NULL::text AS "TABLE_NAME"
					 , 'TABLE'    AS "TABLE_TYPE"
					 , 'relkind: r' AS "REMARKS"
					UNION
					SELECT
					   NULL::text AS "TABLE_CAT"
					 , NULL::text AS "TABLE_SCHEM"
					 , NULL::text AS "TABLE_NAME"
					 , 'VIEW'     AS "TABLE_TYPE"
					 , 'relkind: v' AS "REMARKS"
				};
		}
		else {
			# Default SQL
			my $showschema = "NULL::text";
			my $schemajoin = '';
			my $has_objsubid = '';
			my $tablespacejoin = '';
			my $showtablespace = '';
			my @search;
			if ($version >= 70300) {
				$showschema = "quote_ident(n.nspname)";
				$schemajoin = "LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)";
				$has_objsubid = "AND d.objsubid = 0";
			}
			if ($version >= 70500) {
				$tablespacejoin = 'LEFT JOIN pg_catalog.pg_tablespace t ON (t.oid = c.reltablespace)';
				$showtablespace = ', quote_ident(t.spcname) AS "pg_tablespace_name", quote_ident(t.spclocation) AS "pg_tablespace_location"';
			}

			## If the schema or table has an underscore or a %, use a LIKE comparison
			if (defined $schema and length $schema and $version >= 70300) {
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

			my $whereclause = join "\n\t\t\t\t\t AND " => @search;
			my $schemacase = $version >= 70300 ? "quote_ident(n.nspname)" : "quote_ident(c.relname)";
			$tbl_sql = qq{
				SELECT NULL::text AS "TABLE_CAT"
					 , $showschema AS "TABLE_SCHEM"
					 , quote_ident(c.relname) AS "TABLE_NAME"
					 , CASE
					 		WHEN c.relkind = 'v' THEN
								CASE WHEN $schemacase ~ '^pg_' THEN 'SYSTEM VIEW' ELSE 'VIEW' END
							ELSE
								CASE WHEN $schemacase ~ '^pg_' THEN 'SYSTEM TABLE' ELSE 'TABLE' END
						END AS "TABLE_TYPE"
					 , d.description AS "REMARKS" $showtablespace
				FROM ${DBD::Pg::dr::CATALOG}pg_class AS c
					LEFT JOIN ${DBD::Pg::dr::CATALOG}pg_description AS d
						ON (c.relfilenode = d.objoid $has_objsubid)
					$schemajoin $tablespacejoin
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
			my $version = $dbh->{private_dbdpg}{version};
			my @tables = map { ($version >= 70300
					and (! (ref $attr eq "HASH" and $attr->{pg_noprefix}))) ?
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
			NULLABLE 	    => 'NOTNULL',
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

	my $names = {
		TYPE_NAME         => 0,
		DATA_TYPE         => 1,
		COLUMN_SIZE       => 2,    # was PRECISION originally
		LITERAL_PREFIX    => 3,
		LITERAL_SUFFIX    => 4,
		CREATE_PARAMS     => 5,
		NULLABLE          => 6,
		CASE_SENSITIVE    => 7,
		SEARCHABLE        => 8,
		UNSIGNED_ATTRIBUTE=> 9,
		FIXED_PREC_SCALE  => 10,   # was MONEY originally
		AUTO_UNIQUE_VALUE => 11,   # was AUTO_INCREMENT originally
		LOCAL_TYPE_NAME   => 12,
		MINIMUM_SCALE     => 13,
		MAXIMUM_SCALE     => 14,
		NUM_PREC_RADIX    => 15,
    SQL_DATA_TYPE     => 16,
    SQL_DATETIME_SUB  => 17,
    INTERVAL_PRECISION=> 18,
	};


	#  typname       |typlen|typprtlen|    SQL92
	#  --------------+------+---------+    -------
	#  bool          |     1|        1|    BOOLEAN
	#  text          |    -1|       -1|    like VARCHAR, but automatic storage allocation
	#  bpchar        |    -1|       -1|    CHARACTER(n)    bp=blank padded
	#  varchar       |    -1|       -1|    VARCHAR(n)
	#  int2          |     2|        5|    SMALLINT
	#  int4          |     4|       10|    INTEGER
	#  int8          |     8|       20|    /
	#  money         |     4|       24|    /
	#  float4        |     4|       12|    FLOAT(p)   for p<7=float4, for p<16=float8
	#  float8        |     8|       24|    REAL
	#  abstime       |     4|       20|    /
	#  reltime       |     4|       20|    /
	#  tinterval     |    12|       47|    /
	#  date          |     4|       10|    /
	#  time          |     8|       16|    /
	#  datetime      |     8|       47|    /
	#  timespan      |    12|       47|    INTERVAL
	#  timestamp     |     4|       19|    TIMESTAMP
	#  --------------+------+---------+

		# DBI type definitions / PostgreSQL definitions     # type needs to be DBI-specific (not pg_type)
		#
		# SQL_ALL_TYPES  0
		# SQL_CHAR       1  1042 bpchar
		# SQL_NUMERIC    2   700 float4
		# SQL_DECIMAL    3   700 float4
		# SQL_INTEGER    4    23 int4
		# SQL_SMALLINT   5    21 int2
		# SQL_FLOAT      6   700 float4
		# SQL_REAL       7   701 float8
		# SQL_DOUBLE     8    20 int8
		# SQL_DATE       9  1082 date
		# SQL_TIME      10  1083 time
		# SQL_TIMESTAMP 11  1296 timestamp
		# SQL_VARCHAR   12  1043 varchar

	my $ti = [
		$names,
		# name          type  prec  prefix suffix  create params null case se unsign mon  incr       local   min    max
		#
		[ 'bytea',        -2, 4096,  '\'',  '\'',           undef, 1, '1', 3, undef, '0', '0',     'BYTEA', undef, undef, undef, undef, undef, undef ],
		[ 'bool',          0,    1,  '\'',  '\'',           undef, 1, '0', 2, undef, '0', '0',   'BOOLEAN', undef, undef, undef, undef, undef, undef ],
		[ 'int8',          8,   20, undef, undef,           undef, 1, '0', 2,   '0', '0', '0',   'LONGINT', undef, undef, undef, undef, undef, undef ],
		[ 'int2',          5,    5, undef, undef,           undef, 1, '0', 2,   '0', '0', '0',  'SMALLINT', undef, undef, undef, undef, undef, undef ],
		[ 'int4',          4,   10, undef, undef,           undef, 1, '0', 2,   '0', '0', '0',   'INTEGER', undef, undef, undef, undef, undef, undef ],
		[ 'text',         12, 4096,  '\'',  '\'',           undef, 1, '1', 3, undef, '0', '0',      'TEXT', undef, undef, undef, undef, undef, undef ],
		[ 'float4',        6,   12, undef, undef,     'precision', 1, '0', 2,   '0', '0', '0',     'FLOAT', undef, undef, undef, undef, undef, undef ],
		[ 'float8',        7,   24, undef, undef,     'precision', 1, '0', 2,   '0', '0', '0',      'REAL', undef, undef, undef, undef, undef, undef ],
		[ 'abstime',      10,   20,  '\'',  '\'',           undef, 1, '0', 2, undef, '0', '0',   'ABSTIME', undef, undef, undef, undef, undef, undef ],
		[ 'reltime',      10,   20,  '\'',  '\'',           undef, 1, '0', 2, undef, '0', '0',   'RELTIME', undef, undef, undef, undef, undef, undef ],
		[ 'tinterval',    11,   47,  '\'',  '\'',           undef, 1, '0', 2, undef, '0', '0', 'TINTERVAL', undef, undef, undef, undef, undef, undef ],
		[ 'money',         0,   24, undef, undef,           undef, 1, '0', 2, undef, '1', '0',     'MONEY', undef, undef, undef, undef, undef, undef ],
		[ 'bpchar',        1, 4096,  '\'',  '\'',    'max length', 1, '1', 3, undef, '0', '0', 'CHARACTER', undef, undef, undef, undef, undef, undef ],
		[ 'bpchar',       12, 4096,  '\'',  '\'',    'max length', 1, '1', 3, undef, '0', '0', 'CHARACTER', undef, undef, undef, undef, undef, undef ],
		[ 'varchar',      12, 4096,  '\'',  '\'',    'max length', 1, '1', 3, undef, '0', '0',   'VARCHAR', undef, undef, undef, undef, undef, undef ],
		[ 'date',          9,   10,  '\'',  '\'',           undef, 1, '0', 2, undef, '0', '0',      'DATE', undef, undef, undef, undef, undef, undef ],
		[ 'time',         10,   16,  '\'',  '\'',           undef, 1, '0', 2, undef, '0', '0',      'TIME', undef, undef, undef, undef, undef, undef ],
		[ 'datetime',     11,   47,  '\'',  '\'',           undef, 1, '0', 2, undef, '0', '0',  'DATETIME', undef, undef, undef, undef, undef, undef ],
		[ 'timespan',     11,   47,  '\'',  '\'',           undef, 1, '0', 2, undef, '0', '0',  'INTERVAL', undef, undef, undef, undef, undef, undef ],
		[ 'timestamp',    10,   19,  '\'',  '\'',           undef, 1, '0', 2, undef, '0', '0', 'TIMESTAMP', undef, undef, undef, undef, undef, undef ]
		#
		# intentionally omitted: char, all geometric types, all array types
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

		my $version = $dbh->{private_dbdpg}{version};

		my %type = (

## Basic information:

    6  => ["SQL_DRIVER_NAME",                'DBD/Pg.pm',         ],
   17  => ["SQL_DBMS_NAME",                  'PostgreSQL'         ],
   18  => ["SQL_DBMS_VERSION",               'ODBCVERSION'        ],
   29  => ["SQL_IDENTIFIER_QUOTE_CHAR",      '"'                  ],
   47  => ["SQL_USER_NAME",                  $dbh->{CURRENT_USER} ],

## Size limits

   30  => ["SQL_MAX_COLUMN_NAME_LEN",        'NAMEDATALEN'        ],
   32  => ["SQL_MAX_SCHEMA_NAME_LEN",        'NAMEDATALEN'        ],
   34  => ["SQL_MAX_CATALOG_NAME_LEN",       0                    ],
   35  => ["SQL_MAX_TABLE_NAME_LEN",         'NAMEDATALEN'        ],
   97  => ["SQL_MAX_COLUMNS_IN_GROUP_BY",    0                    ],
   98  => ["SQL_MAX_COLUMNS_IN_INDEX",       0                    ],
   99  => ["SQL_MAX_COLUMNS_IN_ORDER_BY",    0                    ],
  100  => ["SQL_MAX_COLUMNS_IN_SELECT",      0                    ],
  101  => ["SQL_MAX_COLUMNS_IN_TABLE",       0                    ],
  102  => ["SQL_MAX_INDEX_SIZE",             0                    ],
  104  => ["SQL_MAX_ROW_SIZE",               0                    ],
  105  => ["SQL_MAX_STATEMENT_LEN",          0                    ],
  106  => ["SQL_MAX_TABLES_IN_SELECT",       0                    ],
  107  => ["SQL_MAX_USER_NAME_LEN",          'NAMEDATALEN'        ],
  108  => ["SQL_MAX_STATEMENT_LEN",          0                    ],
  109  => ["SQL_MAX_STATEMENT_LEN",          0                    ],
  105  => ["SQL_MAX_STATEMENT_LEN",          0                    ],
  105  => ["SQL_MAX_STATEMENT_LEN",          0                    ],
  112  => ["SQL_MAX_BINARY_LITERAL_LEN",     0                    ],
10005  => ["SQL_MAX_IDENTIFIER_LEN",         'NAMEDATALEN'        ],

## Catalog support

   41  => ["SQL_CATALOG_NAME_SEPARATOR",     ''                   ],
   42  => ["SQL_CATALOG_TERM",               ''                   ],
  114  => ["SQL_CATALOG_LOCATION",           0                    ],
10003  => ["SQL_CATALOG_NAME",               'N'                  ],

## Domain support

  117  => ["SQL_ALTER_DOMAIN",               0                    ],
  130  => ["SQL_CREATE_DOMAIN",              0                    ],
  139  => ["SQL_DROP_DOMAIN",                0                    ],

## Schema support (7.3 and up)

   39  => ["SQL_SCHEMA_TERM",                'schema'             ],
   91  => ["SQL_SCHEMA_USAGE",               'SCHEMAUSAGE'        ],
  131  => ["SQL_CREATE_SCHEMA",              'CREATESCHEMA'       ],
  140  => ["SQL_DROP_SCHEMA",                'DROPSCHEMA'         ],

## Various

    2  => ["SQL_DATA_SOURCE_NAME",           'SOURCENAME'         ],
    7  => ["SQL_DRIVER_VER",                 'DBDVERSION'         ],
   13  => ["SQL_SERVER_NAME",                $dbh->{Name}         ],
   14  => ["SQL_SEARCH_PATTERN_ESCAPE",      '\\'                 ],
   22  => ["SQL_CONCAT_NULL_BEHAVIOR",       0                    ], ## SQL_CB_NULL
   28  => ["SQL_IDENTIFIER_CASE",            4                    ], ## SQL_IC_MIXED
   40  => ["SQL_PROCEDURE_TERM",             'Function'           ],
   45  => ["SQL_TABLE_TERM",                 'Table'              ],
   46  => ["SQL_TXN_CAPABLE",                4                    ], ## SQL_TC_ALL
   87  => ["SQL_COLUMN_ALIAS",               'Y'                  ],
   90  => ["SQL_ORDER_BY_COLUMNS_IN_SELECT", 'N'                  ],
   93  => ["SQL_QUOTED_IDENTIFIER_CASE",     3                    ], ## SQL_IC_SENSITIVE
  113  => ["SQL_LIKE_ESCAPE_CLAUSE",         'Y'                  ],
  127  => ["SQL_CREATE_ASSERTION",           0                    ],
  136  => ["SQL_DROP_ASSERTION",             0                    ],
);

		## Put both numbers and names into a hash
		my %t;
		for (keys %type) {
			$t{$_} = $type{$_}->[1];
			$t{$type{$_}->[0]} = $type{$_}->[1];
		}

		return undef unless exists $t{$type};

		my $ans = $t{$type};

		if ($ans eq 'NAMEDATALEN') {
			return $version >= 70300 ? 63 : 31;
		}
		elsif ($ans eq 'ODBCVERSION') {
			return "00.00.0000" unless $version =~ /^(\d\d?)(\d\d)(\d\d)$/o;
			return sprintf "%02d.%02d.%.2d00", $1,$2,$3;
		}
		elsif ($ans eq 'DBDVERSION') {
			my $simpleversion = $DBD::Pg::VERSION;
			$simpleversion =~ s/_/./g;
			return sprintf "%02d.%02d.%1d%1d%1d%1d", split (/\./, "$simpleversion.0.0.0.0.0.0");
		}
		elsif ($ans eq 'SOURCENAME') {
			return "dbi:Pg:dbname=$dbh->{Name}";
		}
		elsif ($ans eq 'SCHEMAUSAGE') {
			return 0 if $version < 70300;
			my %bitmask = (
				SQL_SU_DML_STATEMENT        => 1,
				SQL_SU_PROCEDURE_INVOCATION => 2,
				SQL_SU_TABLE_DEFINITION     => 4,
				SQL_SU_INDEX_DEFINITION     => 8,
				SQL_SU_PRIVILEGE_DEFINITION => 16,
			);
			return 31; ## all of the above
		}
		elsif ($ans eq 'CREATESCHEMA') {
			return 0 if $version < 70300;
			my %bitmask = (
				SQL_CS_CREATE_SCHEMA         => 1,
	 			SQL_CS_AUTHORIZATION         => 2,
				SQL_CS_DEFAULT_CHARACTER_SET => 4
			);
			return $bitmask{SQL_CS_CREATE_SCHEMA} + $bitmask{SQL_CS_AUTHORIZATION};
		 }
		 elsif ($ans eq 'DROPSCHEMA') {
			return 0 if $version < 70300;
			my %bitmask = (
				SQL_DS_DROP_SCHEMA => 1,
	 			SQL_DS_RESTRICT    => 2,
				SQL_DS_CASCADE     => 4
			);
			return 7; ## All of the above
		 }
		 return $ans;
	} # end of get_info
} 


{ package DBD::Pg::st;

	sub bind_param_array { ## The DBI version is broken, so we implement a near-copy here
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
		1;
	}


} ## end st section

1;

__END__

=head1 NAME

DBD::Pg - PostgreSQL database driver for the DBI module

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

This connects to the database $dbname at localhost without any user
authentication. This is sufficient for the defaults of PostgreSQL (excluding
some package-installed versions).

The following connect statement shows almost all possible parameters:

  $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$host;port=$port;" .
                      "options=$options", "$username", "$password",
                      {AutoCommit => 0});

If a parameter is undefined PostgreSQL first looks for specific environment
variables and then it uses hard-coded defaults:

  parameter  environment variable  hard coded default
  --------------------------------------------------
  host       PGHOST                local domain socket
  hostaddr*  PGHOSTADDR            local domain socket
  port       PGPORT                5432
  dbname**   PGDATABASE            current userid
  username   PGUSER                current userid
  password   PGPASSWORD            ""
  options    PGOPTIONS             ""
  service*   PGSERVICE             ""
  sslmode*   PGSSLMODE             ""

* Only for servers running version 7.4 or greater

** Can also use "db" or "database"

The options parameter specifies runtime options for the Postgres
backend. Common usage is to increase the number of buffers with the C<-B>
option. Also important is the C<-F> option, which disables automatic fsync()
call after each transaction. For further details please refer to the
PostgreSQL documentation at L<http://www.postgresql.org/docs/>.

For authentication with username and password, appropriate entries have to be
made in F<pg_hba.conf>. Please refer to the comments in the F<pg_hba.conf> and
the F<pg_passwd> files for the different types of authentication. Note that
for these two parameters DBI distinguishes between empty and undefined. If
these parameters are undefined DBI substitutes the values of the environment
variables C<DBI_USER> and C<DBI_PASS> if present.

=item B<available_drivers>

  @driver_names = DBI->available_drivers;

Implemented by DBI, no driver-specific impact.

=item B<data_sources>

  @data_sources = DBI->data_sources('Pg');

This driver supports this method. Note that the necessary database connection
to the database "template1" will be made on the localhost without any user
authentication. Other preferences can only be set with the environment
variables C<PGHOST>, C<PGPORT>, C<DBI_USER>, and C<DBI_PASS>.

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
PostgreSQL servers version less than 7.4 will always return a generic
"S1000" code. Success is indicated by a "00000" code.

The list of codes used by PostgreSQL can be found at:
L<http://www.postgresql.org/docs/current/static/errcodes-appendix.html>

=item B<trace>

  $h->trace($trace_level, $trace_filename);

Implemented by DBI, no driver-specific impact.

=item B<trace_msg>

  $h->trace_msg($message_text);

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

The REMARKS field will be returned as C<NULL> for Postgres versions 7.1.x and
older.

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
$backend_pid ] of asynchronous notifications received.

=item getfd

  $fd = $dbh->func('getfd');

Returns fd of the actual connection to server. Can be used with select() and
func('pg_notifies'). Deprecated in favor of C<< $dbh->{pg_socket} >>.

=back

=back

=head1 ATTRIBUTES COMMON TO ALL HANDLES

=over 4

=item B<Warn> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<Active> (boolean, read-only)

Supported by this driver as proposed by DBI. A database handle is active while
it is connected and statement handle is active until it is finished.

=item B<Kids> (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<ActiveKids> (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<CachedKids> (hash ref)

Implemented by DBI, no driver-specific impact.

=item B<CompatMode> (boolean, inherited)

Not used by this driver.

=item B<InactiveDestroy> (boolean)

Implemented by DBI, no driver-specific impact.

=item B<PrintError> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<RaiseError> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<HandleError> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<ChopBlanks> (boolean, inherited)

Supported by this driver as proposed by DBI. This method is similar to the
SQL function C<RTRIM>.

=item B<LongReadLen> (integer, inherited)

Implemented by DBI, not used by this driver.

=item B<LongTruncOk> (boolean, inherited)

Implemented by DBI, not used by this driver.

=item B<Taint> (boolean, inherited)

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

Prepares a statement for later execution. PostgreSQL supports prepared
statements, which enables DBD::Pg to only send the query once, and
simply send the arguments for every subsequent call to execute().
DBD::Pg can use these server-side prepared statements, or it can
just send the entire query to the server each time. The best way
is automatically chosen for each query. This will be sufficient for
most users: keep reading for a more detailed explanation and some
optional flags.

Deciding whether or not to use prepared statements depends on many
factors, but you can force them to be used or not used by passing
the C<pg_server_prepare> attribute to prepare(). A "0" means to never
use prepared statements. This is the default when connected to servers
earlier than version 7.4, which is when prepared statements were introduced.
Setting C<pg_server_prepare> to "1" means that prepared statements
should be used whenever possible. This is the default for servers
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
plan will probably work out well. Further discussion on this subject is beyond
the scope of this documentation: please consult the pgsql-performance mailing
list, L<http://archives.postgresql.org/pgsql-performance/>

Only certain commands will be sent to a server-side prepare: currently these
include C<SELECT>, C<INSERT>, C<UPDATE>, and C<DELETE>. DBD::Pg uses a simple
naming scheme for the prepared statements: C<dbdpg_#>, where "#" starts at 1 and
increases. This number is tracked at the database handle level, so multiple
statement handles will not collide. If you use your own prepare statements, do
not name them "dbdpg_"!

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

  $sth->prepare("SELECT 123, ?", {pg_prepare_now = 1});

The following two examples will NOT be prepared right away:

  $sth->prepare("SELECT 123, ?"); ## has a placeholder

  $sth->prepare("SELECT 123", {pg_prepare_now = 0});

There are times when you may want to prepare a statement yourself. To do this,
simply send the C<PREPARE> statement directly to the server (e.g. with
"do"). Create a statement handle and set the prepared name via
C<pg_prepare_name> attribute. The statement handle can be created with a dummy
statement, as it will not be executed. However, it should have the same
number of placeholders as your prepared statement. Example:

  $dbh->do("PREPARE mystat AS SELECT COUNT(*) FROM pg_class WHERE reltuples < ?");
  $sth = $dbh->prepare("ABC ?");
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

  $SQL = "SELECT count(*) FROM pg_class WHERE relpages > $2";

  $SQL = "SELECT count(*) FROM pg_class WHERE relpages BETWEEN $1 AND $3";

Legal:

  $SQL = "SELECT count(*) FROM pg_class WHERE relpages > $1";

  $SQL = "SELECT count(*) FROM pg_class WHERE relpages BETWEEN $1 AND $2";

  $SQL = "SELECT count(*) FROM pg_class WHERE relpages BETWEEN $1 AND $2 AND reltuples > $1";

  $SQL = "SELECT count(*) FROM pg_class WHERE relpages > $1 AND reltuples > $1";

In the final statement above, DBI thinks there is only one placeholder, so this
statement will replace both placeholders:

  $sth->bind_param(1, 2045);

While execute requires only a single argument as well:

  $sth->execute(2045);

The final placeholder method is the named parameters in the format ":foo". While this
syntax is supported by DBD::Pg, its use is highly discouraged.

The different types of placeholders cannot be mixed within a statement, but you may
use different ones for each statement handle you have. Again, this is not encouraged.

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
name for susequent calls. If you need to disable this caching for some reason,
you can control it via the C<pg_cache> attribute.

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

This is a Postgres-specific extension to the C<ping> command. This will check the 
validity of a database handle in exactly the same way as C<ping>, but instead of 
returning a 0 for an invalid connection, it will return a negative number. The 
positive numbers are documented at C<ping>, the negative ones indicate:

  Value    Meaning
  --------------------------------------------------
   -1      There is no connection to the database at all (e.g. after C<disconnect>)
   -2      An unknown transaction status was returned (e.g. after forking)
   -3      The handle exists, but no data was returned from a test query.

In practice, you should only ever see -1 and -2.

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

Also, two additional non-standard fields are returned:

  pg_type - data type with additional info i.e. "character varying(20)"
  pg_constraint - holds column constraint definition

The REMARKS field will be returned as NULL (C<undef> for PostgreSQL versions
older than 7.2. The TABLE_SCHEM field will be returned as NULL (C<undef>) for
versions older than 7.4.

=item B<table_info>

  $sth = $dbh->table_info( $catalog, $schema, $table, $type );

Supported by this driver as proposed by DBI. This method returns all tables
and views visible to the current user. The $catalog argument is currently
unused. The schema and table arguments will do a C<LIKE> search if a percent
sign (C<%>) or an underscore (C<_>) is detected in the argument. The $type
argument accepts a value of either "TABLE" or "VIEW" (using both is the
default action).

The TABLE_CAT field will always return NULL (C<undef>). The TABLE_SCHEM field
returns NULL (C<undef>) if the server is older than version 7.4.

If your database supports tablespaces (version 8.0 or greater), two additional
columns are returned, "pg_tablespace_name" and "pg_tablespace_location",
that contain the name and location of the tablespace associated with
this table. Tables that have not been assigned to a particular tablespace
will return NULL (C<undef>) for both of these columns.

=item B<primary_key_info>

  $sth = $dbh->primary_key_info( $catalog, $schema, $table, \%attr );

Supported by this driver as proposed by DBI. The $catalog argument is
currently unused, and the $schema argument has no effect against
servers running version 7.2 or older. There are no search patterns allowed,
but leaving the $schema argument blank will cause the first table
found in the schema search path to be used. An additional field, "DATA_TYPE",
is returned and shows the data type for each of the arguments in the
"COLUMN_NAME" field.

This method will also return tablespace information for servers that support
tablespaces. See the C<table_info> entry for more information.

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
This function returns C<undef> for PostgreSQL servers earlier than version
7.3. There are no search patterns allowed, but leaving the $schema argument
blank will cause the first table found in the schema search path to be
used. Two additional fields, "UK_DATA_TYPE" and "FK_DATA_TYPE", are returned
to show the data type for the unique and foreign key columns. Foreign
keys that have no named constraint (where the referenced column only has
an unique index) will return C<undef> for the "UK_NAME" field.

=item B<tables>

  @names = $dbh->tables( $catalog, $schema, $table, $type, \%attr );

Supported by this driver as proposed by DBI. This method returns all tables
and/or views which are visible to the current user: see C<table_info()>
for more information about the arguments. If the database is version 7.3
or later, the name of the schema appears before the table or view name. This
can be turned off by adding in the C<pg_noprefix> attribute:

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

  $sql = $dbh->quote($value, $data_type);

This module implements its own C<quote> method. In addition to the DBI method it
also doubles the backslash, because PostgreSQL treats a backslash as an escape
character.

B<NOTE:> The undocumented (and invalid) support for the C<SQL_BINARY> data
type is officially deprecated. Use C<PG_BYTEA> with C<bind_param()> instead:

  $rv = $sth->bind_param($param_num, $bind_value,
                         { pg_type => DBD::Pg::PG_BYTEA });


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

=item B<Driver>  (handle)

Implemented by DBI, no driver-specific impact.

=item B<Name>  (string, read-only)

The default DBI method is overridden by a driver specific method that returns
only the database name. Anything else from the connection string is stripped
off. Note that, in contrast to the DBI specs, the DBD::Pg implementation fo
this method is read-only.

=item B<RowCacheSize>  (integer)

Implemented by DBI, not used by this driver.

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
the C<utf8> flag, see L<Encode|Encode>. This attribute only relevant under
perl 5.8 and later.

B<NB>: This attribute is experimental and may be subject to change.

=item B<pg_INV_READ> (integer, read-only)

Constant to be used for the mode in C<lo_creat> and C<lo_open>.

=item B<pg_INV_WRITE> (integer, read-only)

Constant to be used for the mode in C<lo_creat> and C<lo_open>.

=item B<pg_bool_tf> (boolean)

PostgreSQL specific attribute. If true, boolean values will be returned
as the characters 't' and 'f' instead of '1' and '0'.

=item B<pg_errorlevel> (integer)

PostgreSQL specific attribute, only works for servers version 7.4 and above.
Sets the amount of information returned by the server's error messages.
Valid entries are 1,2, and 3. Any other number will be forced to the default
value of 2.

A value of 0 ("TERSE") will show severity, primary text, and position only
and will usually fit on a single line. A value of 1 ("DEFAULT") will also
show any detail, hint, or context fields. A value of 2 ("VERBOSE") will
show all available information.

=item B<pg_protocol> (integer, read-only)

PostgreSQL specific attribute. Returns the version of the PostgreSQL server.
If DBD::Pg is unable to figure out the version (e.g. it was compiled
against pre 7.4 libraries), it will return a "0". Otherwise, servers below
version 7.4 return a "2", and (currently) 7.4 and above return a "3".

=item B<pg_lib_version> (integer, read-only)

PostgreSQL specific attribute. Indicates which version of PostgreSQL that 
DBD::Pg was compiled against. In other words, which libraries were used. 
Returns a number with major, minor, and revision together; version 7.4.2 
would be returned as 70402.

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
especially important when using the new server-side prepare system with
PostgreSQL 7.4. See the C<prepare()> method for more information.

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
  $sth->bind_param("$2", "Zool");

  ## We realize that the wrong data type was set above, so we change it:
  $sth->bind_param("$1", 234, { pg_type => PG_INTEGER });

  ## We also got the wrong value, so we change that as well.
  ## Because the data type is sticky, we don't need to change it
  $sth->bind_param(1, 567);

  ## This executes the statement with 567 (integer) and "Zool" (varchar)
  $sth->execute();

=item B<bind_param_inout>

Currently not supported by this driver.

=item B<execute>

  $rv = $sth->execute(@bind_values);

Executes a previously prepared statement. In addition to C<UPDATE>, C<DELETE>,
C<INSERT> statements, for which it returns always the number of affected rows,
the C<execute> method can also be used for C<SELECT ... INTO table> statements.

The "prepare/bind/execute" process has changed significantly for PostgreSQL
servers 7.4 and later: please see the C<prepare()> and C<bind_param()> entries for
much more information.

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

Supported by this driver as proposed by DBI. This is only available for
servers version 7.3 and later. Others will return "2" for all columns.

=item B<CursorName>  (string, read-only)

Not supported by this driver. See the note about B<Cursors> elsewhere in this
document.

=item C<Database>  (dbh, read-only)

Implemented by DBI, no driver-specific impact.

=item C<ParamValues>  (hash ref, read-only)

Supported by this driver as proposed by DBI. If called before C<execute>, the
literal values passed in are returned. If called after C<execute>, then
the quoted versions of the values are shown.

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
only argument is the name of the savepoint. Note that PostgreSQL does allow 
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

=back

=head2 COPY support

DBD::Pg supports the COPY command through three functions: pg_putline, 
pg_getline, and pg_endcopy. The COPY command allows data to be quickly 
loaded or read from a table. The basic process is to issue a COPY 
command via $dbh->do(), do either $dbh->pg_putline or $dbh->pg_getline, 
and then issue a $dbh->pg_endcopy (for pg_putline only).

The first step is to put the server into "COPY" mode. This is done by 
sending a complete COPY command to the server, by using the do() method. 
For example:

  $dbh->do("COPY foobar FROM STDIN");

This would tell the server to enter a COPY IN state. It is now ready to 
receive information via the pg_putline method. The complete syntax of the 
COPY command is more complex and not documented here: the canonical 
PostgreSQL documentation for COPY be found at:

http://www.postgresql.org/docs/current/static/sql-copy.html

Note that 7.2 servers can only accept a small subset of later features in 
the COPY command: most notably they do not accept column specifications.

Once the COPY command has been issued, no other SQL commands are allowed 
until after pg_endcopy has been successfully called. If in a COPY IN state, 
you cannot use pg_getline, and if in COPY OUT state, you cannot use pg_putline.

=over 4

=item B<pg_putline>

Used to put data into a table after the server has been put into COPY IN mode 
by calling "COPY tablename FROM STDIN". The only argument is the data you want 
inserted. The default delimiter is a tab character, but this can be changed in 
the COPY statement. Returns a 1 on sucessful input. Examples:

  $dbh->do("COPY mytable FROM STDIN");
  $dbh->pg_putline("123\tPepperoni\t3\n");
  $dbh->pg_putline("314\tMushroom\t8\n");
  $dbh->pg_putline("6\tAnchovies\t100\n");
  $dbh->pg_endcopy;

  ## This example uses explicit columns and a custom delimiter
  $dbh->do("COPY mytable(flavor, slices) FROM STDIN WITH DELIMITER '~'");
  $dbh->pg_putline("Pepperoni~123\n");
  $dbh->pg_putline("Mushroom~314\n");
  $dbh->pg_putline("Anchovies~6\n");
  $dbh->pg_endcopy;

=item B<pg_getline>

Used to retrieve data from a table after the server has been put into COPY OUT 
mode by calling "COPY tablename TO STDOUT". The first argument to pg_getline is 
the variable into which the data will be stored. The second argument is the size 
of the variable: this should be greater than the expected size of the row. Returns 
a 1 on success, and an empty string when the last row has been fetched. Example:

  $dbh->do("COPY mytable TO STDOUT");
  my @data;
  my $x=0;
  1 while($dbh->pg_getline($data[$x++], 100));
  pop @data; ## Remove final "\\.\n" line

If DBD::Pg is compiled with pre-7.4 libraries, this function will not work: you 
will have to use the old $dbh->func($data, 100, 'getline') command, and call 
pg_getline manually. Users are highly encouraged to upgrade to a newer version 
of PostgreSQL if this is the case.

=item B<pg_endcopy>

When done with pg_putline, call pg_endcopy to put the server back in 
a normal state. Returns a 1 on success. This method will fail if called when not 
in a COPY IN or COPY OUT state. Note that you no longer need to send "\\.\n" when 
in COPY IN mode: pg_endcopy will do this for you automatically as needed.
pg_endcopy is only needed after getline if you are using the old-style method, 
$dbh->func($data, 100, 'getline').


=back

=head2 Large Objects

This driver supports all largeobject functions provided by libpq via the
C<func> method. Please note that, starting with PostgreSQL 6.5, any access to
a large object -- even read-only large objects -- must be put into a
transaction!

=head2 Cursors

Although PostgreSQL has a cursor concept, it has not been used in the current
implementation. Cursors in PostgreSQL can only be used inside a transaction
block. Because only one transaction block at a time is allowed, this would
have implied the restriction not to use any nested C<SELECT> statements. Hence
the C<execute> method fetches all data at once into data structures located in
the front-end application. This approach must to be considered when selecting
large amounts of data!

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

PostgreSQL version 7.3 introduced schema support. Note that the PostgreSQL
schema concept may differ from those of other databases. In a nutshell, a schema
is a named collection of objects within a single database. Please refer to the
PostgreSQL documentation for more details.

Currently, DBD::Pg does not provide explicit support for PostgreSQL schemas.
However, schema functionality may be used without any restrictions by
explicitly addressing schema objects, e.g.

  my $res = $dbh->selectall_arrayref("SELECT * FROM my_schema.my_table");

or by manipulating the schema search path with C<SET search_path>, e.g.

  $dbh->do("SET search_path TO my_schema, public");

=head1 SEE ALSO

L<DBI>

=head1 AUTHORS

DBI and DBD-Oracle by Tim Bunce (Tim.Bunce@ig.co.uk)

DBD-Pg by Edmund Mergl (E.Mergl@bawue.de) and Jeffrey W. Baker
(jwbaker@acm.org). By David Wheeler <david@justatheory.com>, Jason
Stewart <jason@openinformatics.com> and Bruce Momjian
<pgman@candle.pha.pa.us> and others after v1.13.

Parts of this package have been copied from DBI and DBD-Oracle.

B<Mailing List>

The current maintainers may be reached through the 'dbdpg-general' mailing
list: L<http://gborg.postgresql.org/mailman/listinfo/dbdpg-general/>.

This list is available through Gmane (L<http://www.gmane.org/>) as a newsgroup
with the name: C<gmane.comp.db.postgresql.dbdpg>

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

