
#  $Id$
#
#  Copyright (c) 1997,1998,1999,2000 Edmund Mergl
#  Copyright (c) 2002 Jeffrey W. Baker
#  Copyright (c) 2002-2004 PostgreSQL Global Development Group
#  Portions Copyright (c) 1994,1995,1996,1997 Tim Bunce
#
#  You may distribute under the terms of either the GNU General Public
#  License or the Artistic License, as specified in the Perl README file.


use 5.006001;

$DBD::Pg::VERSION = '1.32_2';

{
	package DBD::Pg;

	use DBI ();
	use DynaLoader ();
	use Exporter ();
	@ISA = qw(DynaLoader Exporter);

	%EXPORT_TAGS = (
	pg_types => [ qw(
		PG_BOOL PG_BYTEA PG_CHAR PG_INT8 PG_INT2 PG_INT4 PG_TEXT PG_OID
		PG_FLOAT4 PG_FLOAT8 PG_ABSTIME PG_RELTIME PG_TINTERVAL PG_BPCHAR
		PG_VARCHAR PG_DATE PG_TIME PG_DATETIME PG_TIMESPAN PG_TIMESTAMP
	)]);

	Exporter::export_ok_tags('pg_types');

	require_version DBI 1.35;

	bootstrap DBD::Pg $VERSION;

	$err = 0;		# holds error code for DBI::err
	$errstr = "";	# holds error string for DBI::errstr
	$drh = undef;	# holds driver handle once initialized

	sub driver{
		return $drh if $drh;
		my($class, $attr) = @_;

		$class .= "::dr";

		# not a 'my' since we use it above to prevent multiple drivers

		$drh = DBI::_new_drh($class, {
			'Name' => 'Pg',
			'Version' => $VERSION,
			'Err' => \$DBD::Pg::err,
			'Errstr' => \$DBD::Pg::errstr,
			'Attribution' => 'PostgreSQL DBD by Edmund Mergl',
		});

		$drh;
	}

	## Used by both the dr and db packages
	sub _pg_server_version {
		my $dbh = shift;
		return $dbh->{private_dbdpg}{server_version} if defined $dbh->{private_dbdpg}{server_version};
		my ($version) = $dbh->selectrow_array("SELECT version();");
		$dbh->{private_dbdpg}{server_version} = ($version =~ /^PostgreSQL ([\d\.]+)/) ? $1 : 0;
		return $dbh->{private_dbdpg}{server_version};
	}

	## Is the second version greater than or equal to the first?
    # Returns:
    # 0 if first version is greater
    # 1 if they are equal
    # 2 if second version is greater 
	sub _pg_check_version($$) {
		## Check each section from left to right
		my @uno = split (/\./ => $_[0]);
		my @dos = split (/\./ => $_[1]);
		for (my $i=0; defined $uno[$i] or defined $dos[$i]; $i++) {
			$uno[$i] = 0 if ! defined $uno[$i];
			$dos[$i] = 0 if ! defined $dos[$i];
			return 2 if $uno[$i] < $dos[$i];
			return 0 if $uno[$i] > $dos[$i];
		}
		return 1; ## versions are equal
	}

	## Version 7.3 and up uses schemas, so add a "pg_catalog." to system tables
	sub _pg_use_catalog {
		my $dbh = shift;
		return $dbh->{private_dbdpg}{pg_use_catalog} if defined $dbh->{private_dbdpg}{pg_use_catalog};
		my $version = DBD::Pg::_pg_server_version($dbh);
		$dbh->{private_dbdpg}{pg_use_catalog} = DBD::Pg::_pg_check_version(7.3, $version) ? "pg_catalog." : "";
		return $dbh->{private_dbdpg}{pg_use_catalog};
	}

	1;
}


{ package DBD::Pg::dr; # ====== DRIVER ======

	use strict;

	sub data_sources {
		my $drh = shift;
		my $dbh = DBD::Pg::dr::connect($drh, 'dbname=template1') or return undef;
		$dbh->{AutoCommit}=1;
		my $CATALOG = DBD::Pg::_pg_use_catalog($dbh);
		my $SQL = "SELECT ${CATALOG}quote_ident(datname) FROM ${CATALOG}pg_database ORDER BY 1";
		my $sth = $dbh->prepare($SQL);
		$sth->execute();
		my @sources = map { "dbi:Pg:dbname=$_->[0]" } @{$sth->fetchall_arrayref()};
		$dbh->disconnect;
		return @sources;
	}


	sub connect {
		my($drh, $dbname, $user, $auth)= @_;

		# create a 'blank' dbh

		my $Name = $dbname;
    if ($dbname =~ m#dbname\s*=\s*[\"\']([^\"\']+)#) {
      $Name = "'$1'";
			$dbname =~ s/"/'/g;
		}			
		elsif ($dbname =~ m#dbname\s*=\s*([^;]+)#) {
      $Name = $1;
    }

		$user = "" unless defined($user);
		$auth = "" unless defined($auth);

		$user = $ENV{DBI_USER} if $user eq "";
		$auth = $ENV{DBI_PASS} if $auth eq "";

		$user = "" unless defined($user);
		$auth = "" unless defined($auth);

		my($dbh) = DBI::_new_dbh($drh, {
			'Name' => $Name,
			'User' => $user, 'CURRENT_USER' => $user,
		});

		# Connect to the database..
		DBD::Pg::db::_login($dbh, $dbname, $user, $auth) or return undef;

		$dbh;
	}

}


{ package DBD::Pg::db; # ====== DATABASE ======

	use strict;
	use Carp ();

	sub prepare {
		my($dbh, $statement, @attribs)= @_;

		# create a 'blank' sth

		my $sth = DBI::_new_sth($dbh, {
			'Statement' => $statement,
		});

		DBD::Pg::st::_prepare($sth, $statement, @attribs) or return undef;

		$sth;
	}


	sub ping {
		my($dbh) = @_;
		local $SIG{__WARN__} = sub { } if $dbh->{PrintError};
		local $dbh->{RaiseError} = 0 if $dbh->{RaiseError};
		my $ret = DBD::Pg::db::_ping($dbh);
		return $ret;
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

		my $CATALOG = DBD::Pg::_pg_use_catalog($dbh);
		my $version = DBD::Pg::_pg_server_version($dbh);

		my @search;
		## If the schema or table has an underscore or a %, use a LIKE comparison
		if (defined $schema and length $schema and DBD::Pg::_pg_check_version(7.3, $version)) {
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

		my $showschema = DBD::Pg::_pg_check_version(7.3, $version) ? 
			"n.nspname" : "NULL::text";

		my $schemajoin = DBD::Pg::_pg_check_version(7.3, $version) ? 
			"JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)" : "";

		# col_description is not available for Pg < 7.2
		my $remarks = DBD::Pg::_pg_check_version(7.2, $version) ?
            "${CATALOG}col_description(a.attrelid, a.attnum)" : "NULL::text";

		my $col_info_sql = qq!
			SELECT
				NULL::text AS "TABLE_CAT"
				, $showschema AS "TABLE_SCHEM"
				, c.relname AS "TABLE_NAME"
				, a.attname AS "COLUMN_NAME"
				, a.atttypid AS "DATA_TYPE"
				, ${CATALOG}format_type(a.atttypid, NULL) AS "TYPE_NAME"
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
				, ${CATALOG}format_type(a.atttypid, a.atttypmod) AS "pg_type"
				, ${CATALOG}format_type(a.atttypid, NULL) AS "pg_type_only"
				, a.atttypmod AS "pg_atttypmod"
			FROM
				${CATALOG}pg_type t
				JOIN ${CATALOG}pg_attribute a ON (t.oid = a.atttypid)
				JOIN ${CATALOG}pg_class c ON (a.attrelid = c.oid)
				LEFT JOIN ${CATALOG}pg_attrdef af ON (a.attnum = af.adnum AND a.attrelid = af.adrelid)
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
			pg_type							 18
			pg_type_only				 19
			pg_atttypmod				 20
			/);
		
		my $constraint_query = DBD::Pg::_pg_check_version(7.3, $version)
			? "SELECT consrc FROM pg_catalog.pg_constraint WHERE contype = 'c' AND conname = ?" 
				: "SELECT rcsrc FROM pg_relcheck WHERE rcname = ?";
		my $constraint_sth = $dbh->prepare($constraint_query); 		 

		for my $row (@$data) {
			$row->[$col_map{COLUMN_SIZE}] = 
 				_calc_col_size($row->[$col_map{pg_atttypmod}],$row->[$col_map{COLUMN_SIZE}]);

			# Replace the Pg type with the SQL_ type
			my $w = $row->[$col_map{DATA_TYPE}];
			$row->[$col_map{DATA_TYPE}] = DBD::Pg::db::pg_type_info($dbh,$row->[$col_map{DATA_TYPE}]);
			$w = $row->[$col_map{DATA_TYPE}];
			
			pop @$row;

			# Add pg_constraint
			$constraint_sth->execute("$row->[$col_map{TABLE_NAME}]_$row->[$col_map{COLUMN_NAME}]");
			$col_map{pg_constraint} = 20;
			($row->[$col_map{pg_constraint}]) = $constraint_sth->fetchrow_array; 
		}

		# get rid of atttypmod that we no longer need
		delete $col_map{pg_atttypmod};

		# Since we've processed the data in Perl, we have to jump through a hoop
		# To turn it back into a statement handle 
		# 
		my $sth = _prepare_from_data(
			'column_info', 
			$data, 
				[ sort { $col_map{$a} <=> $col_map{$b}  } keys %col_map]);
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

		my $whereclause = "AND c.relname = " . $dbh->quote($table);

		my $CATALOG = DBD::Pg::_pg_use_catalog($dbh);
		my $gotschema = DBD::Pg::_pg_check_version
			(7.3, DBD::Pg::_pg_server_version($dbh)) ? 1 : 0;
		if (defined $schema and length $schema and $gotschema) {
			$whereclause .= "\n\t\t\tAND n.nspname = " . $dbh->quote($schema);
		}
		my $showschema = $gotschema ? "quote_ident(n.nspname)" : "NULL::text";
		my $schemajoin = $gotschema ? 
			"LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)" : "";
		my $pri_key_sql = qq{
			SELECT
				  c.oid
				, $showschema
				, quote_ident(c.relname)
				, quote_ident(c2.relname)
				, i.indkey
			FROM
				${CATALOG}pg_class c
				JOIN ${CATALOG}pg_index i ON (i.indrelid = c.oid)
				JOIN ${CATALOG}pg_class c2 ON (c2.oid = i.indexrelid)
				$schemajoin
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
			SELECT a.attnum, ${CATALOG}quote_ident(a.attname) AS colname,
				${CATALOG}quote_ident(t.typname) AS typename
			FROM ${CATALOG}pg_attribute a, ${CATALOG}pg_type t
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
				$x++;
			}
		}
		else { ## Nicer way: return only one row

			# TABLE_CAT
			$info->[0] = undef;
			# PK_NAME
			$info->[5] = $info->[3];
			# COLUMN_NAME
			$info->[3] = $attr->{'pg_onerow'} == 2 ? 
				[ map { $attribs->{$_}{colname} } split /\s+/, $info->[4] ] :
					join ', ', map { $attribs->{$_}{colname} } split /\s+/, $info->[4]; 
			# DATA_TYPE
			$info->[6] = $attr->{'pg_onerow'} == 2 ? 
				[ map { $attribs->{$_}{typename} } split /\s+/, $info->[4] ] : 
					join ', ', map { $attribs->{$_}{typename} } split /\s+/, $info->[4];
			# KEY_SEQ
			$info->[4] = $attr->{'pg_onerow'} == 2 ? 
				[ split /\s+/, $info->[4] ] :
					join ', ', split /\s+/, $info->[4];
			$pkinfo = [$info];
		}

		my @cols = (qw(TABLE_CAT TABLE_SCHEM TABLE_NAME COLUMN_NAME
									 KEY_SEQ PK_NAME DATA_TYPE));

		return _prepare_from_data('primary_key_info', $pkinfo, \@cols);

	}

	sub primary_key {
		my $sth = primary_key_info(@_[0..3], {pg_onerow => 2});
		return defined $sth ? @{$sth->fetchall_arrayref()->[0][3]} : undef;
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
		my $version = DBD::Pg::_pg_server_version($dbh);
		return undef unless DBD::Pg::_pg_check_version(7.3, $version);

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
		my $WHERE = $odbc ? "((contype  = 'p'" : "((contype IN ('p','u')";
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

		my @CLI_cols = (qw(UK_TABLE_CAT UK_TABLE_SCHEM UK_TABLE_NAME UK_COLUMN_NAME
									 FK_TABLE_CAT FK_TABLE_SCHEM FK_TABLE_NAME FK_COLUMN_NAME
									 ORDINAL_POSITION UPDATE_RULE DELETE_RULE FK_NAME UK_NAME 
									 DEFERABILITY UNIQUE_OR_PRIMARY UK_DATA_TYPE FK_DATA_TYPE));

		my @ODBC_cols = (qw(PKTABLE_CAT PKTABLE_SCHEM PKTABLE_NAME PKCOLUMN_NAME
									 FKTABLE_CAT FKTABLE_SCHEM FKTABLE_NAME FKCOLUMN_NAME
									 KEY_SEQ UPDATE_RULE DELETE_RULE FK_NAME PK_NAME 
									 DEFERABILITY UNIQUE_OR_PRIMARY PK_DATA_TYPE FKDATA_TYPE));

		return _prepare_from_data('foreign_key_info', $fkinfo, $odbc ? \@ODBC_cols : \@CLI_cols);

	}


	sub table_info {	# DBI spec: TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS
		my $dbh = shift;
		my ($catalog, $schema, $table, $type) = @_;

		my $tbl_sql = ();

		my $version = DBD::Pg::_pg_server_version($dbh);
		my $CATALOG = DBD::Pg::_pg_use_catalog($dbh);

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
			$tbl_sql = DBD::Pg::_pg_check_version(7.3, $version) ? 
				q{SELECT 
						 NULL::text AS "TABLE_CAT"
					 , n.nspname  AS "TABLE_SCHEM"
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
			my $schemajoin = "";
			my $has_objsubid = "";
			if (DBD::Pg::_pg_check_version(7.3, $version)) {
				$showschema = "n.nspname";
				$schemajoin = "LEFT JOIN pg_catalog.pg_namespace n ON (n.oid = c.relnamespace)";
				$has_objsubid = "AND d.objsubid = 0";
			}

			my @search;
			## If the schema or table has an underscore or a %, use a LIKE comparison
			if (defined $schema and length $schema and DBD::Pg::_pg_check_version(7.3, $version)) {
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
			my $schemacase = DBD::Pg::_pg_check_version(7.3, $version) ? "n.nspname" : "c.relname";
			$tbl_sql = qq{
				SELECT NULL::text AS "TABLE_CAT"
					 , $showschema AS "TABLE_SCHEM"
					 , c.relname AS "TABLE_NAME"
					 , CASE
					 		WHEN c.relkind = 'v' THEN
								CASE WHEN $schemacase ~ '^pg_' THEN 'SYSTEM VIEW' ELSE 'VIEW' END
							ELSE
								CASE WHEN $schemacase ~ '^pg_' THEN 'SYSTEM TABLE' ELSE 'TABLE' END
						END AS "TABLE_TYPE"
					 , d.description AS "REMARKS"
				FROM ${CATALOG}pg_class AS c
					LEFT JOIN ${CATALOG}pg_description AS d 
						ON (c.relfilenode = d.objoid $has_objsubid)
					$schemajoin
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
			my $version = DBD::Pg::_pg_server_version($dbh);
			my @tables = map { (DBD::Pg::_pg_check_version(7.3, $version) 
					and (! (ref $attr eq "HASH" and $attr->{noprefix}))) ? 
						"$_->[1].$_->[2]" : $_->[2] } @$tables;
			return @tables;
	}

	sub table_attributes {
		my ($dbh, $table) = @_;
		my $CATALOG = DBD::Pg::_pg_use_catalog($dbh);
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

		foreach my $row (@$attrs) {
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
			@pri_keys = $dbh->primary_key( $CATALOG, undef, $table );
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
 
		my $version = DBD::Pg::_pg_server_version($dbh);
 
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
			return DBD::Pg::_pg_check_version(7.3, $version) ? 63 : 31;
		}
		elsif ($ans eq 'ODBCVERSION') {
			return sprintf "%02d.%02d.%1d%1d%1d%1d", split (/\./, "$version.0.0.0.0.0.0");
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
			return 0 if ! DBD::Pg::_pg_check_version(7.3, $version);
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
			return 0 if ! DBD::Pg::_pg_check_version(7.3, $version);
			my %bitmask = (
				SQL_CS_CREATE_SCHEMA         => 1,
	 			SQL_CS_AUTHORIZATION         => 2,
				SQL_CS_DEFAULT_CHARACTER_SET => 4
			);
			return $bitmask{SQL_CS_CREATE_SCHEMA} + $bitmask{SQL_CS_AUTHORIZATION};
		 }
		 elsif ($ans eq 'DROPSCHEMA') {
			return 0 if ! DBD::Pg::_pg_check_version(7.3, $version);
			my %bitmask = (
				SQL_DS_DROP_SCHEMA => 1,
	 			SQL_DS_RESTRICT    => 2,
				SQL_DS_CASCADE     => 4
			);
			return 7; ## All of the above
		 }
		 return $ans;
	} # end of get_info
} # end of package DBD::Pg::db

{  package DBD::Pg::st; # ====== STATEMENT ======

	# all done in XS
}

1;

__END__

=head1 NAME

DBD::Pg - PostgreSQL database driver for the DBI module

=head1 SYNOPSIS

  use DBI;

  $dbh = DBI->connect("dbi:Pg:dbname=$dbname", "", "");

  # for some advanced uses you may need PostgreSQL type values:
  use DBD::Pg qw(:pg_types);

  # See the DBI module documentation for full details

=head1 DESCRIPTION

DBD::Pg is a Perl module which works with the DBI module to provide access to
PostgreSQL databases.

=head1 MODULE DOCUMENTATION

This documentation describes driver specific behavior and restrictions. It is
not supposed to be used as the only reference for the user. In any case
consult the DBI documentation first!

=head1 THE DBI CLASS

=head2 DBI Class Methods

=over 4

=item B<connect>

To connect to a database with a minimum of parameters, use the following
syntax:

  $dbh = DBI->connect("dbi:Pg:dbname=$dbname", "", "");

This connects to the database $dbname at localhost without any user
authentication. This is sufficient for the defaults of PostgreSQL.

The following connect statement shows all possible parameters:

  $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$host;port=$port;" .
                      "options=$options;tty=$tty", "$username", "$password");

If a parameter is undefined PostgreSQL first looks for specific environment
variables and then it uses hard-coded defaults:

  parameter  environment variable  hard coded default
  --------------------------------------------------
  dbname     PGDATABASE            current userid
  host       PGHOST                localhost
  port       PGPORT                5432
  options    PGOPTIONS             ""
  tty        PGTTY                 ""
  username   PGUSER                current userid
  password   PGPASSWORD            ""

If a host is specified, the postmaster on this host needs to be started with
the C<-i> option (TCP/IP sockets).

The options parameter specifies runtime options for the Postgres
backend. Common usage is to increase the number of buffers with the C<-B>
option. Also important is the C<-F> option, which disables automatic fsync()
call after each transaction. For further details please refer to the
L<postgres>.

For authentication with username and password appropriate entries have to be
made in pg_hba.conf. Please refer to the L<pg_hba.conf> and the L<pg_passwd>
for the different types of authentication. Note that for these two parameters
DBI distinguishes between empty and undefined. If these parameters are
undefined DBI substitutes the values of the environment variables DBI_USER and
DBI_PASS if present.

=item B<available_drivers>

  @driver_names = DBI->available_drivers;

Implemented by DBI, no driver-specific impact.

=item B<data_sources>

  @data_sources = DBI->data_sources('Pg');

The driver supports this method. Note that the necessary database connection to
the database template1 will be done on the localhost without any
user-authentication. Other preferences can only be set with the environment
variables PGHOST, PGPORT, DBI_USER and DBI_PASS.

=item B<trace>

  DBI->trace($trace_level, $trace_file)

Implemented by DBI, no driver-specific impact.

=back

=head2 DBI Dynamic Attributes

See Common Methods.

=head1 METHODS COMMON TO ALL HANDLES

=over 4

=item B<err>

  $rv = $h->err;

Supported by the driver as proposed by DBI. For the connect method it returns
PQstatus. In all other cases it returns PQresultStatus of the current handle.

=item B<errstr>

  $str = $h->errstr;

Supported by the driver as proposed by DBI. It returns the PQerrorMessage
related to the current handle.

=item B<state>

  $str = $h->state;

This driver does not (yet) support the state method.

=item B<trace>

  $h->trace($trace_level, $trace_filename);

Implemented by DBI, no driver-specific impact.

=item B<trace_msg>

  $h->trace_msg($message_text);

Implemented by DBI, no driver-specific impact.

=item B<func>

This driver supports a variety of driver specific functions accessible via the
func interface:

  $attrs = $dbh->func($table, 'table_attributes');

The C<table_attributes> function is no longer recommended. Instead,
you can use the more portable C<column_info> and C<primary_key> functions
to access all the same information.

This method returns for the given table a reference to an array of hashes:

  NAME        attribute name
  TYPE        attribute type
  SIZE        attribute size (-1 for variable size)
  NULLABLE    flag nullable
  DEFAULT     default value
  CONSTRAINT  constraint
  PRIMARY_KEY flag is_primary_key
  REMARKS     attribute description

The REMARKS field will be returned as NULL for Postgres versions 7.1.x and
older.


  $lobjId = $dbh->func($mode, 'lo_creat');

Creates a new large object and returns the object-id. $mode is a bitmask
describing different attributes of the new object. Use the following
constants:

  $dbh->{pg_INV_WRITE}
  $dbh->{pg_INV_READ}

Upon failure it returns undef.

  $lobj_fd = $dbh->func($lobjId, $mode, 'lo_open');

Opens an existing large object and returns an object-descriptor for use in
subsequent lo_* calls. For the mode bits see lo_create. Returns undef upon
failure. Note that 0 is a perfectly correct object descriptor!

  $nbytes = $dbh->func($lobj_fd, $buf, $len, 'lo_write');

Writes $len bytes of $buf into the large object $lobj_fd. Returns the number
of bytes written and undef upon failure.

  $nbytes = $dbh->func($lobj_fd, $buf, $len, 'lo_read');

Reads $len bytes into $buf from large object $lobj_fd. Returns the number of
bytes read and undef upon failure.

  $loc = $dbh->func($lobj_fd, $offset, $whence, 'lo_lseek');

Change the current read or write location on the large object
$obj_id. Currently $whence can only be 0 (L_SET). Returns the current location
and undef upon failure.

  $loc = $dbh->func($lobj_fd, 'lo_tell');

Returns the current read or write location on the large object $lobj_fd and
undef upon failure.

  $lobj_fd = $dbh->func($lobj_fd, 'lo_close');

Closes an existing large object. Returns true upon success and false upon
failure.

  $ret = $dbh->func($lobjId, 'lo_unlink');

Deletes an existing large object. Returns true upon success and false upon
failure.

  $lobjId = $dbh->func($filename, 'lo_import');

Imports a Unix file as large object and returns the object id of the new
object or undef upon failure.

  $ret = $dbh->func($lobjId, $filename, 'lo_export');

Exports a large object into a Unix file. Returns false upon failure, true
otherwise.

  $ret = $dbh->func($line, 'putline');

Used together with the SQL-command 'COPY table FROM STDIN' to copy large
amount of data into a table avoiding the overhead of using single
insert commands. The application must explicitly send the two characters "\."
to indicate to the backend that it has finished sending its data.

  $ret = $dbh->func($buffer, length, 'getline');

Used together with the SQL-command 'COPY table TO STDOUT' to dump a complete
table.

  $ret = $dbh->func('pg_notifies');

Returns either undef or a reference to two-element array [ $table,
$backend_pid ] of asynchronous notifications received.

  $fd = $dbh->func('getfd');

Returns fd of the actual connection to server. Can be used with select() and
func('pg_notifies').

=back

=head1 ATTRIBUTES COMMON TO ALL HANDLES

=over 4

=item B<Warn> (boolean, inherited)

Implemented by DBI, no driver-specific impact.

=item B<Active> (boolean, read-only)

Supported by the driver as proposed by DBI. A database handle is active while
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

Supported by the driver as proposed by DBI. This method is similar to the
SQL-function RTRIM.

=item B<LongReadLen> (integer, inherited)

Implemented by DBI, not used by the driver.

=item B<LongTruncOk> (boolean, inherited)

Implemented by DBI, not used by the driver.

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

PostgreSQL does not have the concept of preparing a statement. Hence the
prepare method just stores the statement after checking for place-holders. No
information about the statement is available after preparing it.

=item B<prepare_cached>

  $sth = $dbh->prepare_cached($statement, \%attr);

Implemented by DBI, no driver-specific impact. This method is not useful for
this driver, because preparing a statement has no database interaction.

=item B<do>

  $rv  = $dbh->do($statement, \%attr, @bind_values);

Implemented by DBI, no driver-specific impact. See the notes for the execute
method elsewhere in this document.

=item B<commit>

  $rc  = $dbh->commit;

Supported by the driver as proposed by DBI. See also the notes about
B<Transactions> elsewhere in this document.

=item B<rollback>

  $rc  = $dbh->rollback;

Supported by the driver as proposed by DBI. See also the notes about
B<Transactions> elsewhere in this document.

=item B<disconnect>

  $rc  = $dbh->disconnect;

Supported by the driver as proposed by DBI.

=item B<ping>

  $rc = $dbh->ping;

This driver supports the ping method, which can be used to check the validity
of a database handle. The ping method issues an empty query and checks the
result status.

=item B<column_info>

	$sth = $dbh->column_info( $catalog, $schema, $table, $column );

Supported by the driver as proposed by the DBI with the follow exceptions.
These fields are currently always returned with NULL values:

   TABLE_CAT
   BUFFER_LENGTH
   DECIMAL_DIGITS
   NUM_PREC_RADIX
   SQL_DATA_TYPE
   SQL_DATETIME_SUB
   CHAR_OCTET_LENGTH

Also, four additional non-standard fields are returned:

  pg_type
  pg_type_only
  pg_attypmod
  pg_constraint - holds column constraint definition

The REMARKS field will be returned as NULL for Postgres versions 7.1.x and
older.

=item B<table_info>

  $sth = $dbh->table_info( $catalog, $schema, $table, $type );

Supported by the driver as proposed by DBI. This method returns all tables 
and views visible to the current user. The $catalog argument is currently 
unused. The schema and table arguments will do a 'LIKE' search if a 
percent sign (%) or an underscore (_) are detected in the argument.
The $type argument accepts a value of wither "TABLE" or "VIEW" 
(using both is the default action).

=item B<primary_key_info>

  $sth = $dbh->primary_key_info( $catalog, $schema, $table, \%attr );

Supported by the driver as proposed by DBI. The $catalog argument is 
curently unused, and the $schema argument has no effect against 
servers running version 7.2 or less. There are no search patterns allowed, 
but leaving the $schema argument blank will cause the first table 
found in the schema search path to be used. An additional field, DATA_TYPE, 
is returned and shows the data type for each of the arguments in the 
COLUMN_NAME field.

In addition to the standard format of returning one row for each column 
found for the primary key, you can pass the argument "pg_onerow" to force 
a single row to be used. If the primary key has multiple columns, the 
KEY_SEQ, COLUMN_NAME, and DATA_TYPE fields will return a comma-delimited 
string. If "pg_onerow" is set to "2", the fields will be returned as an 
arrayref, which can be useful when multiple columns are involved:

  $sth = $dbh->primary_key_info('', '', 'dbd_pg_test', {pg_onerow => 2});
  if (defined $sth) {
    my $pk = $sth->fetchall_arrayref()->[0];
    print "Table $pk->[2] has a primary key on these columns:\n";
    for (my $x=0; defined $pk->[3][$x]; $x++) {
      print "Column: $pk->[3][$x]  (data type: $pk->[6][$x])\n";
    }
  }

=item B<primary_key>

Supported by the driver as proposed by DBI.


=item B<foreign_key_info>

  $sth = $dbh->foreign_key_info( $pk_catalog, $pk_schema, $pk_table,
                                 $fk_catalog, $fk_schema, $fk_table );

Supported by the driver as proposed by DBI, using the SQL/CLI variant. 
This function returns undef for PostgreSQL servers earlier than version 
7.3. There are no search patterns allowed, but leaving the $schema argument 
blank will cause the first table found in the schema search path to be 
used. Two additional fields, UK_DATA_TYPE and FK_DATA_TYPE, are returned 
which show the data type for the unique and foreign key columns. Foreign 
keys which have no named constraint (where the referenced column only has 
an unique index) will return undef for the UK_NAME field.

=item B<tables>

  @names = $dbh->tables( $catalog, $schema, $table, $type, \%attr );

Supported by the driver as proposed by DBI. This method returns all tables 
and/or views which are visible to the current user: see the table_info() 
for more information about the arguments. If the database is version 7.3 
or higher, the name of the schema appears before the table or view name. This 
can be turned off by adding in the "noprefix" attribute:

  my @tables = $dbh->tables( '', '', 'dbd_pg_test', '', {noprefix => 1} );


=item B<type_info_all>

  $type_info_all = $dbh->type_info_all;

Supported by the driver as proposed by DBI. Information is only provided for 
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
the L<pgbuiltin>.

=item B<type_info>

  @type_info = $dbh->type_info($data_type);

Implemented by DBI, no driver-specific impact.

=item B<quote>

  $sql = $dbh->quote($value, $data_type);

This module implements its own quote method. In addition to the DBI method it
also doubles the backslash, because PostgreSQL treats a backslash as an escape
character.

B<NOTE:> The undocumented (and invalid) support for the C<SQL_BINARY> data
type is officially deprecated. Use C<PG_BYTEA> with C<bind_param()> instead:

  $rv = $sth->bind_param($param_num, $bind_value,
                         { pg_type => DBD::Pg::PG_BYTEA });

=back

=head2 Database Handle Attributes

=over 4

=item B<AutoCommit>  (boolean)

Supported by the driver as proposed by DBI. According to the classification of
DBI, PostgreSQL is a database, in which a transaction must be explicitly
started. Without starting a transaction, every change to the database becomes
immediately permanent. The default of AutoCommit is on, which corresponds to
the default behavior of PostgreSQL. When setting AutoCommit to off, a
transaction will be started and every commit or rollback will automatically
start a new transaction. For details see the notes about B<Transactions>
elsewhere in this document.

=item B<Driver>  (handle)

Implemented by DBI, no driver-specific impact.

=item B<Name>  (string, read-only)

The default method of DBI is overridden by a driver specific method, which
returns only the database name. Anything else from the connection string is
stripped off. Note, that here the method is read-only in contrast to the DBI
specs.

=item B<RowCacheSize>  (integer)

Implemented by DBI, not used by the driver.

=item B<pg_auto_escape> (boolean)

PostgreSQL specific attribute. If true, then quotes and backslashes in all
parameters will be escaped in the following way:

  escape quote with a quote (SQL)
  escape backslash with a backslash

The default is on. Note, that PostgreSQL also accepts quotes, which are
escaped by a backslash. Any other ASCII character can be used directly in a
string constant.

=item B<pg_enable_utf8> (boolean)

PostgreSQL specific attribute.  If true, then the utf8 flag will be
turned for returned character data (if the data is valid utf8).  For
details about the utf8 flag, see L<Encode>.  This is only relevant under
perl 5.8 and higher.

B<NB>: This attribute is experimental and may be subject to change.

=item B<pg_INV_READ> (integer, read-only)

Constant to be used for the mode in lo_creat and lo_open.

=item B<pg_INV_WRITE> (integer, read-only)

Constant to be used for the mode in lo_creat and lo_open.

=item B<pg_bool_tf> (boolean)

PostgreSQL specific attribute. If true, boolean values will be returned 
as the characters 't' and 'f' instead of '1' and '0'.

=back

=head1 DBI STATEMENT HANDLE OBJECTS

=head2 Statement Handle Methods

=over 4

=item B<bind_param>

  $rv = $sth->bind_param($param_num, $bind_value, \%attr);

Supported by the driver as proposed by DBI.

B<NOTE:> The undocumented (and invalid) support for the C<SQL_BINARY>
SQL type is officially deprecated. Use C<PG_BYTEA> instead:

  $rv = $sth->bind_param($param_num, $bind_value,
                         { pg_type => DBD::Pg::PG_BYTEA });

=item B<bind_param_inout>

Not supported by this driver.

=item B<execute>

  $rv = $sth->execute(@bind_values);

Supported by the driver as proposed by DBI. In addition to 'UPDATE', 'DELETE',
'INSERT' statements, for which it returns always the number of affected rows,
the execute method can also be used for 'SELECT ... INTO table' statements.

=item B<fetchrow_arrayref>

  $ary_ref = $sth->fetchrow_arrayref;

Supported by the driver as proposed by DBI.

=item B<fetchrow_array>

  @ary = $sth->fetchrow_array;

Supported by the driver as proposed by DBI.

=item B<fetchrow_hashref>

  $hash_ref = $sth->fetchrow_hashref;

Supported by the driver as proposed by DBI.

=item B<fetchall_arrayref>

  $tbl_ary_ref = $sth->fetchall_arrayref;

Implemented by DBI, no driver-specific impact.

=item B<finish>

  $rc = $sth->finish;

Supported by the driver as proposed by DBI.

=item B<rows>

  $rv = $sth->rows;

Supported by the driver as proposed by DBI. In contrast to many other drivers
the number of rows is available immediately after executing the statement.

=item B<bind_col>

  $rc = $sth->bind_col($column_number, \$var_to_bind, \%attr);

Supported by the driver as proposed by DBI.

=item B<bind_columns>

  $rc = $sth->bind_columns(\%attr, @list_of_refs_to_vars_to_bind);

Supported by the driver as proposed by DBI.

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
independent of any table by using so called object identifiers. This explains
why the blob_read method is blessed into the STATEMENT package and not part of
the DATABASE package. Here the field parameter has been used to handle this
object identifier. The offset and len parameter may be set to zero, in which
case the driver fetches the whole blob at once.

Starting with PostgreSQL-6.5 every access to a blob has to be put into a
transaction. This holds even for a read-only access.

See also the PostgreSQL-specific functions concerning blobs which are
available via the func-interface.

For further information and examples about blobs, please read the chapter
about Large Objects in the PostgreSQL Programmer's Guide.

=back

=head2 Statement Handle Attributes

=over 4

=item B<NUM_OF_FIELDS>  (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<NUM_OF_PARAMS>  (integer, read-only)

Implemented by DBI, no driver-specific impact.

=item B<NAME>  (array-ref, read-only)

Supported by the driver as proposed by DBI.

=item B<NAME_lc>  (array-ref, read-only)

Implemented by DBI, no driver-specific impact.

=item B<NAME_uc>  (array-ref, read-only)

Implemented by DBI, no driver-specific impact.

=item B<TYPE>  (array-ref, read-only)

Supported by the driver as proposed by DBI

=item B<PRECISION>  (array-ref, read-only)

Not supported by the driver.

=item B<SCALE>  (array-ref, read-only)

Not supported by the driver.

=item B<NULLABLE>  (array-ref, read-only)

Not supported by the driver.

=item B<CursorName>  (string, read-only)

Not supported by the driver. See the note about B<Cursors> elsewhere in this
document.

=item B<Statement>  (string, read-only)

Supported by the driver as proposed by DBI.

=item B<RowCache>  (integer, read-only)

Not supported by the driver.

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
command. Possible types are: INSERT, DELETE, UPDATE, SELECT.

=back

=head1 FURTHER INFORMATION

=head2 Transactions

The transaction behavior is now controlled with the attribute AutoCommit. For
a complete definition of AutoCommit please refer to the DBI documentation.

According to the DBI specification the default for AutoCommit is TRUE. In this
mode, any change to the database becomes valid immediately. Any 'begin',
'commit' or 'rollback' statement will be rejected.

If AutoCommit is switched-off, immediately a transaction will be started by
issuing a 'begin' statement. Any 'commit' or 'rollback' will start a new
transaction. A disconnect will issue a 'rollback' statement.

=head2 Large Objects

The driver supports all large-objects related functions provided by libpq via
the func-interface. Please note, that starting with PostgreSQL 6.5 any access
to a large object - even read-only - has to be put into a transaction!

=head2 Cursors

Although PostgreSQL has a cursor concept, it has not been used in the current
implementation. Cursors in PostgreSQL can only be used inside a transaction
block. Because only one transaction block at a time is allowed, this would
have implied the restriction, not to use any nested SELECT statements. Hence
the execute method fetches all data at once into data structures located in
the frontend application. This has to be considered when selecting large
amounts of data!

=head2 Datatype bool

The current implementation of PostgreSQL returns 't' for true and 'f' for
false. From the Perl point of view a rather unfortunate choice. The DBD::Pg
module translates the result for the data-type bool in a perl-ish like manner:
'f' -> '0' and 't' -> '1'. This way the application does not have to check the
database-specific returned values for the data-type bool, because Perl treats
'0' as false and '1' as true. You may set the pg_bool_tf attribute to change 
the values back to 't' and 'f' if you wish.

Boolean values can be passed to PostgreSQL as TRUE, 't', 'true', 'y', 'yes' or
'1' for true and FALSE, 'f', 'false', 'n', 'no' or '0' for false.

=head2 Schema support

PostgreSQL version 7.3 introduced schema support. Note that the PostgreSQL
schema concept may differ to that of other databases. Please refer to the
PostgreSQL documentation for more details.

Currently DBD::Pg does not provide explicit support for PostgreSQL schemas.
However, schema functionality may be used without any restrictions by
explicitly addressing schema objects, e.g.

  my $res = $dbh->selectall_arrayref("SELECT * FROM my_schema.my_table");

or by manipulating the schema search path with SET search_path, e.g.

  $dbh->do("SET search_path TO my_schema, public");

=head1 SEE ALSO

L<DBI>

=head1 AUTHORS

DBI and DBD-Oracle by Tim Bunce (Tim.Bunce@ig.co.uk)

DBD-Pg by Edmund Mergl (E.Mergl@bawue.de) and Jeffrey W. Baker
(jwbaker@acm.org). By David Wheeler <david@wheeler.net>, Jason
Stewart <jason@openinformatics.com> and Bruce Momjian
<pgman@candle.pha.pa.us> and others after v1.13.

Major parts of this package have been copied from DBI and DBD-Oracle.

B<Mailing List>

The current maintainers may be reached through the 'dbdpg-general' mailing
list: L<http://gborg.postgresql.org/mailman/listinfo/dbdpg-general/>.

This list is available through Gmane (L<http://www.gmane.org/>) as a newsgroup
with the name: C<gmane.comp.db.postgresql.dbdpg>

=head1 COPYRIGHT

The DBD::Pg module is free software. You may distribute under the terms of
either the GNU General Public License or the Artistic License, as specified in
the Perl README file.

=head1 ACKNOWLEDGMENTS

See also B<DBI/ACKNOWLEDGMENTS>.

=cut

