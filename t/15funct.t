#!/usr/bin/perl -w -I./t
$| = 1;

# vim:ts=2:sw=2:ai:aw:nu:
use DBI qw(:sql_types);
use Data::Dumper;

use strict;

my ($pf, $sf);

use Test::More tests => 59;

if (!exists($ENV{DBDPG_MAINTAINER})) {
    print "1..0\n";
    exit;
}

my $dbh = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 0});
    
# my $dbh = DBI->connect() or die "Connect failed: $DBI::errstr\n";
ok ( defined $dbh, "Connection" ); # print "ok 2\n";

#
# Test the different methods, so are expected to fail.
#

my $sth;

# foreach (@{ $DBI::EXPORT_TAGS{sql_types} }) {
# 	no strict 'refs';
# 	printf "%s=%d\n", $_, &{"DBI::$_"};
# }

my $get_info = {
	  SQL_DBMS_NAME	=> 17
	, SQL_DBMS_VER	=> 18
	, SQL_IDENTIFIER_QUOTE_CHAR	=> 29
	, SQL_CATALOG_NAME_SEPARATOR	=> 41
	, SQL_CATALOG_LOCATION	=> 114
};

# Ping
 eval {
	 ok( $dbh->ping(), "Testing Ping" );
 };
ok ( !$@, "Ping Tested" );

# Get Info
 eval {
	 $sth = $dbh->get_info();
 };
ok ($@, "Call to get_info with 0 arguements, error expected: $@" );
$sth = undef;

# Table Info
 eval {
	 $sth = $dbh->table_info();
 };
ok ((!$@ and defined $sth), "table_info tested" );
$sth = undef;

# Column Info
 eval {
	 $sth = $dbh->column_info();
 };
ok ((!$@ and defined $sth), "column_info tested" );
#ok ($@, "Call to column_info with 0 arguements, error expected: $@" );
$sth = undef;


# Tables
 eval {
	 $sth = $dbh->tables();
 };
ok ((!$@ and defined $sth), "tables tested" );
$sth = undef;

# Type Info All
 eval {
	 $sth = $dbh->type_info_all();
 };
ok ((!$@ and defined $sth), "type_info_all tested" );
$sth = undef;

# Type Info
 eval {
	 my @types = $dbh->type_info();
	die unless @types;
 };
ok (!$@, "type_info(undef)");
$sth = undef;

# Quote
 eval {
	my $val = $dbh->quote();
 	die unless $val;
 };
ok ($@, "quote error expected, $@ " . $dbh->errstr );
$sth = undef;
# Tests for quote:
my @qt_vals = (1, 2, undef, 'NULL', "ThisIsAString", "This is Another String");
my @expt_vals = (q{'1'}, q{'2'}, "NULL", q{'NULL'}, q{'ThisIsAString'}, q{'This is Another String'});
for (my $x = 0; $x <= $#qt_vals; $x++) {
	local $^W = 0;
	my $val = $dbh->quote( $qt_vals[$x] );	
	is( $val, $expt_vals[$x], "$x: quote on $qt_vals[$x] returned $val" );
}

is( $dbh->quote( 1, SQL_INTEGER() ), 1, "quote(1, SQL_INTEGER)" );


# Quote Identifier
 eval {
	 my $val = $dbh->quote_identifier();
 	die unless $val;
 };
ok ($@, "quote_identifier error expected $@ " .  $dbh->errstr );
$sth = undef;

#	, SQL_IDENTIFIER_QUOTE_CHAR	=> 29
#	, SQL_CATALOG_NAME_SEPARATOR	=> 41
my $qt  = $dbh->get_info( $get_info->{SQL_IDENTIFIER_QUOTE_CHAR} );
my $sep = $dbh->get_info( $get_info->{SQL_CATALOG_NAME_SEPARATOR} );

my $cmp_str = qq{${qt}link${qt}${sep}${qt}schema${qt}${sep}${qt}table${qt}};
is( $dbh->quote_identifier( "link", "schema", "table" )
	, $cmp_str
	, q{quote_identifier( "link", "schema", "table" )}
);

# Test ping

ok ($dbh->ping, "Ping the current connection ..." );

# Test Get Info.

#	SQL_KEYWORDS
#	SQL_CATALOG_TERM
#	SQL_DATA_SOURCE_NAME
#	SQL_DBMS_NAME
#	SQL_DBMS_VERSION
#	SQL_DRIVER_NAME
#	SQL_DRIVER_VER
#	SQL_PROCEDURE_TERM
#	SQL_SCHEMA_TERM
#	SQL_TABLE_TERM
#	SQL_USER_NAME

foreach my $info (sort keys %$get_info) 
{
	my $type =  $dbh->get_info($get_info->{$info});
	ok( defined $type,  "get_info($info) ($get_info->{$info}) $type" );
}

# Test Table Info
$sth = $dbh->table_info( undef, undef, undef );
ok( defined $sth, "table_info(undef, undef, undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->table_info( undef, undef, undef, "VIEW" );
ok( defined $sth, "table_info(undef, undef, undef, \"VIEW\") tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

# Test Table Info Rule 19a
$sth = $dbh->table_info( '%', '', '');
ok( defined $sth, "table_info('%', '', '',) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

# Test Table Info Rule 19b
$sth = $dbh->table_info( '', '%', '');
ok( defined $sth, "table_info('', '%', '',) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

# Test Table Info Rule 19c
$sth = $dbh->table_info( '', '', '', '%');
ok( defined $sth, "table_info('', '', '', '%',) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

# Test to see if this database contains any of the defined table types.
$sth = $dbh->table_info( '', '', '', '%');
ok( defined $sth, "table_info('', '', '', '%',) tested" );
if ($sth) {
	my $ref = $sth->fetchall_hashref( 'TABLE_TYPE' );
	foreach my $type ( sort keys %$ref ) {
		my $tsth = $dbh->table_info( undef, undef, undef, $type );
		ok( defined $tsth, "table_info(undef, undef, undef, $type) tested" );
		DBI::dump_results($tsth) if defined $tsth;
		$tsth->finish;
	}
	$sth->finish;
}
$sth = undef;

# Test Column Info
$sth = $dbh->column_info( undef, undef, undef, undef );
ok( defined $sth, "column_info(undef, undef, undef, undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", undef, undef );
ok( defined $sth, "column_info(undef, 'auser', undef, undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'ause%'", undef, undef );
ok( defined $sth, "column_info(undef, 'ause%', undef, undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'auser','replicator'", undef, undef );
ok( defined $sth, "column_info(undef, 'auser','replicator', undef, undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'auser','repl%'", undef, undef );
ok( defined $sth, "column_info(undef, 'auser','repl%', undef, undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'fred','repl%'", undef, undef );
ok( defined $sth, "column_info(undef, 'fred','repl%', undef, undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'fred','jim'", undef, undef );
ok( defined $sth, "column_info(undef, 'fred','jim', undef, undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", "'pga_schema'", undef );
ok( defined $sth, "column_info(undef, 'auser', 'pga_schema', undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", "'pga_%'", undef );
ok( defined $sth, "column_info(undef, 'auser', 'pga_%', undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'ause%'", "'pga_%'", undef );
ok( defined $sth, "column_info(undef, 'ause%', 'pga_%', undef) tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", "'pga_schema'", "'schemaname'" );
ok( defined $sth, "column_info(undef, 'auser', 'pga_schema', 'schemaname') tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", "'pga_schema'", "'schema%'" );
ok( defined $sth, "column_info(undef, 'auser', 'pga_schema', 'schema%') tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", "'pga_%'", "'schema%'" );
ok( defined $sth, "column_info(undef, 'auser', 'pga_%', 'schema%') tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

$sth = $dbh->column_info( undef, "'ause%'", "'pga_%'", "'schema%'" );
ok( defined $sth, "column_info(undef, 'ause%', 'pga_%', 'schema%') tested" );
DBI::dump_results($sth) if defined $sth;
$sth = undef;

SKIP: {
	# Test call to primary_key_info
	local ($dbh->{Warn}, $dbh->{PrintError});
	$dbh->{PrintError} = $dbh->{Warn} = 0;

	# Primary Key Info
	eval {
		$sth = $dbh->primary_key_info();
		die unless $sth;
	};
	ok ($@, "Call to primary_key_info with 0 arguements, error expected: $@" );
	$sth = undef;

	# Primary Key
	eval {
		$sth = $dbh->primary_key();
		die unless $sth;
	};
	ok ($@, "Call to primary_key with 0 arguements, error expected: $@" );
	$sth = undef;

	$sth = $dbh->primary_key_info(undef, undef, undef );

	skip "primary_key_info not supported by provider", 5 unless $sth;

	ok( defined $sth, "Statement handle defined for primary_key_info()" );

	if ( defined $sth ) {
		while( my $row = $sth->fetchrow_arrayref ) {
				local $^W = 0;
				# print join( ", ", @$row, "\n" );	
		}

		undef $sth;

	}

	$sth = $dbh->primary_key_info(undef, undef, undef );
	ok( defined $sth, "Statement handle defined for primary_key_info()" );

	my ( %catalogs, %schemas, %tables);

	my $cnt = 0;
	while( my ($catalog, $schema, $table) = $sth->fetchrow_array ) {
		local $^W = 0;
		$catalogs{$catalog}++	if $catalog;
		$schemas{$schema}++		if $schema;
		$tables{$table}++			if $table;
		$cnt++;
	}
	ok( $cnt > 0, "At least one table has a primary key." );

	$sth = $dbh->primary_key_info(undef, qq{'$ENV{DBI_USER}'}, undef );
	ok( 
		defined $sth
		, "Getting primary keys for tables owned by $ENV{DBI_USER}");
	DBI::dump_results($sth) if defined $sth;

}

undef $sth;

SKIP: {
	# foreign_key_info
	local ($dbh->{Warn}, $dbh->{PrintError});
	$dbh->{PrintError} = $dbh->{Warn} = 0;
	eval {
	$sth = $dbh->foreign_key_info();
		die unless $sth;
	};
	skip "foreign_key_info not supported by driver", 1 if $@;
	ok( defined $sth, "Statement handle defined for foreign_key_info()" );
	DBI::dump_results($sth) if defined $sth;
	$sth = undef;
}

ok( !$dbh->disconnect, "Disconnect from database" );

exit(0);

