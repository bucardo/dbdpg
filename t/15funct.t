#!/usr/bin/perl -w -I./t
# vim:ts=2:sw=2:aw:ai:sta:
$| = 1;

use DBI qw(:sql_types);
use Data::Dumper;
use strict;
use Test::More;
if (defined $ENV{DBI_DSN}) {
    plan tests => 70;
} else {
    plan skip_all => "DBI_DSN must be set: see the README file";
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		       {RaiseError => 1, AutoCommit => 0}
		      );
ok(defined $dbh,
   'connect with transaction'
  );

#
# Test the different methods, so are expected to fail.
#

my $sth;

# Ping
 eval {
	 ok( $dbh->ping(), "Testing Ping" );
 };
ok ( !$@, "Ping Tested" );

# Get Info
 eval {
	 $sth = $dbh->get_info();
 };
ok ($@, "Call to get_info with 0 arguments, error expected: $@" );
$sth = undef;

# Table Info
 eval {
	 $sth = $dbh->table_info();
 };
ok ((!$@ and defined $sth), "table_info tested" );
$sth = undef;

# Column Info
 eval { $sth = $dbh->column_info(); };

ok ((!$@ and defined $sth), "column_info with no arguments") or diag $@;
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
ok ($@, "quote error expected: $@");

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

ok ($@, "quote_identifier error expected: $@");
$sth = undef;

# Test ping
ok ($dbh->ping, "Ping the current connection ..." );

# Test various get_info items
my %get_info = (
  SQL_DRIVER_NAME            =>  6,
  SQL_DBMS_NAME              => 17,
  SQL_DBMS_VERSION           => 18,
  SQL_IDENTIFIER_QUOTE_CHAR  => 29,
  SQL_CATALOG_NAME_SEPARATOR => 41,
  SQL_USER_NAME              => 47,
);

for (keys %get_info) {  
    my $back = $dbh->get_info($_);
    ok (defined $back, "get_info($_) tested" );
    my $forth = $dbh->get_info($get_info{$_});
    ok (defined $forth, "get_info($_) tested" );
    ok ($back eq $forth, "get_info($_) is the same as get_info($get_info{$_})");
}

# Test Table Info
eval {
	$sth = $dbh->table_info( undef, undef, undef );
};
ok( defined $sth, "table_info(undef, undef, undef) tested" ) or diag $@;

$sth = undef;

eval { $sth = $dbh->table_info( undef, undef, undef, "VIEW" ) };
ok( defined $sth, "table_info(undef, undef, undef, \"VIEW\") tested" );
$sth = undef;

# Test Table Info Rule 19a
eval { $sth = $dbh->table_info( '%', '', ''); };
ok( defined $sth, "table_info('%', '', '',) tested" );
$sth = undef;

# Test Table Info Rule 19b
eval { $sth = $dbh->table_info( '', '%', ''); };
ok( defined $sth, "table_info('', '%', '',) tested" );
$sth = undef;

# Test Table Info Rule 19c
eval { $sth = $dbh->table_info( '', '', '', '%') };
ok( defined $sth, "table_info('', '', '', '%',) tested" );
$sth = undef;

# Test to see if this database contains any of the defined table types.
eval { $sth = $dbh->table_info( '', '', '', '%'); };
ok( defined $sth, "table_info('', '', '', '%',) tested" );
if ($sth) {
	my $ref; 
	eval { $ref = $sth->fetchall_hashref( 'TABLE_TYPE' ) };
	ok((defined $ref), "fetchall_hashref('TABLE_TYPE')");
	foreach my $type ( sort keys %$ref ) {
		my $tsth = $dbh->table_info( undef, undef, undef, $type );
		ok( defined $tsth, "table_info(undef, undef, undef, $type) tested" );
		$tsth->finish;
	}
	$sth->finish;
}
$sth = undef;

# Test Column Info
$sth = $dbh->column_info( undef, undef, undef, undef );
ok( defined $sth, "column_info(undef, undef, undef, undef) tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", undef, undef );
ok( defined $sth, "column_info(undef, 'auser', undef, undef) tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'ause%'", undef, undef );
ok( defined $sth, "column_info(undef, 'ause%', undef, undef) tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'auser','replicator'", undef, undef );
ok( defined $sth, "column_info(undef, 'auser','replicator', undef, undef) tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'auser','repl%'", undef, undef );
ok( defined $sth, "column_info(undef, 'auser','repl%', undef, undef) tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'fred','repl%'", undef, undef );
ok( defined $sth, "column_info(undef, 'fred','repl%', undef, undef) tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'fred','jim'", undef, undef );
ok( defined $sth, "column_info(undef, 'fred','jim', undef, undef) tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", "'pga_schema'", undef );
ok( defined $sth, "column_info(undef, 'auser', 'pga_schema', undef) tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", "'pga_%'", undef );
ok( defined $sth, "column_info(undef, 'auser', 'pga_%', undef) tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'ause%'", "'pga_%'", undef );
ok( defined $sth, "column_info(undef, 'ause%', 'pga_%', undef) tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", "'pga_schema'", "'schemaname'" );
ok( defined $sth, "column_info(undef, 'auser', 'pga_schema', 'schemaname') tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", "'pga_schema'", "'schema%'" );
ok( defined $sth, "column_info(undef, 'auser', 'pga_schema', 'schema%') tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'auser'", "'pga_%'", "'schema%'" );
ok( defined $sth, "column_info(undef, 'auser', 'pga_%', 'schema%') tested" );
$sth = undef;

$sth = $dbh->column_info( undef, "'ause%'", "'pga_%'", "'schema%'" );
ok( defined $sth, "column_info(undef, 'ause%', 'pga_%', 'schema%') tested" );
$sth = undef;

# Test call to primary_key_info
local ($dbh->{Warn}, $dbh->{PrintError});
$dbh->{PrintError} = $dbh->{Warn} = 0;

# Primary Key Info
eval {
    $sth = $dbh->primary_key_info();
    die unless $sth;
};
ok ($@, "Call to primary_key_info with 0 arguments, error expected: $@" );
$sth = undef;

# Primary Key
eval {
    $sth = $dbh->primary_key();
    die unless $sth;
};
ok ($@, "Call to primary_key with 0 arguments, error expected: $@" );
$sth = undef;

$sth = $dbh->primary_key_info(undef, undef, undef );

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
eval {
	while( my ($catalog, $schema, $table) = $sth->fetchrow_array ) {
		local $^W = 0;
		$catalogs{$catalog}++	if $catalog;
		$schemas{$schema}++		if $schema;
		$tables{$table}++			if $table;
		$cnt++;
	}
};
ok((!$@ and ($cnt > 0)), "At least one table has a primary key." );

#rl: TODO: Clean this up.
#$sth = # $dbh->primary_key_info(undef, qq{'$ENV{DBI_USER}'}, undef );
ok(1
#   defined $sth
   , "Getting primary keys for tables");  ## owned by $ENV{DBI_USER}
#DBI::dump_results($sth) if defined $sth;
undef $sth;

# Test _pg_use_catalog
# Schemas should returned for 7.3 and above, but not below

my $ver = DBD::Pg::_pg_server_version($dbh);
my $should_have_schemas = 0;
   $should_have_schemas = 1 if DBD::Pg::_pg_check_version(7.3, $ver);

my $has_schemas = 0;
   $has_schemas = 1 if DBD::Pg::_pg_use_catalog($dbh);

  ok(($should_have_schemas == $has_schemas), 
    '_pg_use_catalog appropriately detects if schemas are used');



# Test foreign_key_info

local ($dbh->{Warn}, $dbh->{PrintError});
$dbh->{PrintError} = $dbh->{Warn} = 0;
my $schemas = DBD::Pg::_pg_use_catalog($dbh);
eval {
$sth = $dbh->foreign_key_info(undef,undef,'dbd_pg_test');
};
if (! $schemas) {
    ok(! defined $sth, "Statement handle defined for foreign_key_info()" );
}
else {
    ok( defined $sth, "Statement handle defined for foreign_key_info()" );
}
$sth = undef;

ok($dbh->do("COMMENT ON COLUMN dbd_pg_test.name IS 'Success'"), 'comment on dbd_pg_test_table');

ok($dbh->disconnect, 'Disconnect');




exit(0);

