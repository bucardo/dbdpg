#!/usr/bin/perl -w -I./t
$| = 1;

use DBI qw(:sql_types);
use strict;
use Test::More;

if (defined $ENV{DBI_DSN}) {
    plan tests => 26;
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


# Column Info
 eval { $sth = $dbh->column_info(); };

ok ((!$@ and defined $sth), "column_info with no arguments") or diag $@;
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


ok($dbh->do("COMMENT ON COLUMN dbd_pg_test.name IS 'Success'"), 'comment on dbd_pg_test_table');

                                                                                                                                                     
$dbh->do("COMMENT ON COLUMN dbd_pg_col_info.myvalue IS 'test'");

my $rows;
eval {	
	$sth = $dbh->column_info( undef, undef, 'dbd_pg_test','name' );
	$rows = $sth->fetchall_arrayref({});
};	
is($@,'', 'column_info call survives');
is(scalar @$rows, 1, 'column_info REMARKS returns number of expected rows');
my $row = $rows->[0];

    SKIP: {
        my $ver = DBD::Pg::_pg_server_version($dbh);
        my $at_least_7_2 = DBD::Pg::_pg_check_version(7.2, $ver);
        skip "column_info REMARKS will be NULL below 7.2 (Current version: $ver)", 1 unless $at_least_7_2;
        is($row->{REMARKS},'Success','column_info REMARKS has expected value');

    }

$sth = undef;
#########


 	like($row->{COLUMN_DEF},"/^'Testing Default'(?:::character varying)?\$/",'column_info default value');

	cmp_ok($row->{COLUMN_SIZE},'==', 20, 'column_info field size for type varchar');
	cmp_ok($row->{DATA_TYPE},'==', 12, 'column_info data type varchar');

	$sth = $dbh->column_info( undef, undef, 'dbd_pg_test','score' );
	$row = $sth->fetchrow_hashref;
	like($row->{pg_constraint}
		, qr/\(\(\(score\s+=\s+1(?:::double\s+precision)?\)\s+OR\s+\(score\s+=\s+2(?:::double precision)?\)\)\s+OR\s+\(score\s+=\s+3(?:::double precision)?\)\)/i
		, 'column_info constraints');

    eval {
	    $sth = $dbh->column_info( undef, undef, 'dbd_pg_test','date' );
        $row = $sth->fetchrow_hashref;
    };

    my $ver = DBD::Pg::_pg_server_version($dbh);

    # Modern version of Postgres will create the field as the type SQL_TYPE_TIMESTAMP (93)
    # Postgres 7.2, 7.1, and presumably earlier, will create the field as SQL_TYPE_TIMESTAMP_WITH_TIMEZONE (95)
    my $expected_type = DBD::Pg::_pg_check_version(7.3, $ver) ? 93 : 95;

    is($row->{DATA_TYPE},$expected_type, 'timestamp column has expected numeric data type');

    #####


    ######
    
    ok($dbh->disconnect, 'Disconnect');


exit(0);

