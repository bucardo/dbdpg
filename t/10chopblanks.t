use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
    plan tests => 8;
} else {
    plan skip_all => "DBI_DSN must be set: see the README file";
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
	{
		RaiseError => 1,
		AutoCommit => 0,
		ChopBlanks => 1
	}
);

ok( defined $dbh, 'connect to database with ChopBlanks attribute set.' );

$dbh->do(q{INSERT INTO dbd_pg_test (id, fixed) VALUES (111, 'fiver')});
$dbh->do(q{INSERT INTO dbd_pg_test (id, fixed) VALUES (222, 'foo')});
$dbh->do(q{INSERT INTO dbd_pg_test (id, fixed, val) VALUES (333, ' bar', 'waldo  ')});

my ($val) = $dbh->selectrow_array(q{SELECT fixed FROM dbd_pg_test WHERE id = 111});
is( $val, 'fiver', "Check value for 'fiver'" );

($val) = $dbh->selectrow_array(q{SELECT fixed FROM dbd_pg_test WHERE id = 222});
is( $val, 'foo', "Check value for 'foo'" );

($val) = $dbh->selectrow_array(q{SELECT fixed FROM dbd_pg_test WHERE id = 333});
is( $val, ' bar', "Check value for ' bar'" );

$dbh->{ChopBlanks}=0;

($val) = $dbh->selectrow_array(q{SELECT fixed FROM dbd_pg_test WHERE id = 333});
is( $val, ' bar ', "Check value for ' bar '" );

$dbh->{ChopBlanks}=1;

($val) = $dbh->selectrow_array(q{SELECT fixed FROM dbd_pg_test WHERE id = 333});
is( $val, ' bar', "Check value for ' bar'" );

($val) = $dbh->selectrow_array(q{SELECT val FROM dbd_pg_test WHERE id = 333});
is( $val, 'waldo  ', "Check value for 'waldo  '" );

$dbh->do(q{DELETE from dbd_pg_test WHERE fixed IS NOT NULL});

ok( $dbh->disconnect, "Disconnect from database" );
