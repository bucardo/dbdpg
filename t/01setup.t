use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 5;
} else {
  plan skip_all => 'cannot test without DB info';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		    {RaiseError => 1, AutoCommit => 1});
ok(defined $dbh,'connect without transaction');
{
  local $dbh->{PrintError} = 0;
  local $dbh->{RaiseError} = 0;
  $dbh->do(q{DROP TABLE dbd_pg_test});
}

my $sql = <<SQL;
CREATE TABLE dbd_pg_test (
  id int,
  name text,
  val text,
  fixed character(5),
  score float,
  date timestamp default 'now()',
  testarray text[][]
)
SQL

## The "dbd_pg_test" table is so important we bail if we cannot create it
$dbh->{RaiseError}=0;
ok($dbh->do($sql),
   'create table'
  ) or print STDOUT qq{Bail out! Could not create temporary table "dbd_pg_test"\n};
$dbh->{RaiseError}=1;

# First, test that we can trap warnings.
eval { local $dbh->{PrintError} = 0; $dbh->do( "DROP TABLE dbd_pg_test2" ) };
{
    my $warning;
    local $SIG{__WARN__} = sub { $warning = "@_" };
    $dbh->do( "CREATE TEMPORARY TABLE dbd_pg_test2 (id integer primary key)" );
    # XXX This will have to be updated if PostgreSQL ever changes its warnings...
    like($warning, '/^NOTICE:.*CREATE TABLE/',
	 'PQsetNoticeProcessor working' );
}
eval { local $dbh->{PrintError} = 0; $dbh->do( "DROP TABLE dbd_pg_test2" ) };

# Next, test that we can disable warnings using $dbh.
eval { local $dbh->{PrintError} = 0; $dbh->do( "DROP TABLE dbd_pg_test3" ) };
{
    my $warning;
    local $SIG{__WARN__} = sub { $warning = "@_" };
    local $dbh->{Warn} = 0;
    $dbh->do( "CREATE TEMPORARY TABLE dbd_pg_test3 (id integer primary key)" );
    # XXX This will have to be updated if PostgreSQL ever changes its
    # warnings...
    is( $warning, undef, 'PQsetNoticeProcessor respects dbh->{Warn}' );
}
eval { local $dbh->{PrintError} = 0; $dbh->do( "DROP TABLE dbd_pg_test3" ) };

ok($dbh->disconnect(),
   'disconnect'
  );

