use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
    plan tests => 6;
} else {
    plan skip_all => "DBI_DSN must be set: see the README file";
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		    {RaiseError => 1, PrintError => 1, AutoCommit => 1});
ok(defined $dbh,'connect without transaction');
{
  local $dbh->{PrintError} = 0;
  local $dbh->{RaiseError} = 0;
  $dbh->do(q{DROP TABLE dbd_pg_test});
}

my $sql = <<SQL;
CREATE TABLE dbd_pg_test (
  id integer not null primary key,
  name varchar(20) default 'Testing Default' ,
  val text,
  score float CHECK(score IN ('1','2','3')),
  fixed character(5),
  date timestamp default 'now()',
  testarray text[][]
)
SQL

## The "dbd_pg_test" table is so important we bail if we cannot create it
## We also check that we can capture the notice raised for the primary key
{
  my $warning;
  local $SIG{__WARN__} = sub { $warning = "@_" };
  $dbh->{RaiseError}=0;
  ok($dbh->do($sql),
    'create table'
    ) or print STDOUT qq{Bail out! Could not create temporary table "dbd_pg_test"\n};
  $dbh->{RaiseError}=1;
  like($warning, '/^NOTICE:.*CREATE TABLE/', 'PQsetNoticeProcessor working' );

}

# A second table is needed to test to make sure we return the right number of rows
# for some joins
ok($dbh->do(" CREATE TABLE dbd_pg_col_info ( myvalue character varying(20) )"), 'CREATE table dbd_pg_col_Info');


# Test that we can disable warnings using $dbh.
eval { local $dbh->{PrintError} = 0; $dbh->do( "DROP TABLE dbd_pg_test2" ) };
{
    my $warning;
    local $SIG{__WARN__} = sub { $warning = "@_" };
    local $dbh->{Warn} = 0;
    $dbh->do( "CREATE TEMPORARY TABLE dbd_pg_test2 (id integer primary key)" );
    # XXX This will have to be updated if PostgreSQL ever changes its
    # warnings...
    is( $warning, undef, 'PQsetNoticeProcessor respects dbh->{Warn}' );
}
eval { local $dbh->{PrintError} = 0; $dbh->do( "DROP TABLE dbd_pg_test2" ) };

ok($dbh->disconnect(),
   'disconnect'
  );

