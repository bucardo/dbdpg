use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 4;
} else {
  plan skip_all => 'cannot test without DB info';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		    {RaiseError => 1, AutoCommit => 1});
ok(defined $dbh,'connect without transaction');
{
  local $dbh->{PrintError} = 0;
  local $dbh->{RaiseError} = 0;
  $dbh->do(q{DROP TABLE test});
}

my $sql = <<SQL;
CREATE TABLE test (
  id int,
  name text,
  val text,
  score float,
  date timestamp default 'now()',
  array text[][]
)
SQL

ok($dbh->do($sql),
   'create table'
  );

eval { local $dbh->{PrintError} = 0; $dbh->do( "drop table test2" ) };
{
    my $warning;
    local $SIG{__WARN__} = sub { $warning = "@_" };
    $dbh->do( "create table test2 (id integer primary key)" );
    # XXX This will have to be updated if PostgreSQL ever changes its
    # warnings...
    like($warning, '/^NOTICE:.*CREATE TABLE/',
	 'PQsetNoticeProcessor working' );
}
eval { local $dbh->{PrintError} = 0; $dbh->do( "drop table test2" ) };

ok($dbh->disconnect(),
   'disconnect'
  );

