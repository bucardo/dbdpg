use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
    plan tests => 2;
} else {
    plan skip_all => "DBI_DSN must be set: see the README file";
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		       {RaiseError => 1, AutoCommit => 0}
		      );
ok(defined $dbh,
   'connect with transaction'
  );

my $sql = <<SQL;
  SELECT 0
SQL
my $sth = $dbh->prepare($sql);
$sth->execute();
$sth->fetchall_arrayref();

ok($sth->rows(), 'rows');
$dbh->rollback;
$dbh->disconnect;
