use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
    plan tests => 8;
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
        SELECT *
          FROM dbd_pg_test
SQL

ok($dbh->prepare($sql),
   "prepare: $sql"
  );

$sql = <<SQL;
        SELECT id
          FROM dbd_pg_test
SQL

ok($dbh->prepare($sql),
   "prepare: $sql"
  );

$sql = <<SQL;
        SELECT id
             , name
          FROM dbd_pg_test
SQL

ok($dbh->prepare($sql),
   "prepare: $sql"
  );

$sql = <<SQL;
        SELECT id
             , name
          FROM dbd_pg_test
         WHERE id = 1
SQL

ok($dbh->prepare($sql),
   "prepare: $sql"
  );

$sql = <<SQL;
        SELECT id
             , name
          FROM dbd_pg_test
         WHERE id = ?
SQL

ok($dbh->prepare($sql),
   "prepare: $sql"
  );

$sql = <<SQL;
        SELECT *
           FROM dbd_pg_test
         WHERE id = ?
           AND name = ?
           AND val = ?
           AND score = ?
           and date = ?
SQL

ok($dbh->prepare($sql),
   "prepare: $sql"
  );

ok($dbh->disconnect(),
   'disconnect'
  );
