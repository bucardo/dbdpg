use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
    plan tests => 12;
} else {
    plan skip_all => "DBI_DSN must be set: see the README file";
}

my $dbh1 = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
			{RaiseError => 1, AutoCommit => 1}
		       );
ok(defined $dbh1,
   'connect first dbh'
  );

my $dbh2 = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
			{RaiseError => 1, AutoCommit => 1}
		       );
ok(defined $dbh2,
   'connect second dbh'
  );

ok($dbh1->do(q{DELETE FROM dbd_pg_test}),
   'delete'
  );

my $rows = ($dbh1->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test}))[0];
ok($rows == 0,
   'fetch on empty table from dbh1'
  );

$rows = ($dbh2->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test}))[0];
ok($rows == 0,
   'fetch on empty table from dbh2'
  );

ok($dbh1->do(q{INSERT INTO dbd_pg_test (id, name, val) VALUES (1, 'foo', 'horse')}),
   'insert'
  );

$rows = ($dbh1->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test}))[0];
ok($rows == 1,
   'fetch one row from dbh1'
  );

$rows = ($dbh2->selectrow_array(q{SELECT COUNT(*) FROM dbd_pg_test}))[0];
ok($rows == 1,
   'fetch one row from dbh1'
  );

local $SIG{__WARN__} = sub {};
ok(!$dbh1->commit(),
   'commit'
  );

ok(!$dbh1->rollback(),
   'rollback'
  );

ok($dbh1->disconnect(),
   'disconnect on dbh1'
);

ok($dbh2->disconnect(),
   'disconnect on dbh2'
);
