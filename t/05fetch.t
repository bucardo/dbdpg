use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 7;
} else {
  plan skip_all => 'cannot test without DB info';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		       {RaiseError => 1, AutoCommit => 0}
		      );
ok(defined $dbh,
   'connect with transaction'
  );

$dbh->do(q{INSERT INTO test (id, name, val) VALUES (1, 'foo', 'horse')});
$dbh->do(q{INSERT INTO test (id, name, val) VALUES (2, 'bar', 'chicken')});
$dbh->do(q{INSERT INTO test (id, name, val) VALUES (3, 'baz', 'pig')});
ok($dbh->commit(),
   'commit'
   );

my $sql = <<SQL;
  SELECT id
  , name
  FROM test
SQL
my $sth = $dbh->prepare($sql);
$sth->execute();

my $rows = 0;
while (my ($id, $name) = $sth->fetchrow_array()) {
  if (defined($id) && defined($name)) {
    $rows++;
  }
}
$sth->finish();
ok($rows == 3,
   'fetch three rows'
  );

$sql = <<SQL;
       SELECT id
       , name
       FROM test
       WHERE 1 = 0
SQL
$sth = $dbh->prepare($sql);
$sth->execute();

$rows = 0;
while (my ($id, $name) = $sth->fetchrow_array()) {
  $rows++;
}
$sth->finish();

ok($rows == 0,
   'fetch zero rows'
   );

$sql = <<SQL;
       SELECT id
       , name
       FROM test
       WHERE id = ?
SQL
$sth = $dbh->prepare($sql);
$sth->execute(1);

$rows = 0;
while (my ($id, $name) = $sth->fetchrow_array()) {
  if (defined($id) && defined($name)) {
    $rows++;
  }
}
$sth->finish();

ok($rows == 1,
   'fetch one row on id'
  );

$sql = <<SQL;
       SELECT id
       , name
       FROM test
       WHERE name = ?
SQL
$sth = $dbh->prepare($sql);
$sth->execute('foo');

$rows = 0;
while (my ($id, $name) = $sth->fetchrow_array()) {
  if (defined($id) && defined($name)) {
    $rows++;
  }
}
$sth->finish();

ok($rows == 1,
   'fetch one row on name'
   );

ok($dbh->disconnect(),
   'disconnect'
  );
