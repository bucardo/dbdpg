use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 15;
} else {
  plan skip_all => 'cannot test without DB info';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		       {RaiseError => 1, AutoCommit => 0}
		      );
ok(defined $dbh,
   'connect with transaction'
  );

# Insert into array
my $values = [["a,b", 'c","d', "e'"], ['f', 'g', 'h']];
ok($dbh->do(q{INSERT INTO test (id, name, array) VALUES (?, ?, ?)}, {}, 1, 'array1', $values),
	'insert statement with references'
  );

my $sql = <<SQL;
   SELECT array[1][1],array[1][2],array[1][3],
   array[2][1],array[2][2],array[2][3]
   FROM test
   WHERE id = ?
   AND name = ?
SQL
my $sth = $dbh->prepare($sql);
ok(defined $sth,
   "prepare: $sql"
  );

ok($sth->bind_param(1, '1'),
   'bind parameter 1',
  );
ok($sth->bind_param(2, 'array1'),
   'bind parameter 2'
   );

ok($sth->execute,
	'execute statement with references'
   );

my @result = $sth->fetchrow_array;

ok(scalar(@result) == 6,
	'fetch 6 columns'
  );

ok($result[0] eq 'a,b',
	'values are equal'
  );

ok($result[1] eq 'c","d',
	'values are equal'
  );

ok($result[2] eq "e'",
    'values are equal'
  );

ok($result[3] eq 'f',
	'values are equal'
  );

ok($result[4] eq 'g',
	'values are equal'
  );

ok($result[5] eq 'h',
	'values are equal'
  );

ok($sth->finish(),
   'finish'
   );

ok($dbh->disconnect(),
   'disconnect'
  );
