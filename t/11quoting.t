use strict;
use DBI;
use Test::More;

if (defined $ENV{DBI_DSN}) {
  plan tests => 9;
} else {
  plan skip_all => 'cannot test without DB info';
}

my $dbh = DBI->connect($ENV{DBI_DSN}, $ENV{DBI_USER}, $ENV{DBI_PASS},
		       {RaiseError => 1, AutoCommit => 0}
		      );
ok(defined $dbh,
   'connect with transaction'
  );

my %tests = (
	     one=>["'", "''''"],,
	     two=>["''", "''''''"],
	     three=>["\\", q{'\\\\'}],
	     four=> ["\\'",q{'\\\\'''}],
	     five=> ["\\'?:", q{'\\\\''?:'}],
	     six=> [undef, "NULL"],
	    );

foreach my $test (keys %tests) {
  my ($unq, $quo, $ref);

  $unq = $tests{$test}->[0];
  $ref = $tests{$test}->[1];
  $quo = $dbh->quote($unq);

  # If the test fails, Test::More will print out what was compared to what.
  is($quo, $ref, "Compare quote $test");
}

# Make sure that SQL_BINARY doesn't work.

{
     local $SIG{__WARN__} =
         sub { ok($_[0] =~ /^Use of SQL_BINARY/,
               'warning with SQL_BINARY');
         };

  local $dbh->{PrintError} = 0;
  $dbh->quote('foo', DBI::SQL_BINARY);
};

ok($dbh->disconnect(),
   'disconnect'
  );
