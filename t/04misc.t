#!perl

## Various stuff that does not go elsewhere

use strict;
use warnings;
use Test::More;
use DBI;
use DBD::Pg;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (defined $dbh) {
	plan tests => 13;
}
else {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}

ok( defined $dbh, 'Connect to database for miscellaneous tests');

#
# Test of the "data_sources" method
#

my @result;
eval {
	@result = DBI->data_sources('Pg');
};
is($@, q{}, 'The data_sources() method did not throw an exception');

is (grep (/^dbi:Pg:dbname=template1$/, @result), '1', 'The data_sources() method returns a template1 listing');

my $t=q{The data_sources() returns undef when fed a bogus second argument};
@result = DBI->data_sources('Pg','foobar');
is_deeply(@result, undef, $t);

my $port = $dbh->{pg_port};

$t=q{The data_sources() returns information when fed a valid port as the second arg};
@result = DBI->data_sources('Pg',"port=$port");
ok(defined $result[0], $t);


#
# Test the use of $DBDPG_DEFAULT
#

my $sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id, pname) VALUES (?,?)});
eval {
$sth->execute(600,$DBDPG_DEFAULT);
};
$sth->execute(602,123);
ok (!$@, qq{Using \$DBDPG_DEFAULT ($DBDPG_DEFAULT) works});

#
# Test transaction status changes
#

$dbh->{AutoCommit} = 1;
$dbh->begin_work();
$dbh->do('SELECT 123');

$t = q{Raw ROLLBACK via do() resets the transaction status correctly};
eval { $dbh->do('ROLLBACK'); };
is($@, q{}, $t);
eval { $dbh->begin_work(); };
is($@, q{}, $t);

$t = q{Using dbh->commit() resets the transaction status correctly};
eval { $dbh->commit(); };
is($@, q{}, $t);
eval { $dbh->begin_work(); };
is($@, q{}, $t);

$t = q{Raw COMMIT via do() resets the transaction status correctly};
eval { $dbh->do('COMMIT'); };
is($@, q{}, $t);
eval { $dbh->begin_work(); };
is($@, q{}, $t);

$t = q{Calling COMMIT via prepare/execute resets the transaction status correctly};
$sth = $dbh->prepare('COMMIT');
$sth->execute();
eval { $dbh->begin_work(); };
is($@, q{}, $t);

cleanup_database($dbh,'test');
$dbh->disconnect();
