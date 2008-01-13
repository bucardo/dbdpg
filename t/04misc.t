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

if (defined $ENV{DBI_DSN}) {
	plan tests => 6;
} else {
	plan skip_all => 'Cannot run test unless DBI_DSN is defined. See the README file';
}

my $dbh = connect_database();
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

cleanup_database($dbh,'test');
$dbh->disconnect();
