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

if (! defined $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 36;

isnt( $dbh, undef, 'Connect to database for miscellaneous tests');

my $t = q{Method 'server_trace_flag' is available without a database handle};
my $num;
eval {
	$num = DBD::Pg->parse_trace_flag('NONE');
};
is( $@, q{}, $t);

$t = q{Method 'server_trace_flag' returns undef on bogus argument};
is( $num, undef, $t);

$t = q{Method 'server_trace_flag' returns 0x00000100 for DBI value 'SQL'};
$num = DBD::Pg->parse_trace_flag('SQL');
is( $num, 0x00000100, $t);

$t = q{Method 'server_trace_flag' returns 0x01000000 for DBD::Pg flag 'PGLIBPQ'};
$num = DBD::Pg->parse_trace_flag('PGLIBPQ');
is( $num, 0x01000000, $t);

$t = q{Database handle method 'server_trace_flag' returns undef on bogus argument};
$num = $dbh->parse_trace_flag('NONE');
is( $num, undef, $t);

$t = q{Database handle method 'server_trace_flag' returns 0x00000100 for DBI value 'SQL'};
$num = $dbh->parse_trace_flag('SQL');
is( $num, 0x00000100, $t);

$t = q{Database handle method 'server_trace_flags' returns 0x01000100 for 'SQL|PGLIBPQ'};
$num = $dbh->parse_trace_flags('SQL|PGLIBPQ');
is( $num, 0x01000100, $t);

$t = q{Database handle method 'server_trace_flags' returns 0x03000100 for 'SQL|PGLIBPQ|PGBEGIN'};
$num = $dbh->parse_trace_flags('SQL|PGLIBPQ|PGBEGIN');
is( $num, 0x03000100, $t);

my $flagexp = 24;
for my $flag (qw/PGLIBPQ PGBEGIN PGEND PGPREFIX PGLOGIN PGQUOTE/) {
	my $hex = 2**$flagexp++;
	$t = qq{Database handle method 'server_trace_flags' returns $hex for flag $flag};
	$num = $dbh->parse_trace_flags($flag);
	is( $num, $hex, $t);
}

SKIP: {

	eval {
		require File::Temp;
	};
	$@ and skip q{Must have File::Temp to complete trace flag testing}, 9;

	my ($fh,$filename) = File::Temp::tempfile('dbdpg_test_XXXXXX', SUFFIX => 'tst', UNLINK => 1);
	my ($flag, $info, $expected, $SQL);

	$t=q{Trace flag 'SQL' works as expected};
	$flag = $dbh->parse_trace_flags('SQL');
	$dbh->trace($flag, $filename);
	$SQL = q{SELECT 'dbdpg_flag_testing'};
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; $info = <$fh>; }
	$expected = qq{begin;\n\n$SQL;\n\ncommit;\n\n};
	is($info, $expected, $t);

	$t=q{Trace flag 'PGLIBPQ' works as expected};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	$dbh->trace($dbh->parse_trace_flag('PGLIBPQ'), $filename);
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; $info = <$fh>; }
	$expected = q{PQexec
PQresultStatus
PQresultErrorField
PQclear
PQexec
PQresultStatus
PQresultErrorField
PQntuples
PQclear
PQtransactionStatus
PQtransactionStatus
PQexec
PQresultStatus
PQresultErrorField
PQclear
};
	is($info, $expected, $t);

	$t=q{Trace flag 'PGBEGIN' works as expected};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	$dbh->trace($dbh->parse_trace_flags('PGBEGIN'), $filename);
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; $info = <$fh>; }
	$expected = q{Begin pg_quickexec (query: SELECT 'dbdpg_flag_testing' async: 0 async_status: 0)
Begin _result (sql: begin)
Begin _sqlstate
Begin _sqlstate
Begin dbd_db_commit
Begin pg_db_rollback_commit (action: commit AutoCommit: 0 BegunWork: 0)
Begin PGTransactionStatusType
Begin _result (sql: commit)
Begin _sqlstate
};
	is($info, $expected, $t);

	$t=q{Trace flag 'PGPREFIX' works as expected};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	$dbh->trace($dbh->parse_trace_flags('PGBEGIN|PGPREFIX'), $filename);
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; $info = <$fh>; }
	$expected = q{dbdpg: Begin pg_quickexec (query: SELECT 'dbdpg_flag_testing' async: 0 async_status: 0)
dbdpg: Begin _result (sql: begin)
dbdpg: Begin _sqlstate
dbdpg: Begin _sqlstate
dbdpg: Begin dbd_db_commit
dbdpg: Begin pg_db_rollback_commit (action: commit AutoCommit: 0 BegunWork: 0)
dbdpg: Begin PGTransactionStatusType
dbdpg: Begin _result (sql: commit)
dbdpg: Begin _sqlstate
};
	is($info, $expected, $t);

	$t=q{Trace flag 'PGEND' works as expected};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	$dbh->trace($dbh->parse_trace_flags('PGEND'), $filename);
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; $info = <$fh>; }
	$expected = q{End _sqlstate (imp_dbh->sqlstate: 00000)
End _sqlstate (status: 1)
End _result
End _sqlstate (imp_dbh->sqlstate: 00000)
End _sqlstate (status: 2)
End pg_quickexec (rows: 1, txn_status: 2)
End _sqlstate (imp_dbh->sqlstate: 00000)
End _sqlstate (status: 1)
End _result
End pg_db_rollback_commit (result: 1)
};
	is($info, $expected, $t);

	$t=q{Trace flag 'PGLOGIN' returns undef if no activity};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	$dbh->trace($dbh->parse_trace_flags('PGLOGIN'), $filename);
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; $info = <$fh>; }
	$expected = undef;
	is($info, $expected, $t);

	$t=q{Trace flag 'PGLOGIN' works as expected with DBD::Pg->parse_trace_flag()};
	$dbh->disconnect();
	my $flagval = DBD::Pg->parse_trace_flag('PGLOGIN');
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	DBI->trace($flagval, $filename);
	$dbh = connect_database({nosetup => 1});
	$dbh->do($SQL);
	$dbh->disconnect();
	$dbh = connect_database({nosetup => 1});
	$dbh->disconnect();
	DBI->trace(0);
	seek $fh,0,0;
	{ local $/; $info = <$fh>; }
	$expected = q{Login connection string: 
Connection complete
Disconnection complete
};
	$info =~ s/(Login connection string: ).+/$1/g;
	is($info, "$expected$expected", $t);

	$t=q{Trace flag 'PGLOGIN' works as expected with DBD::Pg->parse_trace_flags()};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	DBI->trace($flagval, $filename);
	$dbh = connect_database({nosetup => 1});
	$dbh->disconnect();
	DBI->trace(0);
	seek $fh,0,0;
	{ local $/; $info = <$fh>; }
	$expected = q{Login connection string: 
Connection complete
Disconnection complete
};
	$info =~ s/(Login connection string: ).+/$1/g;
	is($info, "$expected", $t);

	$t=q{Trace flag 'PGPREFIX' and 'PGBEGIN' append to 'PGLOGIN' work as expected};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	DBI->trace($flagval, $filename);
	$dbh = connect_database({nosetup => 1});
	$dbh->do($SQL);
	$flagval += $dbh->parse_trace_flags('PGPREFIX|PGBEGIN');
	$dbh->trace($flagval);
	$dbh->do($SQL);
	$dbh->trace(0);
	$dbh->rollback();
	seek $fh,0,0;
	{ local $/; $info = <$fh>; }
	$expected = q{Login connection string: 
Connection complete
dbdpg: Begin pg_quickexec (query: SELECT 'dbdpg_flag_testing' async: 0 async_status: 0)
dbdpg: Begin _sqlstate
};
	$info =~ s/(Login connection string: ).+/$1/g;
	is($info, "$expected", $t);

} ## end trace flag testing using File::Temp

#
# Test of the "data_sources" method
#

my @result;
eval {
	@result = DBI->data_sources('Pg');
};
is( $@, q{}, 'The data_sources() method did not throw an exception');

is( grep (/^dbi:Pg:dbname=template1$/, @result), '1', 'The data_sources() method returns a template1 listing');

$t=q{The data_sources() returns undef when fed a bogus second argument};
@result = DBI->data_sources('Pg','foobar');
is_deeply( @result, undef, $t);

my $port = $dbh->{pg_port};

$t=q{The data_sources() returns information when fed a valid port as the second arg};
@result = DBI->data_sources('Pg',"port=$port");
isnt( $result[0], undef, $t);

#
# Test the use of $DBDPG_DEFAULT
#

my $sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id, pname) VALUES (?,?)});
eval {
$sth->execute(600,$DBDPG_DEFAULT);
};
$sth->execute(602,123);
is( $@, q{}, qq{Using \$DBDPG_DEFAULT ($DBDPG_DEFAULT) works});

#
# Test transaction status changes
#

$dbh->{AutoCommit} = 1;
$dbh->begin_work();
$dbh->do('SELECT 123');

$t = q{Raw ROLLBACK via do() resets the transaction status correctly};
eval { $dbh->do('ROLLBACK'); };
is( $@, q{}, $t);
eval { $dbh->begin_work(); };
is( $@, q{}, $t);

$t = q{Using dbh->commit() resets the transaction status correctly};
eval { $dbh->commit(); };
is( $@, q{}, $t);
eval { $dbh->begin_work(); };
is( $@, q{}, $t);

$t = q{Raw COMMIT via do() resets the transaction status correctly};
eval { $dbh->do('COMMIT'); };
is( $@, q{}, $t);
eval { $dbh->begin_work(); };
is( $@, q{}, $t);

$t = q{Calling COMMIT via prepare/execute resets the transaction status correctly};
$sth = $dbh->prepare('COMMIT');
$sth->execute();
eval { $dbh->begin_work(); };
is( $@, q{}, $t);

cleanup_database($dbh,'test');
$dbh->disconnect();
