#!perl

## Various stuff that does not go elsewhere

use 5.006;
use strict;
use warnings;
use Test::More;
use Data::Dumper;
use DBI;
use DBD::Pg;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! defined $dbh) {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 54;

isnt ($dbh, undef, 'Connect to database for miscellaneous tests');

my $t = q{Method 'server_trace_flag' is available without a database handle};
my $num;
eval {
	$num = DBD::Pg->parse_trace_flag('NONE');
};
is ($@, q{}, $t);

$t='Method "server_trace_flag" returns undef on bogus argument';
is ($num, undef, $t);

$t=q{Method "server_trace_flag" returns 0x00000100 for DBI value 'SQL'};
$num = DBD::Pg->parse_trace_flag('SQL');
is ($num, 0x00000100, $t);

$t=q{Method "server_trace_flag" returns 0x01000000 for DBD::Pg flag 'pglibpq'};
$num = DBD::Pg->parse_trace_flag('pglibpq');
is ($num, 0x01000000, $t);

$t=q{Database handle method "server_trace_flag" returns undef on bogus argument};
$num = $dbh->parse_trace_flag('NONE');
is ($num, undef, $t);

$t=q{Database handle method "server_trace_flag" returns 0x00000100 for DBI value 'SQL'};
$num = $dbh->parse_trace_flag('SQL');
is ($num, 0x00000100, $t);

$t=q{Database handle method 'server_trace_flags' returns 0x01000100 for 'SQL|pglibpq'};
$num = $dbh->parse_trace_flags('SQL|pglibpq');
is ($num, 0x01000100, $t);

$t=q{Database handle method 'server_trace_flags' returns 0x03000100 for 'SQL|pglibpq|pgstart'};
$num = $dbh->parse_trace_flags('SQL|pglibpq|pgstart');
is ($num, 0x03000100, $t);

my $flagexp = 24;
my $sth = $dbh->prepare('SELECT 1');
for my $flag (qw/pglibpq pgstart pgend pgprefix pglogin pgquote/) {

	my $hex = 2**$flagexp++;
	$t = qq{Database handle method "server_trace_flag" returns $hex for flag $flag};
	$num = $dbh->parse_trace_flag($flag);
	is ($num, $hex, $t);

	$t = qq{Database handle method 'server_trace_flags' returns $hex for flag $flag};
	$num = $dbh->parse_trace_flags($flag);
	is ($num, $hex, $t);

	$t = qq{Statement handle method "server_trace_flag" returns $hex for flag $flag};
	$num = $sth->parse_trace_flag($flag);
	is ($num, $hex, $t);

	$t = qq{Statement handle method 'server_trace_flags' returns $hex for flag $flag};
	$num = $sth->parse_trace_flag($flag);
	is ($num, $hex, $t);
}

SKIP: {

	eval {
		require File::Temp;
	};
	$@ and skip ('Must have File::Temp to complete trace flag testing', 9);

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
	{ local $/; ($info = <$fh>) =~ s/\r//go; }
	$expected = qq{begin;\n\n$SQL;\n\ncommit;\n\n};
	is ($info, $expected, $t);

	$t=q{Trace flag 'pglibpq' works as expected};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	$dbh->trace($dbh->parse_trace_flag('pglibpq'), $filename);
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; ($info = <$fh>) =~ s/\r//go; }
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
	is ($info, $expected, $t);

	$t=q{Trace flag 'pgstart' works as expected};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	$dbh->trace($dbh->parse_trace_flags('pgstart'), $filename);
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; ($info = <$fh>) =~ s/\r//go; }
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
	is ($info, $expected, $t);

	$t=q{Trace flag 'pgprefix' works as expected};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	$dbh->trace($dbh->parse_trace_flags('pgstart|pgprefix'), $filename);
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; ($info = <$fh>) =~ s/\r//go; }
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
	is ($info, $expected, $t);

	$t=q{Trace flag 'pgend' works as expected};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	$dbh->trace($dbh->parse_trace_flags('pgend'), $filename);
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; ($info = <$fh>) =~ s/\r//go; }
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
	is ($info, $expected, $t);

	$t=q{Trace flag 'pglogin' returns undef if no activity};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	$dbh->trace($dbh->parse_trace_flags('pglogin'), $filename);
	$dbh->do($SQL);
	$dbh->commit();
	$dbh->trace(0);
	seek $fh,0,0;
	{ local $/; $info = <$fh>; }
	$expected = undef;
	is ($info, $expected, $t);

	$t=q{Trace flag 'pglogin' works as expected with DBD::Pg->parse_trace_flag()};
	$dbh->disconnect();
	my $flagval = DBD::Pg->parse_trace_flag('pglogin');
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
	{ local $/; ($info = <$fh>) =~ s/\r//go; }
	$expected = q{Login connection string: 
Connection complete
Disconnection complete
};
	$info =~ s/(Login connection string: ).+/$1/g;
	is ($info, "$expected$expected", $t);

	$t=q{Trace flag 'pglogin' works as expected with DBD::Pg->parse_trace_flag()};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	DBI->trace($flagval, $filename);
	$dbh = connect_database({nosetup => 1});
	$dbh->disconnect();
	DBI->trace(0);
	seek $fh,0,0;
	{ local $/; ($info = <$fh>) =~ s/\r//go; }
	$expected = q{Login connection string: 
Connection complete
Disconnection complete
};
	$info =~ s/(Login connection string: ).+/$1/g;
	is ($info, "$expected", $t);

	$t=q{Trace flag 'pgprefix' and 'pgstart' appended to 'pglogin' work as expected};
	seek $fh, 0, 0;
	truncate $fh, tell($fh);
	DBI->trace($flagval, $filename);
	$dbh = connect_database({nosetup => 1});
	$dbh->do($SQL);
	$flagval += $dbh->parse_trace_flags('pgprefix|pgstart');
	$dbh->trace($flagval);
	$dbh->do($SQL);
	$dbh->trace(0);
	$dbh->rollback();
	seek $fh,0,0;
	{ local $/; ($info = <$fh>) =~ s/\r//go; }
	$expected = q{Login connection string: 
Connection complete
dbdpg: Begin pg_quickexec (query: SELECT 'dbdpg_flag_testing' async: 0 async_status: 0)
dbdpg: Begin _sqlstate
};
	$info =~ s/(Login connection string: ).+/$1/g;
	is ($info, "$expected", $t);

} ## end trace flag testing using File::Temp

#
# Test of the "data_sources" method
#

$t='The "data_sources" method did not throw an exception';
my @result;
eval {
	@result = DBI->data_sources('Pg');
};
is ($@, q{}, $t);

$t='The "data_sources" method returns a template1 listing';
if (! defined $result[0]) {
	fail ('The data_sources() method returned an empty list');
}
else {
	is (grep (/^dbi:Pg:dbname=template1$/, @result), '1', $t);
}

$t='The "data_sources" method returns undef when fed a bogus second argument';
@result = DBI->data_sources('Pg','foobar');
is_deeply (@result, undef, $t);

$t='The "data_sources" method returns information when fed a valid port as the second arg';
my $port = $dbh->{pg_port};
@result = DBI->data_sources('Pg',"port=$port");
isnt ($result[0], undef, $t);

#
# Test the use of $DBDPG_DEFAULT
#

$t=qq{Using \$DBDPG_DEFAULT ($DBDPG_DEFAULT) works};
$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id, pname) VALUES (?,?)});
eval {
$sth->execute(600,$DBDPG_DEFAULT);
};
$sth->execute(602,123);
is ($@, q{}, $t);

#
# Test transaction status changes
#

$t='Raw ROLLBACK via do() resets the transaction status correctly';
$dbh->{AutoCommit} = 1;
$dbh->begin_work();
$dbh->do('SELECT 123');
eval { $dbh->do('ROLLBACK'); };
is ($@, q{}, $t);
eval { $dbh->begin_work(); };
is ($@, q{}, $t);

$t='Using dbh->commit() resets the transaction status correctly';
eval { $dbh->commit(); };
is ($@, q{}, $t);
eval { $dbh->begin_work(); };
is ($@, q{}, $t);

$t='Raw COMMIT via do() resets the transaction status correctly';
eval { $dbh->do('COMMIT'); };
is ($@, q{}, $t);
eval { $dbh->begin_work(); };
is ($@, q{}, $t);

$t='Calling COMMIT via prepare/execute resets the transaction status correctly';
$sth = $dbh->prepare('COMMIT');
$sth->execute();
eval { $dbh->begin_work(); };
is ($@, q{}, $t);

cleanup_database($dbh,'test');
$dbh->disconnect();
