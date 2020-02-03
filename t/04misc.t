#!perl

## Various stuff that does not go elsewhere

use 5.008001;
use strict;
use warnings;
use Test::More;
use Data::Dumper;
use DBI;
use DBD::Pg qw/:pg_types :pg_limits/;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 95;

isnt ($dbh, undef, 'Connect to database for miscellaneous tests');

my $t = q{Method 'server_trace_flag' is available without a database handle};
my $num;
eval {
    $num = DBD::Pg->parse_trace_flag('NONE');
};
is ($@, q{}, $t);

$t = 'Constant PG_MIN_SMALLINT returns expected value of -32768';
my $sth = $dbh->prepare('SELECT ?::smallint');
$sth->execute(PG_MIN_SMALLINT);
is ( $sth->fetch->[0], -32768, $t);

$t = 'Trying to fit one less than PG_MIN_SMALLINT into a smallint returns expected error';
eval { $sth->execute(PG_MIN_SMALLINT-1) };
is ( $dbh->state, '22003', $t);
$dbh->rollback();

$t = 'Constant PG_MAX_SMALLINT returns expected value of 32767';
$sth->execute(PG_MAX_SMALLINT);
is ( $sth->fetch->[0], 32767, $t);

$t = 'Trying to fit one more than PG_MAX_SMALLINT into a smallint returns expected error';
eval { $sth->execute(PG_MAX_SMALLINT+1) };
is ( $dbh->state, '22003', $t);
$dbh->rollback();

$t = 'Constant PG_MIN_INTEGER returns expected value of -2147483648';
$sth = $dbh->prepare('SELECT ?::integer');
$sth->execute(PG_MIN_INTEGER);
is ( $sth->fetch->[0], -2147483648, $t);

$t = 'Trying to fit one less than PG_MIN_INTEGER into an int returns expected error';
eval { $sth->execute(PG_MIN_INTEGER-1) };
is ( $dbh->state, '22003', $t);
$dbh->rollback();

$t = 'Constant PG_MAX_INTEGER returns expected value of 2147483647';
$sth->execute(PG_MAX_INTEGER);
is ( $sth->fetch->[0], 2147483647, $t);

$t = 'Trying to fit one more than PG_MAX_INTEGER into an int returns expected error';
eval { $sth->execute(PG_MAX_INTEGER+1) };
is ( $dbh->state, '22003', $t);
$dbh->rollback();

$t = 'Constant PG_MIN_BIGINT returns expected value of -9223372036854775808';
$sth = $dbh->prepare('SELECT ?::bigint');
$sth->execute(PG_MIN_BIGINT);
is ( $sth->fetch->[0], '-9223372036854775808', $t);

$t = 'Trying to fit one less than PG_MIN_BIGINT into a bigint returns expected error';
## Unlike the others, we cannot modify Perl side in case of a 32-bit system
$sth = $dbh->prepare('SELECT ?::bigint-1');
eval { $sth->execute(PG_MIN_BIGINT) };
is ( $dbh->state, '22003', $t);
$dbh->rollback();

$t = 'Constant PG_MAX_BIGINT returns expected value of 9223372036854775807';
$sth = $dbh->prepare('SELECT ?::bigint');
$sth->execute(PG_MAX_BIGINT);
is ( $sth->fetch->[0], '9223372036854775807', $t);

$t = 'Trying to fit one more than PG_MAX_BIGINT into a bigint returns expected error';
$sth = $dbh->prepare('SELECT ?::bigint+1');
eval { $sth->execute(PG_MAX_BIGINT) };
is ( $dbh->state, '22003', $t);
$dbh->rollback();

$t = 'Constant PG_MIN_SMALLSERIAL is set to 1';
is (PG_MIN_SMALLSERIAL, 1, $t);

$t = 'Constant PG_MAX_SMALLSERIAL returns expected value of 32767 (same as PG_MAX_SMALLINT)';
$sth = $dbh->prepare('SELECT ?::bigint');
$sth->execute(PG_MAX_SMALLSERIAL);
is ( $sth->fetch->[0], 32767, $t);

$t = 'Constant PG_MIN_SERIAL is set to 1';
is (PG_MIN_SERIAL, 1, $t);

$t = 'Constant PG_MAX_SERIAL returns expected value of 2147483647 (same as PG_MAX_INTEGER)';
$sth->execute(PG_MAX_SERIAL);
is ( $sth->fetch->[0], 2147483647, $t);

$t = 'Constant PG_MIN_BIGSERIAL is set to 1';
is (PG_MIN_BIGSERIAL, 1, $t);

$t = 'Constant PG_MIN_BIGINT returns expected value of 9223372036854775807 (same as PG_MAX_BIGINT)';
$sth->execute(PG_MAX_BIGSERIAL);
is ( $sth->fetch->[0], '9223372036854775807', $t);

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
$sth = $dbh->prepare('SELECT 1');
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

    my $SQL = q{
CREATE OR REPLACE FUNCTION dbdpg_test_error_handler(TEXT)
RETURNS boolean
LANGUAGE plpgsql
AS $BC$
 DECLARE
   level ALIAS FOR $1;
 BEGIN 
  IF level ~* 'notice' THEN
    RAISE NOTICE 'RAISE NOTICE FROM dbdpg_test_error_handler';
  ELSIF level ~* 'warning' THEN
    RAISE WARNING 'RAISE WARNING FROM dbdpg_test_error_handler';
  ELSIF level ~* 'exception' THEN
    RAISE EXCEPTION 'RAISE EXCEPTION FROM dbdpg_test_error_handler';
  END IF;
  RETURN TRUE;
 END;
$BC$
};

    eval {
        $dbh->do($SQL);
        $dbh->commit();
    };
    if ($@) {
        $dbh->rollback();
        $@ and skip ('Cannot load function  for testing', 6);
    }

    $sth = $dbh->prepare('SELECT * FROM dbdpg_test_error_handler( ? )');

    is( $sth->err, undef, q{Statement attribute 'err' is initially undef});

    $dbh->do(q{SET client_min_messages = 'ERROR'});

  TODO: {
        local $TODO = q{Known bug: notice and warnings should set err to 6};

        for my $level (qw/notice warning/) {
            $sth->execute($level);
            is( $sth->err, 6, qq{Statement attribute 'err' set to 6 for level $level});
        }
    }

    for my $level (qw/exception/) {
        eval { $sth->execute($level);};
        is( $sth->err, 7, qq{Statement attribute 'err' set to 7 for level $level});
        $dbh->rollback;
    }

    for my $level (qw/normal/) {
        $sth->execute($level);
        is( $sth->err, undef, q{Statement attribute 'err' set to undef when no notices raised});
    }

    $sth->finish;

    is( $sth->err, undef, q{Statement attribute 'err' set to undef after statement finishes});

    $dbh->do('DROP FUNCTION dbdpg_test_error_handler(TEXT)') or die $dbh->errstr;
    $dbh->do('SET client_min_messages = NOTICE');
    $dbh->commit();

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
    $expected = q{PQclear
PQexec
PQresultStatus
PQresultErrorField
PQclear
PQexec
PQresultStatus
PQresultErrorField
PQntuples
PQtransactionStatus
PQtransactionStatus
PQclear
PQexec
PQresultStatus
PQresultErrorField
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
is (scalar @result, 0, $t);

$t='The "data_sources" method returns information when fed a valid port as the second arg';
my $port = $dbh->{pg_port};
@result = DBI->data_sources('Pg',"port=$port");
isnt ($result[0], undef, $t);

SKIP: {

    $t=q{The "data_sources" method returns information when 'dbi:Pg' is uppercased};

    if (! exists $ENV{DBI_DSN} or $ENV{DBI_DSN} !~ /pg/i) {
        skip 'Cannot test data_sources() DBI_DSN munging unless DBI_DSN is set', 2;
    }

    my $orig = $ENV{DBI_DSN};
    $ENV{DBI_DSN} =~ s/DBI:PG/DBI:PG/i;
    @result = DBI->data_sources('Pg');
    like ((join '' => @result), qr{template0}, $t);

    $t=q{The "data_sources" method returns information when 'DBI:' is mixed case};

    $ENV{DBI_DSN} =~ s/DBI:PG/dBi:pg/i;
    @result = DBI->data_sources('Pg');
    like ((join '' => @result), qr{template0}, $t);

    $ENV{DBI_DSN} = $orig;

}

#
# Test the use of $DBDPG_DEFAULT
#

## Do NOT use the variable at all before the call - even in a string (test for RT #112309)
$t=q{Using $DBDPG_DEFAULT works};
$sth = $dbh->prepare(q{INSERT INTO dbd_pg_test (id, pname) VALUES (?,?)});
eval {
    $sth->execute(600,$DBDPG_DEFAULT);
};
is ($@, q{}, $t);
$sth->execute(602,123);

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

## Check for problems in pg_st_split_statement by having it parse long strings
my $problem;
for my $length (0..16384) {
    my $sql = sprintf 'SELECT %*d', $length + 3, $length;
    my $cur_len = $dbh->selectrow_array($sql);
    next if $cur_len == $length;
    $problem = "length $length gave us a select of $cur_len";
    last;
}

if (defined $problem) {
    fail ("pg_st_split_statment failed: $problem");
}
else {
    pass ('pg_st_split_statement gave no problems with various lengths');
}

# PostgreSQL 8.1 fails with "ERROR:  stack depth limit exceeded"
# with the default value of 2048
$dbh->do('set max_stack_depth = 4096');
## Check for problems with insane number of placeholders
for my $ph (1..13) {
    my $total = 2**$ph;
    $t = "prepare/execute works with $total placeholders";
    my $sql = 'SELECT count(*) FROM pg_class WHERE relpages IN (' . ('?,' x $total);
    $sql =~ s/.$/\)/;
    $sth = $dbh->prepare($sql);
    my @arr = (1..$total);
    my $count = $sth->execute(@arr);
    is $count, 1, $t;
    $sth->finish();
}

## Make sure our mapping of char/SQL_CHAR/bpchar is working as expected
$dbh->do('CREATE TEMP TABLE tt (c_test int, char4 char(4))');

$sth = $dbh->prepare ('SELECT * FROM tt');
$sth->execute;
my @stt = @{$sth->{TYPE}};

$sth = $dbh->prepare('INSERT INTO tt VALUES (?,?)');

$sth->bind_param(1, undef, $stt[0]); ## 4
$sth->bind_param(2, undef, $stt[1]); ## 1 aka SQL_CHAR
$sth->execute(2, '0301');

my $SQL = 'SELECT char4 FROM tt';
my $result = $dbh->selectall_arrayref($SQL)->[0][0];

$t = q{Using bind_param with type 1 yields a correct bpchar value};
is( $result, '0301', $t);

cleanup_database($dbh,'test');
$dbh->disconnect();

