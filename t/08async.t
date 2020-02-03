#!perl

## Test asynchronous queries

use 5.008001;
use strict;
use warnings;
use Test::More;
use Time::HiRes qw/sleep/;
use DBD::Pg ':async';
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}

plan tests => 67;

isnt ($dbh, undef, 'Connect to database for async testing');

my ($t,$sth,$res);
my $pgversion = $dbh->{pg_server_version};

## First, test out do() in all its variants

$t=q{Method do() works as expected with no args };
eval {
    $res = $dbh->do('SELECT 123');
};
is ($@, q{}, $t);
is ($res, 1, $t);

$t=q{Method do() works as expected with an unused attribute };
eval {
    $res = $dbh->do('SELECT 123', {pg_nosuch => 'arg'});
};
is ($@, q{}, $t);
is ($res, 1, $t);

$t=q{Method do() works as expected with an unused attribute and a non-prepared param };
eval {
    $res = $dbh->do('SET random_page_cost TO ?', undef, '2.2');
};
is ($@, q{}, $t);
is ($res, '0E0', $t);

$t=q{Method do() works as expected with an unused attribute and multiple real bind params };
eval {
    $res = $dbh->do('SELECT count(*) FROM pg_class WHERE reltuples IN (?,?,?)', undef, 1,2,3);
};
is ($@, q{}, $t);
is ($res, 1, $t);

$t=q{Cancelling a non-async do() query gives an error };
eval {
    $res = $dbh->pg_cancel();
};
like ($@, qr{No asynchronous query is running}, $t);

$t=q{Method do() works as expected with an asychronous flag };
eval {
    $res = $dbh->do('SELECT 123', {pg_async => PG_ASYNC});
};
is ($@, q{}, $t);
is ($res, '0E0', $t);

$t=q{Database attribute "async_status" returns 1 after async query};
$res = $dbh->{pg_async_status};
is ($res, +1, $t);

sleep 1;
$t=q{Cancelling an async do() query works };
eval {
    $res = $dbh->pg_cancel();
};
is ($@, q{}, $t);

$t=q{Database method pg_cancel returns a false value when cancellation works but finished};
is ($res, q{}, $t);

$t=q{Database attribute "async_status" returns -1 after pg_cancel};
$res = $dbh->{pg_async_status};
is ($res, -1, $t);

$t=q{Running do() after a cancelled query works};
eval {
    $res = $dbh->do('SELECT 123');
};
is ($@, q{}, $t);

$t=q{Database attribute "async_status" returns 0 after normal query run};
$res = $dbh->{pg_async_status};
is ($res, 0, $t);

$t=q{Method pg_ready() fails after a non-async query};
eval {
    $dbh->pg_ready();
};
like ($@, qr{No async}, $t);

$res = $dbh->do('SELECT 123', {pg_async => PG_ASYNC});
$t=q{Method pg_ready() works after a non-async query};
## Sleep a sub-second to make sure the server has caught up
sleep 0.2;
eval {
    $res = $dbh->pg_ready();
};
is ($@, q{}, $t);

$t=q{Database method pg_ready() returns 1 after a completed async do()};
is ($res, 1, $t);

$res = $dbh->pg_ready();
$t=q{Database method pg_ready() returns true when called a second time};
is ($res, 1, $t);

$t=q{Database method pg_ready() returns 1 after a completed async do()};
is ($res, 1, $t);
$t=q{Cancelling an async do() query works };
eval {
    $res = $dbh->pg_cancel();
};
is ($@, q{}, $t);
$t=q{Database method pg_cancel() returns expected false value for completed value};
is ($res, q{}, $t);

$t=q{Method do() runs after pg_cancel has cleared the async query};
eval {
    $dbh->do('SELECT 456');
};
is ($@, q{}, $t);

$dbh->do(q{SELECT 'async2'}, {pg_async => PG_ASYNC});

$t=q{Method do() fails when async query has not been cleared};
eval {
    $dbh->do(q{SELECT 'async_blocks'});
};
like ($@, qr{previous async}, $t);

$t=q{Database method pg_result works as expected};
eval {
    $res = $dbh->pg_result();
};
is ($@, q{}, $t);

$t=q{Database method pg_result() returns correct value};
is ($res, 1, $t);

$t=q{Database method pg_result() fails when called twice};
eval {
    $dbh->pg_result();
};
like ($@, qr{No async}, $t);

$t=q{Database method pg_cancel() fails when called after pg_result()};
eval {
    $dbh->pg_cancel();
};
like ($@, qr{No async}, $t);

$t=q{Database method pg_ready() fails when called after pg_result()};
eval {
    $dbh->pg_ready();
};
like ($@, qr{No async}, $t);

$t=q{Database method do() works after pg_result()};
eval {
    $dbh->do('SELECT 123');
};
is ($@, q{}, $t);

SKIP: {

    if ($pgversion < 80200) {
        skip ('Need pg_sleep() to perform rest of async tests: your Postgres is too old', 14);
    }

    eval {
        $dbh->do('SELECT pg_sleep(0)');
    };
    is ($@, q{}, 'Calling pg_sleep works as expected');

    my $time = time();
    eval {
        $res = $dbh->do('SELECT pg_sleep(2)', {pg_async => PG_ASYNC});
    };
    $time = time()-$time;
    $t = q{Database method do() returns right away when in async mode};
    cmp_ok ($time, '<=', 1, $t);

    $t=q{Method pg_ready() returns false when query is still running};
    $res = $dbh->pg_ready();
    is ($res, 0, $t);

    pass ('Sleeping to allow query to finish');
    sleep(3);
    $t=q{Method pg_ready() returns true when query is finished};
    $res = $dbh->pg_ready();
    ok ($res, $t);

    $t=q{Method do() will not work if async query not yet cleared};
    eval {
        $dbh->do('SELECT pg_sleep(2)', {pg_async => PG_ASYNC});
    };
    like ($@, qr{previous async}, $t);

    $t=q{Database method pg_cancel() works while async query is running};
    eval {
        $res = $dbh->pg_cancel();
    };
    is ($@, q{}, $t);
    $t=q{Database method pg_cancel returns false when query has already finished};
    ok (!$res, $t);

    $t=q{Database method pg_result() fails after async query has been cancelled};
    eval {
        $res = $dbh->pg_result();
    };
    like ($@, qr{No async}, $t);

    $t=q{Database method do() cancels the previous async when requested};
    eval {
        $res = $dbh->do('SELECT pg_sleep(2)', {pg_async => PG_ASYNC + PG_OLDQUERY_CANCEL});
    };
    is ($@, q{}, $t);

    $t=q{Database method pg_result works when async query is still running};
    eval {
        $res = $dbh->pg_result();
    };
    is ($@, q{}, $t);

    ## Now throw in some execute after the do()
    $sth = $dbh->prepare('SELECT 567');

    $t = q{Running execute after async do() gives an error};
    $dbh->do('SELECT pg_sleep(2)', {pg_async => PG_ASYNC});
    eval {
        $res = $sth->execute();
    };
    like ($@, qr{previous async}, $t);

    $t = q{Running execute after async do() works when told to cancel};
    $sth = $dbh->prepare('SELECT 678', {pg_async => PG_OLDQUERY_CANCEL});
    eval {
        $sth->execute();
    };
    is ($@, q{}, $t);

    $t = q{Running execute after async do() works when told to wait};
    $dbh->do('SELECT pg_sleep(2)', {pg_async => PG_ASYNC});
    $sth = $dbh->prepare('SELECT 678', {pg_async => PG_OLDQUERY_WAIT});
    eval {
        $sth->execute();
    };
    is ($@, q{}, $t);

    $sth->finish();

} ## end of pg_sleep skip


$t=q{Method execute() works when prepare has PG_ASYNC flag};
$sth = $dbh->prepare('SELECT 123', {pg_async => PG_ASYNC});
eval {
    $sth->execute();
};
is ($@, q{}, $t);

$t=q{Database attribute "async_status" returns 1 after prepare async};
$res = $dbh->{pg_async_status};
is ($res, 1, $t);

$t=q{Method do() fails when previous async prepare has been executed};
eval {
    $dbh->do('SELECT 123');
};
like ($@, qr{previous async}, $t);

$t=q{Method execute() fails when previous async prepare has been executed};
eval {
    $sth->execute();
};
like ($@, qr{previous async}, $t);

$t=q{Database method pg_cancel works if async query has already finished};
sleep 0.5;
eval {
    $res = $sth->pg_cancel();
};
is ($@, q{}, $t);

$t=q{Statement method pg_cancel() returns a false value when cancellation works but finished};
is ($res, q{}, $t);

$t=q{Method do() fails when previous execute async has not been cleared};
$sth->execute();
$sth->finish(); ## Ideally, this would clear out the async, but it cannot at the moment
eval {
    $dbh->do('SELECT 345');
};
like ($@, qr{previous async}, $t);

$dbh->pg_cancel;

$t=q{Directly after pg_cancel(), pg_async_status is -1};
is ($dbh->{pg_async_status}, -1, $t);

$t=q{Method execute() works when prepare has PG_ASYNC flag};
$sth->execute();

$t=q{After async execute, pg_async_status is 1};
is ($dbh->{pg_async_status}, 1, $t);

$t=q{Method pg_result works after a prepare/execute call};
eval {
    $res = $dbh->pg_result;
};
is ($@, q{}, $t);

$t=q{Method pg_result() returns expected result after prepare/execute select};
is ($res, 1, $t);

$t=q{Method fetchall_arrayref works after pg_result};
eval {
    $res = $sth->fetchall_arrayref();
};
is ($@, q{}, $t);

$t=q{Method fetchall_arrayref returns correct result after pg_result};
is_deeply ($res, [[123]], $t);

$dbh->do('CREATE TABLE dbd_pg_test5(id INT, t TEXT)');
$dbh->commit();
$sth->execute();

$t=q{Method prepare() works when passed in PG_OLDQUERY_CANCEL};

my $sth2;
my $SQL = 'INSERT INTO dbd_pg_test5(id) SELECT 123 UNION SELECT 456';
eval {
    $sth2 = $dbh->prepare($SQL, {pg_async => PG_ASYNC + PG_OLDQUERY_CANCEL});
};
is ($@, q{}, $t);

$t=q{Fetch on cancelled statement handle fails};
eval {
    $sth->fetch();
};
like ($@, qr{no statement executing}, $t);

$t=q{Method execute works after async + cancel prepare};
eval {
    $sth2->execute();
};
is ($@, q{}, $t);

$t=q{Statement method pg_result works on async statement handle};
eval {
    $res = $sth2->pg_result();
};
is ($@, q{}, $t);

$t=q{Statement method pg_result returns correct result after execute};
is ($res, 2, $t);

$sth2->execute();

$t=q{Database method pg_result works on async statement handle};
eval {
    $res = $sth2->pg_result();
};
is ($@, q{}, $t);
$t=q{Database method pg_result returns correct result after execute};
is ($res, 2, $t);

$dbh->do('DROP TABLE dbd_pg_test5');

## TODO: More pg_sleep tests with execute

cleanup_database($dbh,'test');
$dbh->disconnect;

