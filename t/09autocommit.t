if (!exists($ENV{DBDPG_MAINTAINER})) {
    print "1..0\n";
    exit;
}
use strict;
use DBI;

main();

sub main {
    my ($n, $dbh1, $dbh2, $rows, $str);
    
    print "1..10\n";
    
    $n = 1;
    
    $dbh1 = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 1});
    $dbh2 = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 1});

    print "ok $n\n"; $n++;

    $dbh1->do(q{DELETE FROM test});
    
    print "ok $n\n"; $n++;
    
    $rows = ($dbh1->selectrow_array(q{SELECT COUNT(*) FROM test}))[0];
    if ($rows != 0) {
        print "not ";
    }

    print "ok $n\n"; $n++;

    $rows = ($dbh2->selectrow_array(q{SELECT COUNT(*) FROM test}))[0];
    if ($rows != 0) {
        print "not ";
    }

    print "ok $n\n"; $n++;

    $dbh1->do(q{INSERT INTO test (id, name, val) VALUES (1, 'foo', 'horse')});
    
    print "ok $n\n"; $n++;
    
    $rows = ($dbh1->selectrow_array(q{SELECT COUNT(*) FROM test}))[0];
    if ($rows != 1) {
        print "not ";
    }

    print "ok $n\n"; $n++;

    $rows = ($dbh2->selectrow_array(q{SELECT COUNT(*) FROM test}))[0];
    if ($rows != 1) {
        print "not ";
    }

    print "ok $n\n"; $n++;

    $SIG{__WARN__} = sub {};
    $dbh1->commit();

    print "ok $n\n"; $n++;

    $dbh1->rollback();

    print "ok $n\n"; $n++;

    delete $SIG{__WARN__};

    $dbh1->disconnect();
    $dbh2->disconnect();

    print "ok $n\n"; $n++;
    
}

1;
