if (!exists($ENV{DBDPG_MAINTAINER})) {
    print "1..0\n";
    exit;
}

use strict;
use DBI;

main();

sub main {
    my ($n, $dbh, $sth);
    
    print "1..8\n";
    
    $n = 1;
    
    $dbh = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 0});
    
    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT *
          FROM test
    });
    
    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT id
          FROM test
    });
    
    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
    });
    
    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE id = 1
    });
    
    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE id = ?
    });
    
    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE id = ?
           AND name = ?
           AND value = ?
           AND score = ?
           and data = ?
    });
    
    print "ok $n\n"; $n++;

    $dbh->disconnect();
    
    print "ok $n\n"; $n++;
}

1;
