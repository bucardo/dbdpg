if (!exists($ENV{DBDPG_MAINTAINER})) {
    print "1..0\n";
    exit;
}

use strict;
use DBI;

main();

sub main {
    my ($n, $dbh);
    
    print "1..3\n";
    
    $n = 1;
    
    $dbh = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 1});
    
    print "ok $n\n"; $n++;
    
    {
        local $dbh->{PrintError} = 0;
        local $dbh->{RaiseError} = 0;        
        $dbh->do(q{DROP TABLE test});
    }

    $dbh->do(q{CREATE TABLE test (id int, name text, val text, score float, date timestamp default 'now()')});
    
    print "ok $n\n"; $n++;
    
    $dbh->disconnect();
    
    print "ok $n\n"; $n++;
}

1;
    
