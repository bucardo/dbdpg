if (!exists($ENV{DBDPG_MAINTAINER})) {
    print "1..0\n";
    exit;
}

use strict;
use DBI;

main();

sub main {
    my ($n, $dbh, $sth, @types);
    
    print "1..2\n";
    
    $n = 1;
    
    $dbh = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 0});    
    eval {
        local $dbh->{PrintError} = 0;
        $dbh->do(q{DROP TABLE tt});
        $dbh->commit();
    };
    $dbh->rollback();
        
    $dbh->do(q{CREATE TABLE tt (blah numeric(5,2), foo text)});
    $sth = $dbh->prepare(qq{
        SELECT * FROM tt WHERE FALSE
    });
    $sth->execute();

    @types = @{$sth->{pg_type}};

    if ($types[0] ne 'numeric') {
        print "not ";
    }
    
    print "ok $n\n"; $n++;
    
    if ($types[1] ne 'text') {
        print "not ";
    }
    
    print "ok $n\n"; $n++;

    $sth->finish();
    $dbh->rollback();
    $dbh->disconnect();
}

1;
