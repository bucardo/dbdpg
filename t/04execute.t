if (!exists($ENV{DBDPG_MAINTAINER})) {
    print "1..0\n";
    exit;
}

use strict;
use DBI;

main();

sub main {
    my ($n, $dbh, $sth);
    
    print "1..10\n";
    
    $n = 1;
    
    $dbh = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 0});
    
    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE id = ?
    });
    $sth->bind_param(1, 1);
    $sth->execute();
    
    print "ok $n\n"; $n++;

    $sth->bind_param(1, 2);
    $sth->execute();
    
    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE id = ?
           AND name = ?
    });
    $sth->bind_param(1, 2);
    $sth->bind_param(2, 'foo');
    $sth->execute();
    
    print "ok $n\n"; $n++;

    eval {
        local $dbh->{PrintError} = 0;
        
        $sth = $dbh->prepare(q{
            SELECT id
                 , name
              FROM test
             WHERE id = ?
               AND name = ?
        });
        $sth->bind_param(1, 2);
        $sth->execute();
    };
    if ($@) {
        print "ok $n\n"; $n++;
    } else {
        print "not ok $n\n"; $n++;
    }

    eval {
        local $dbh->{PrintError} = 0;
        
        $sth = $dbh->prepare(q{
            SELECT id
                 , name
              FROM test
             WHERE id = ?
               AND name = ?
        });
        $sth->bind_param(2, 'foo');
        $sth->execute();
    };
    if ($@) {
        print "ok $n\n"; $n++;
    } else {
        print "not ok $n\n"; $n++;
    }

    eval {
        local $dbh->{PrintError} = 0;
        
        $sth = $dbh->prepare(q{
            SELECT id
                 , name
              FROM test
             WHERE id = ?
               AND name = ?
        });
        $sth->execute();
    };
    if ($@) {
        print "ok $n\n"; $n++;
    } else {
        print "not ok $n\n"; $n++;
    }

    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE id = ?
           AND name = ?
    });
    $sth->execute(1, 'foo');

    eval {
        local $dbh->{PrintError} = 0;
        
        $sth = $dbh->prepare(q{
            SELECT id
                 , name
              FROM test
             WHERE id = ?
               AND name = ?
        });
        $sth->execute(1);
    };
    if ($@) {
        print "ok $n\n"; $n++;
    } else {
        print "not ok $n\n"; $n++;
    }

    $sth->finish();

    print "ok $n\n"; $n++;

    $dbh->disconnect();
    
    print "ok $n\n"; $n++;
}

1;    
