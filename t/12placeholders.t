if (!exists($ENV{DBDPG_MAINTAINER})) {
    print "1..0\n";
    exit;
}

use strict;
use DBI;

main();

sub main {
    my ($n, $dbh, $sth, $quo, $retr);
    
    print "1..9\n";
    
    $n = 1;
    
    $dbh = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 0});    
    $dbh->do(q{DELETE FROM test});
    
    print "ok $n\n"; $n++;
    
    $quo = $dbh->quote("\\'?:");
    $sth = $dbh->prepare(qq{
        INSERT INTO test (name) VALUES ($quo)
    });
    $sth->execute();

    $sth = $dbh->prepare(qq{
        SELECT name
          FROM test
         WHERE name = $quo;
    });
    $sth->execute();
    
    ($retr) = $sth->fetchrow_array();
    if (!(defined($retr) && $retr eq "\\'?:")) {
        print "not ";
    }
    
    print "ok $n\n"; $n++;
    
    eval {
        local $dbh->{PrintError} = 0;
        $sth->execute('foo');
    };
    if (!$@) {
        print "not ";
    }
    
    print "ok $n\n"; $n++;
    
    $sth = $dbh->prepare(q{
        SELECT name
          FROM test
         WHERE name = ?
    });    
    $sth->execute("\\'?:");
    
    ($retr) = $sth->fetchrow_array();
    if (!(defined($retr) && $retr eq "\\'?:")) {
        print "not ";
    }
    
    print "ok $n\n"; $n++;
    
    $sth = $dbh->prepare(q{
        SELECT name
          FROM test
         WHERE name = :1
    });    
    $sth->execute("\\'?:");
    
    ($retr) = $sth->fetchrow_array();
    if (!(defined($retr) && $retr eq "\\'?:")) {
        print "not ";
    }
    
    print "ok $n\n"; $n++;
  
    $sth = $dbh->prepare(q{
        SELECT name
          FROM test
         WHERE name = '?'
    });
    
    eval {
        local $dbh->{PrintError} = 0;
        $sth->execute('foo');
    };
    if (!$@) {
        print "not ";
    }
    
    print "ok $n\n"; $n++;
    
    $sth = $dbh->prepare(q{
        SELECT name
          FROM test
         WHERE name = ':1'
    });
    
    eval {
        local $dbh->{PrintError} = 0;
        $sth->execute('foo');
    };
    if (!$@) {
        print "not ";
    }
    
    print "ok $n\n"; $n++;
    
    $sth = $dbh->prepare(q{
        SELECT name
          FROM test
         WHERE name = '\\\\'
           AND name = '?'
    });
    
    eval {
        local $dbh->{PrintError} = 0;
        locat $sth->{PrintError} = 0;
        $sth->execute('foo');
    };
    if (!$@) {
        print "not ";
    }
    
    print "ok $n\n"; $n++;
    $sth->finish();
    $dbh->rollback();
    $dbh->disconnect();
    
    print "ok $n\n"; $n++;
}

1;
