if (!exists($ENV{DBDPG_MAINTAINER})) {
    print "1..0\n";
    exit;
}

use strict;
use DBI;

main();

sub main {
    my ($n, $dbh, $sth);
    
    print "1..7\n";
    
    $n = 1;
    
    $dbh = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 0});
    
    print "ok $n\n"; $n++;

    $dbh->do(q{INSERT INTO test (id, name, value) VALUES (1, 'foo', 'horse')});
    $dbh->do(q{INSERT INTO test (id, name, value) VALUES (2, 'bar', 'chicken')});
    $dbh->do(q{INSERT INTO test (id, name, value) VALUES (3, 'baz', 'pig')});
    $dbh->commit();

    print "ok $n\n"; $n++;
    
    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
    });
    $sth->execute();
    
    my $rows = 0;
    while (my ($id, $name) = $sth->fetchrow_array()) {
        if (defined($id) && defined($name)) {
            $rows++;
        }
    }
    
    $sth->finish();
    
    if ($rows != 3) {
        print "not ";
    }

    print "ok $n\n"; $n++;
    
    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE 1 = 0
    });
    $sth->execute();
    
    $rows = 0;
    while (my ($id, $name) = $sth->fetchrow_array()) {
        $rows++;
    }
    
    $sth->finish();
    
    if ($rows != 0) {
        print "not ";
    }

    print "ok $n\n"; $n++;
    
    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE id = ?
    });
    $sth->execute(1);
    
    $rows = 0;
    while (my ($id, $name) = $sth->fetchrow_array()) {
        if (defined($id) && defined($name)) {
            $rows++;
        }
    }
    
    $sth->finish();
    
    if ($rows != 1) {
        print "not ";
    }

    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE name = ?
    });
    $sth->execute('foo');
    
    $rows = 0;
    while (my ($id, $name) = $sth->fetchrow_array()) {
        if (defined($id) && defined($name)) {
            $rows++;
        }
    }
    
    $sth->finish();
    
    if ($rows != 1) {
        print "not ";
    }

    print "ok $n\n"; $n++;
        
    $dbh->disconnect();
    
    print "ok $n\n"; $n++;
}

1;    
    
