if (!exists($ENV{DBDPG_MAINTAINER})) {
    print "1..0\n";
    exit;
}

use strict;
use DBI;

main();

sub main {
    my ($n, $dbh, $sth);
    
    print "1..6\n";
    
    $n = 1;
    
    $dbh = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 0});
    
    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE id = ?
    });
    $sth->bind_param(1, 'foo');
    $sth->bind_param(1, 1);
    
    print "ok $n\n"; $n++;

    $sth = $dbh->prepare(q{
        SELECT id
             , name
          FROM test
         WHERE id = ?
           AND name = ?
    });
    $sth->bind_param(1, 'foo');
    $sth->bind_param(2, 'bar');
    $sth->bind_param(2, 'baz');
    
    print "ok $n\n"; $n++;

    $sth->finish();

    print "ok $n\n"; $n++;

    # Make sure that we get warnings when we try to use SQL_BINARY.
    {
        local $SIG{__WARN__} =
          sub { print $_[0] =~ /^Use of SQL type SQL_BINARY/ ?
                  "ok $n\n" : "no ok $n\n"; $n++
          };

        $sth = $dbh->prepare(q{
            SELECT id
                 , name
              FROM test
             WHERE id = ?
               AND name = ?
        });
        $sth->bind_param(1, 'foo', DBI::SQL_BINARY);
    }
    $dbh->disconnect();

    print "ok $n\n"; $n++;
}

1;            
