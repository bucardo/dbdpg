#!perl -w
if (!exists($ENV{DBDPG_MAINTAINER})) {
    print "1..0\n";
    exit;
}

use strict;
use DBI;

main();

sub main {
    my ($n, $dbh, @tests);
    
    @tests = (
        ["'", "'\\" . sprintf("%03o", ord("'")) . "'"]
      , ["''", "'" . ("\\" . sprintf("%03o", ord("'")))x2 . "'"]
      , ["\\", "'\\" . sprintf("%03o", ord("\\")) . "'"]
      , ["\\'", sprintf("'\\%03o\\%03o'", ord("\\"), ord("'"))]
      , ["\\'?:", sprintf("'\\%03o\\%03o?:'", ord("\\"), ord("'"))]
    );
    
    print "1..8\n";
    
    $n = 1;

    $dbh = DBI->connect("dbi:Pg:dbname=$ENV{DBDPG_TEST_DB};host=$ENV{DBDPG_TEST_HOST}", $ENV{DBDPG_TEST_USER}, $ENV{DBDPG_TEST_PASS}, {RaiseError => 1, AutoCommit => 0, PrintError => 0});
    print "ok $n\n"; $n++;
    
    for (@tests) {
        my ($unq, $quo, $ref);
        
        $unq = $_->[0];
        $ref = $_->[1];
        $quo = $dbh->quote($unq);
        
        if ($quo ne $ref) {
            warn "$unq -> $quo rather than $ref";
            print "not ";
        }
        
        print "ok $n\n"; $n++;
    }
    
    # Make sure that SQL_BINARY doesn't work.
#    eval { $dbh->quote('foo', { TYPE => DBI::SQL_BINARY })};
    eval { $dbh->quote('foo', DBI::SQL_BINARY)};
    print $@ && $@ =~ /Use of SQL_BINARY invalid in quote/ ?
      "ok $n\n" : "not ok $n\n"; $n++;

    $dbh->disconnect();
    
    print "ok $n\n"; $n++;
}

1;
