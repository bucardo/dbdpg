#!perl

## UNUSED TEST

#use 5.010;
use strict;
use warnings;
use Data::Dumper;
##use Data::HexDump;
use DBD::Pg ':async';
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

pass ('This test is not ready yet');
done_testing();

exit if 1;

use constant {
    DUP_OBJ => '42710',
    USECS   => 1_000_000,
    PG_TO_UNIX_EPOCH_DELTA => 946_684_800,
};

my $slot = 'dbd_pg_test';
my $plugin = 'test_decoding';

my $dbh = connect_database();

my $repl_dbh;
if ($dbh) {
    if ($dbh->{pg_server_version} >= 9.4) {
        $repl_dbh = DBI->connect("$ENV{DBI_DSN};replication=database", $ENV{DBI_USER}, '',
                                 {RaiseError => 1, PrintError => 0, AutoCommit => 1});
        $repl_dbh->{pg_enable_utf8} = 0;
    } else {
        plan skip_all => 'Cannot test logical replication on Postgres < 9.4';
    }
}
else {
	plan skip_all => 'Connection to database failed, cannot continue testing';
}

ok (defined $repl_dbh, 'Connect to database for logical replication testing');

my ($systemid, $timeline, $xlogpos, $dbname) = $repl_dbh->selectrow_array('IDENTIFY_SYSTEM');

ok ($dbname, "connected to specific dbname=$dbname");

my $rv;

eval {
    $rv = $repl_dbh->do(sprintf 'CREATE_REPLICATION_SLOT %s LOGICAL %s',
                        $repl_dbh->quote_identifier($slot), $repl_dbh->quote_identifier($plugin));
};
if ($@) {
    if ($repl_dbh->state ne DUP_OBJ) {
        die sprintf 'err: %s; errstr: %s; state: %s', $repl_dbh->err, $repl_dbh->errstr, $repl_dbh->state;
    } else {
        $rv = 1;
    }
}
ok ($rv, 'replication slot created');

$rv = $repl_dbh->do(sprintf 'START_REPLICATION SLOT %s LOGICAL 0/0', $repl_dbh->quote_identifier($slot));
ok ($rv, 'replication started');

my $lastlsn = 0;
my $tx_watch;
while (1) {
    my @status = ('r', $lastlsn, $lastlsn, 0, ((time - PG_TO_UNIX_EPOCH_DELTA) * USECS), 0);
    my $status = pack 'Aq>4b', @status;
    $repl_dbh->pg_putcopydata($status)
        or die sprintf 'err: %s; errstr: %s; state: %s', $repl_dbh->err, $repl_dbh->errstr, $repl_dbh->state;

    if (!$tx_watch) {
        $dbh->do('set client_min_messages to ERROR');
        $dbh->do('drop table if exists dbd_pg_repltest');
        $dbh->do('create table dbd_pg_repltest (id int)');
        $dbh->do('insert into dbd_pg_repltest (id) values (1)');
        $tx_watch = $dbh->selectrow_array('select txid_current()');
        $dbh->commit;
    }

    my $n = $repl_dbh->pg_getcopydata_async(my $msg);

    if (0 == $n) {
        # nothing ready
        sleep 1;
        next;
    }

    if (-1 == $n) {
        # COPY closed
        last;
    }

    if (-2 == $n) {
        die 'could not read COPY data: ' . $repl_dbh->errstr;
    }

    if ('k' eq substr $msg, 0, 1) {
        my ($type, $lsnpos, $ts, $reply) = unpack 'Aq>2b', $msg;

        $ts = ($ts / USECS) + PG_TO_UNIX_EPOCH_DELTA;

        next;
    }

    if ('w' ne substr $msg, 0, 1) {
        die sprintf 'unrecognized streaming header: "%s"', substr($msg, 0, 1);
    }

    my ($type, $startpos, $lsnpos, $ts, $string) = unpack 'Aq>3a*', $msg;

    $ts = ($ts / USECS) + PG_TO_UNIX_EPOCH_DELTA;

    if ($string eq 'table dbd_pg_testschema.dbd_pg_repltest: INSERT: id[integer]:1') {
        pass ('saw insert event');
        last;
    } elsif ($tx_watch and my ($tx) = $string =~ /^COMMIT (\d+)$/) {
        if ($tx > $tx_watch) {
            fail ('saw insert event');
            last;
        }
    }

    $lastlsn = $lsnpos;
}

$repl_dbh->disconnect();

# cleanup the replication slot and test table
$dbh->do('select pg_drop_replication_slot(?)', undef, $slot);
$dbh->do('drop table if exists dbd_pg_repltest');
$dbh->commit();

$dbh->disconnect();

done_testing();
