#!/usr/bin/env perl

BEGIN {
	use lib '.', 'blib/lib', 'blib/arch';
	system 'make';
}

use strict;
use warnings;
use DBI ':sql_types';
use utf8;
use Data::Dumper;
use YAML;
use DBD::Pg qw/:pg_types/;
use Data::Peek;
use Devel::Leak;
use Time::HiRes qw/ sleep /;

use vars qw/$sth $info $count $SQL/;

my $tracelevel = shift || 0;
$ENV{DBI_TRACE} = $tracelevel;

my $DSN = 'DBI:Pg:dbname=postgres';
my $dbh = DBI->connect($DSN, '', '', {AutoCommit=>0,RaiseError=>1,PrintError=>0})
  or die "Connection failed!\n";

my $me = $dbh->{Driver}{Name};
my $sversion = $dbh->{pg_server_version};
print "DBI is version $DBI::VERSION, I am $me, version of DBD::Pg is $DBD::Pg::VERSION, server is $sversion\n";

print "Name: $dbh->{Name}\n";

$dbh->{RaiseError} = 0;
$dbh->{PrintError} = 1;
$dbh->{AutoCommit} = 1;


exit;

#column_types_github_issue_24();

#read_only_arrays();

# bad_string_length();

# jsonb_placeholder();

#fatal_client();

#user_arrays();

#commit_return_test();

#utf8_print_test();

#memory_leak_test_bug_65734();

#memory_leak_arrays();


sub column_types_github_issue_24 {

    ## Code from https://gist.githubusercontent.com/jef-sure/9a28e7c12f0c03d32080456afd4dafd3/raw/4ada2362371d930c9b035bd749f7b93a6d75cfc1/column-types.pl

    sub table_columns {
        my $table = $_[0];
        my @columnlist;
        my $cih = $dbh->column_info(undef, undef, $table, undef) or die "no table $table";
        my $i = 0;
        while (my $chr = $cih->fetchrow_hashref) {
            my $cn = $chr->{COLUMN_NAME};
            $cn =~ s/\"//g;
            push @columnlist, [$cn, $chr->{TYPE_NAME}];
        }
        return \@columnlist;
    }

    sub query_columns {
        my $query = $_[0];
        my $sth = $dbh->prepare($query) or die "query $query error: " . $dbh->errstr;
        $sth->execute or die "query $query error: " . $dbh->errstr;
        my @columnlist;
        for (my $cn = 0; $cn < @{$sth->{NAME}}; ++$cn) {
            my $ti = $dbh->type_info($sth->{TYPE}->[$cn]);
            my $cn = $sth->{NAME}->[$cn];
            $cn =~ s/\"//g;
            push @columnlist, [$cn, $ti->{TYPE_NAME} // 'UNKNOWN'];
        }
        return \@columnlist;
    }

    sub print_columns {
        my ($name, $cref) = @_;
        print "\n$name:\n";
        for my $ci (@$cref) {
            print "$ci->[0]: $ci->[1]\n";
        }
    }

#anton=> \d todo
#                           Table "public.todo"
#  Column   |  Type   |                     Modifiers
#-----------+---------+---------------------------------------------------
# id        | integer | not null default nextval('todo_id_seq'::regclass)
# title     | text    |
# completed | boolean |
# misc      | jsonb   |
#Indexes:
#    "todo_pkey" PRIMARY KEY, btree (id)

    $SQL = 'CREATE TABLE todo ( id SERIAL PRIMARY KEY, title text, completed boolean, misc jsonb )';
    $dbh->do($SQL);

    print_columns("todo",               table_columns("todo"));
    print_columns("select * from todo", query_columns("select * from todo"));

#output:
#
#todo:
#id: integer
#title: text
#completed: boolean
#misc: jsonb
#
#select * from todo:
#id: int4
#title: UNKNOWN
#completed: bool
#misc: unknown

    exit;

} ## end of column_types_github_issue_24


sub read_only_arrays {

    ## For RT ticket #107556

    $SQL = 'SELECT 5, NULL, ARRAY[1,2,3], ARRAY[1,NULL,3]';
    $sth = $dbh->prepare($SQL);
    $sth->execute;
    while( my $row = $sth->fetchrow_arrayref ) {
        $row->[0] += 0; # ok
        $row->[1] += 0; # ok
        $_ += 0 foreach @{ $row->[2] }; # ok
        $_ += 0 foreach @{ $row->[3] }; # error: Modification of a read-only value attempted
    }

    exit;

} ## end of read_only_arrays

sub bad_string_length {

    ## RT Ticket 114548
    $SQL = 'SELECT md5(x::text) FROM generate_series(1,5) x';

    $sth = $dbh->prepare($SQL);
    $sth->execute();
    my $md5size;
    $sth->bind_columns(\$md5size);
    while ($sth->fetch()) {
        print "\n";
        DDump $md5size;
        print $md5size , "\n";
        printf "%vx\n", $md5size;
        print '.' x 32, '-' x 32 . "\n";
        print substr($md5size, 0, 32), " (" . length($md5size) . ' -- ' . length(substr($md5size, 0, 32)) . ")\n";
    }

} ## end of bad_string_length

sub jsonb_placeholder {

    ## Github #33
    ## https://github.com/bucardo/dbdpg/issues/33

    print "Starting jsonb placeholder test\n";

    $SQL = q{ SELECT '{"a":1}'::jsonb \? 'abc' and 1=$1 };

    for ( my $i=0; $i<100; $i++ ) {
        print "$i.. ";
        $sth = $dbh->prepare($SQL);
        $sth->execute(2);
        $sth->finish();
    }
    print "\n";
}


sub fatal_client {

    ## RT 109591

    print "Test of client_min_messages FATAL and resulting errstr\n";

    $dbh->do(q{SET client_min_messages = 'FATAL'});

    eval {
        $dbh->do('SELECT 1 FROM nonesuch');
    };

    printf "\$@ is: %s\n", $@;
    printf "errstr is: %s\n", $dbh->errstr;
    printf "state is: %s\n", $dbh->state;


    exit;


} ## end of fatal_client


sub memory_leak_arrays {

#  $dbh->{pg_expand_array} = 0;

	$dbh->do('CREATE TABLE leaktest ( id TEXT, arr TEXT[] )');
	$dbh->do('TRUNCATE TABLE leaktest');
	for my $var (qw/ a b c/ ) {
		$dbh->do(qq{INSERT INTO leaktest VALUES ( '$var', '{"a","b","c"}' )});
	}

	my $sth = $dbh->prepare( 'SELECT arr FROM leaktest' );
	my $count0 = 0;

	{
		my $handle;
		my $count1 = Devel::Leak::NoteSV( $handle );
		$sth->execute();
		my $r = $sth->fetchall_arrayref( {} );
		my $count2 = Devel::Leak::NoteSV( $handle );
		$count0 ||= $count1;
		my $diff = $count2 - $count0;
		printf "New SVs: %4d  Total: %d\n", $diff, $count2;
		sleep 0.2;
		last if $diff > 100;
		redo;
	}

} ## end of memory_leak_arrays


sub user_arrays {

print "User arrays!\n";

print Dumper $dbh->type_info(-5);

$dbh->do ("create table xx_test (c_test bigint)");
my $sth = $dbh->prepare ("select * from xx_test");
$sth->execute;
DDumper ($sth->{TYPE}[0], $dbh->type_info ($sth->{TYPE}[0]));
$dbh->do ("drop table xx_test");

exit;

$dbh->do('drop table if exists domodomo');
$dbh->do('create domain domo as int[][]');
$dbh->do('create table domodomo (id serial, foo domo)');
$SQL = 'INSERT INTO domodomo(foo) VALUES (?)';
$sth = $dbh->prepare($SQL);
$sth->execute(q!{{1},{2}}!);

$SQL = 'SELECT foo FROM domodomo';
my $f = $dbh->prepare($SQL);
$f->execute();
my $res = $f->fetchall_arrayref();
print Dumper $res;
print $res->[0];

$dbh->do("CREATE TYPE customint AS ENUM('1','2')");
my $q2 = $dbh->prepare("SELECT '{1,2}'::customint[]");
$q2->execute();
print Dumper $q2->fetchrow_array(); # prints "{1,2}", not an array


exit;

} ## end of user_arrays


sub commit_return_test {

	$dbh->{RaiseError} = 0;
	$dbh->{PrintError} = 1;
	$dbh->{AutoCommit} = 0;

	## Test value returned by the commit() method
	my $res = $dbh->commit();
	print "-->Initial commit returns a value of $res\n";

	$res = $dbh->commit();
	print "-->When called twice, commit returns a value of $res\n";

	$dbh->do('SELECT 123');
	$dbh->do('SELECT fail');
	$dbh->do('SELECT 111');

	$res = $dbh->commit();
	print "-->After exception, commit returns a value of $res\n";

	$dbh->do('SELECT 456');

	return;

} ## end of commit_return_test


sub utf8_print_test {

	## Set things up
	$dbh->do('CREATE TEMPORARY TABLE ctest (c TEXT)');

	## Add some UTF-8 content
	$dbh->do("INSERT INTO ctest VALUES ('*JIHOMORAVSKÝ*')");
	$dbh->do("INSERT INTO ctest VALUES ('*Špindlerův Mlýn*')");

	## Pull data back out via execute/bind/fetch
	$SQL = 'SELECT c FROM ctest';

	my $result;

	for my $loop (1..4) {

		my $onoff = 'off';
		if ($loop == 1 or $loop==3) {
			$dbh->{pg_enable_utf8} = 0;
		}
		else {
			$dbh->{pg_enable_utf8} = 1;
			$onoff = 'on';
		}

		if ($loop>2) {
			binmode STDOUT, ':utf8';
		}

		$sth = $dbh->prepare($SQL);
		$sth->execute();
		$sth->bind_columns(\$result);
		while ($sth->fetch() ) {
			print DPeek $result;
			print "\n Print with pg_enable_utf8 $onoff: $result\n";
			warn " Warn with pg_enable_utf8 $onoff: $result\n\n";
			utf8::upgrade($result);
			print DPeek $result; print "\n\n";
		}
	}

} ## end of utf8_print_test

sub memory_leak_test_bug_65734 {

	## Memory leak when an array appears in the bind variables

	## Set things up
	$dbh->do('CREATE TEMPORARY TABLE tbl1 (id SERIAL PRIMARY KEY, val INTEGER[])');
	$dbh->do('CREATE TEMPORARY TABLE tbl2 (id SERIAL PRIMARY KEY, val INTEGER)');

	## Subroutine that performs the leaking action
	sub leakmaker1 {
		$dbh->do('INSERT INTO tbl1(val) VALUES (?)', undef, [123]);
	}

	## Control subroutine that does not leak
	sub leakmaker2 {
		$dbh->do('INSERT INTO tbl2(val) VALUES (?)', undef, 123);
	}

	leakcheck(\&leakmaker1,1000);

	exit;

} ## end of memory_leak_test_bug_65734


sub leakcheck {

	my $sub = shift;
	my $count = shift || 1000;
	my $maxsize = shift || 100000;

	## Safety check:
	if (exists $ENV{DBI_TRACE} and $ENV{DBI_TRACE} != 0 and $ENV{DBI_TRACE} != 42) {
		$maxsize = 1;
	}

	my $runs = 0;

	while (1) {

		last if $runs++ >= $maxsize;

		&$sub();

		unless ($runs % $count) {
			printf "Cycles: %d\tProc size: %uK\n",
				  $runs,
				  (-f "/proc/$$/stat")
				  ? do { local @ARGV="/proc/$$/stat"; (split (/\s/, <>))[22] / 1024 }
				  : -1;
		}


	}

} ## end of leakcheck

__END__
