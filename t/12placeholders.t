#!perl

## Test of placeholders

use 5.008001;
use strict;
use warnings;
use Test::More;
use lib 't','.';
use DBI qw/:sql_types/;
use DBD::Pg qw/:pg_types/;
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}
plan tests => 259;

my $t='Connect to database for placeholder testing';
isnt ($dbh, undef, $t);

my $pgversion = $dbh->{pg_server_version};
if ($pgversion >= 80100) {
  $dbh->do('SET escape_string_warning = false');
}

my ($result, $SQL, $qresult);

# Make sure that quoting works properly.
$t='Quoting works properly';
my $E = $pgversion >= 80100 ? q{E} : q{};
my $quo = $dbh->quote('\\\'?:');
is ($quo, qq{${E}'\\\\''?:'}, $t);

$t='Quoting works with a function call';
# Make sure that quoting works with a function call.
# It has to be in this function, otherwise it doesn't fail the
# way described in https://rt.cpan.org/Ticket/Display.html?id=4996.
sub checkquote {
    my $str = shift;
    return is ($dbh->quote(substr($str, 0, 10)), "'$str'", $t);
}

checkquote('one');
checkquote('two');
checkquote('three');
checkquote('four');

## Github issue #33
my $sth;
if ($dbh->{pg_server_version} >= 90400) {

    $SQL = q{ SELECT '{"a":1}'::jsonb \? 'abc' AND 123=$1};
    for (1..300) {
        $sth = $dbh->prepare($SQL);
        $sth->execute(123);
    }
}

$t='Fetch returns the correct quoted value';
$sth = $dbh->prepare(qq{INSERT INTO dbd_pg_test (id,pname) VALUES (?, $quo)});
$sth->execute(100);
my $sql = "SELECT pname FROM dbd_pg_test WHERE pname = $quo";
$sth = $dbh->prepare($sql);
$sth->execute();
my ($retr) = $sth->fetchrow_array();
is ($retr, '\\\'?:', $t);

$t='Execute with one bind param where none expected fails';
eval {
    $sth = $dbh->prepare($sql);
    $sth->execute('foo');
};
like ($@, qr{when 0 are needed}, $t);

$t='Execute with ? placeholder works';
$sql = 'SELECT pname FROM dbd_pg_test WHERE pname = ?';
$sth = $dbh->prepare($sql);
$sth->execute('\\\'?:');
($retr) = $sth->fetchrow_array();
is ($retr, '\\\'?:', $t);

$t='Execute with :1 placeholder works';
$sql = 'SELECT pname FROM dbd_pg_test WHERE pname = :1';
$sth = $dbh->prepare($sql);
$sth->bind_param(':1', '\\\'?:');
$sth->execute();
($retr) = $sth->fetchrow_array();
is ($retr, '\\\'?:', $t);

$t='Execute with $1 placeholder works';
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = $1 AND pname <> 'foo'};
$sth = $dbh->prepare($sql);
$sth->execute('\\\'?:');
($retr) = $sth->fetchrow_array();
is ($retr, '\\\'?:', $t);

$t='Execute with quoted ? fails with a placeholder';
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = '?'};
eval {
    $sth = $dbh->prepare($sql);
    $sth->execute('foo');
};
like ($@, qr{when 0 are needed}, $t);

$t='Execute with quoted :1 fails with a placeholder';
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = ':1'};
eval {
    $sth = $dbh->prepare($sql);
    $sth->execute('foo');
};
like ($@, qr{when 0 are needed}, $t);

$t='Execute with quoted ? fails with a placeholder';
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = '\\\\' AND pname = '?'};
eval {
    $sth = $dbh->prepare($sql);
    $sth->execute('foo');
};
like ($@, qr{when 0 are needed}, $t);

$t='Execute with named placeholders works';
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = :foobar2 AND pname = :foobar AND pname = :foobar2};
eval {
    $sth = $dbh->prepare($sql);
    $sth->bind_param(':foobar', 123);
    $sth->bind_param(':foobar2', 456);
    $sth->execute();
};
is ($@, q{}, $t);

## Same, but fiddle with whitespace
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = :foobar AND pname = :foobar2 AND pname = :foobar2};
eval {
    $sth = $dbh->prepare($sql);
    $sth->bind_param(':foobar', 123);
    $sth->bind_param(':foobar2', 456);
    $sth->execute();
};
is ($@, q{}, $t);

$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = :foobar AND pname = :foobar AND pname = :foobar2 };
eval {
    $sth = $dbh->prepare($sql);
    $sth->bind_param(':foobar', 123);
    $sth->bind_param(':foobar2', 456);
    $sth->execute();
};
is ($@, q{}, $t);

$t='Execute with repeated named placeholders works';
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = :foobar AND pname = :foobar };
eval {
    $sth = $dbh->prepare($sql);
    $sth->bind_param(':foobar', 123);
    $sth->execute();
};
is ($@, q{}, $t);

## Same thing, different whitespace
$sql = q{SELECT pname FROM dbd_pg_test WHERE pname = :foobar AND pname = :foobar};
eval {
    $sth = $dbh->prepare($sql);
    $sth->bind_param(':foobar', 123);
    $sth->execute();
};
is ($@, q{}, $t);

$t='Prepare with large number of parameters works';
## Test large number of placeholders
$sql = 'SELECT 1 FROM dbd_pg_test WHERE id IN (' . '?,' x 300 . '?)';
my @args = map { $_ } (1..301);
$sth = $dbh->prepare($sql);
my $count = $sth->execute(@args);
$sth->finish();
is ($count, 1, $t);

$sth->finish();

$t='Prepare with backslashes inside quotes works';
$SQL = q{SELECT setting FROM pg_settings WHERE name = 'backslash_quote'};
$count = $dbh->selectall_arrayref($SQL)->[0];
my $backslash = defined $count ? $count->[0] : 0;
my $scs = $dbh->{pg_standard_conforming_strings};
$SQL = $scs ? q{SELECT E'\\'?'} : q{SELECT '\\'?'};
$sth = $dbh->prepare($SQL);
eval {
    $sth->execute();
};
my $expected = $backslash eq 'off' ? qr{unsafe} : qr{};
like ($@, $expected, $t);

## Test quoting of geometric types

my @geotypes = qw/point line lseg box path polygon circle/;

eval { $dbh->do('DROP TABLE dbd_pg_test_geom'); }; $dbh->commit();

$SQL = 'CREATE TABLE dbd_pg_test_geom ( id INT, argh TEXT[], ';
for my $type (@geotypes) {
    $SQL .= "x$type $type,";
}
$SQL =~ s/,$/)/;
$dbh->do($SQL);
$dbh->commit();

my %typemap = (
    point   => PG_POINT,
    line    => PG_LINE,
    lseg    => PG_LSEG,
    box     => PG_BOX,
    path    => PG_PATH,
    polygon => PG_POLYGON,
    circle  => PG_CIRCLE,
);

my $testdata = q{
point datatype integers
12,34
'12,34'
(12,34)

point datatype floating point numbers
1.34,667
'1.34,667'
(1.34,667)

point datatype exponential numbers
1e134,9E4
'1e134,9E4'
(1e+134,90000)

point datatype plus and minus signs
1e+134,-.45
'1e+134,-.45'
(1e+134,-0.45)

point datatype invalid number
123,abc
ERROR: Invalid input for geometric type
ERROR: any

point datatype invalid format
123
'123'
ERROR: any

point datatype invalid format
123,456,789
'123,456,789'
ERROR: any

point datatype invalid format
<(2,4),6>
ERROR: Invalid input for geometric type
ERROR: any

point datatype invalid format
[(1,2)]
ERROR: Invalid input for geometric type
ERROR: any

line datatype integers
12,34
'12,34'
ERROR: not yet implemented

line datatype floating point numbers
1.34,667
'1.34,667'
ERROR: not yet implemented

line datatype exponential numbers
1e134,9E4
'1e134,9E4'
ERROR: not yet implemented

line datatype plus and minus signs
1e+134,-.45
'1e+134,-.45'
ERROR: not yet implemented

line datatype invalid number
123,abc
ERROR: Invalid input for geometric type
ERROR: not yet implemented


lseg datatype invalid format
12,34
'12,34'
ERROR: any

lseg datatype integers
(12,34),(56,78)
'(12,34),(56,78)'
[(12,34),(56,78)]

lseg datatype floating point and exponential numbers
(1.2,3.4),(5e3,7E1)
'(1.2,3.4),(5e3,7E1)'
[(1.2,3.4),(5000,70)]


box datatype invalid format
12,34
'12,34'
ERROR: any

box datatype integers
(12,34),(56,78)
'(12,34),(56,78)'
(56,78),(12,34)

box datatype floating point and exponential numbers
(1.2,3.4),(5e3,7E1)
'(1.2,3.4),(5e3,7E1)'
(5000,70),(1.2,3.4)


path datatype invalid format
12,34
'12,34'
ERROR: any

path datatype integers
(12,34),(56,78)
'(12,34),(56,78)'
((12,34),(56,78))

path datatype floating point and exponential numbers
(1.2,3.4),(5e3,7E1)
'(1.2,3.4),(5e3,7E1)'
((1.2,3.4),(5000,70))

path datatype alternate bracket format
[(1.2,3.4),(5e3,7E1)]
'[(1.2,3.4),(5e3,7E1)]'
[(1.2,3.4),(5000,70)]

path datatype many elements
(1.2,3.4),(5,6),(7,8),(-9,10)
'(1.2,3.4),(5,6),(7,8),(-9,10)'
((1.2,3.4),(5,6),(7,8),(-9,10))

path datatype fails with braces
{(1,2),(3,4)}
ERROR: Invalid input for path type
ERROR: any


polygon datatype invalid format
12,34
'12,34'
ERROR: any

polygon datatype integers
(12,34),(56,78)
'(12,34),(56,78)'
((12,34),(56,78))

polygon datatype floating point and exponential numbers
(1.2,3.4),(5e3,7E1)
'(1.2,3.4),(5e3,7E1)'
((1.2,3.4),(5000,70))

polygon datatype many elements
(1.2,3.4),(5,6),(7,8),(-9,10)
'(1.2,3.4),(5,6),(7,8),(-9,10)'
((1.2,3.4),(5,6),(7,8),(-9,10))

polygon datatype fails with brackets
[(1,2),(3,4)]
ERROR: Invalid input for geometric type
ERROR: any



circle datatype invalid format
(12,34)
'(12,34)'
ERROR: any

circle datatype integers
<(12,34),5>
'<(12,34),5>'
<(12,34),5>

circle datatype floating point and exponential numbers
<(-1.2,2E2),3e3>
'<(-1.2,2E2),3e3>'
<(-1.2,200),3000>

circle datatype fails with brackets
[(1,2),(3,4)]
ERROR: Invalid input for circle type
ERROR: any

};

$testdata =~ s/^\s+//;
my $curtype = '';
for my $line (split /\n\n+/ => $testdata) {
    my ($text,$input,$quoted,$rows) = split /\n/ => $line;
    next if ! $text;
    $t = "Geometric type test: $text";
    (my $type) = ($text =~ m{(\w+)});
    last if $type eq 'LAST';
    if ($curtype ne $type) {
        $curtype = $type;
        eval { $dbh->do('DEALLOCATE geotest'); }; $dbh->commit();
        $dbh->do(qq{PREPARE geotest($type) AS INSERT INTO dbd_pg_test_geom(x$type) VALUES (\$1)});
        $sth = $dbh->prepare(qq{INSERT INTO dbd_pg_test_geom(x$type) VALUES (?)});
        $sth->bind_param(1, '', {pg_type => $typemap{$type} });
    }
    $dbh->do('DELETE FROM dbd_pg_test_geom');
    eval { $qresult = $dbh->quote($input, {pg_type => $typemap{$type}}); };
    if ($@) {
        if ($quoted !~ /ERROR: (.+)/) { ## no critic
            fail ("$t error: $@");
        }
        else {
            like ($@, qr{$1}, $t);
        }
    }
    else {
        is ($qresult, $quoted, $t);
    }
    $dbh->commit();

    eval { $dbh->do("EXECUTE geotest('$input')"); };
    if ($@) {
        if ($rows !~ /ERROR: .+/) {
            fail ("$t error: $@");
        }
        else {
            ## Do any error for now: i18n worries
            pass ($t);
        }
    }
    $dbh->commit();

    eval { $sth->execute($input); };
    if ($@) {
        if ($rows !~ /ERROR: .+/) {
            fail ($t);
        }
        else {
            ## Do any error for now: i18n worries
            pass ($t);
        }
    }
    $dbh->commit();

    if ($rows !~ /ERROR/) {
        $SQL = "SELECT x$type FROM dbd_pg_test_geom";
        $expected = [[$rows],[$rows]];
        $result = $dbh->selectall_arrayref($SQL);
        is_deeply ($result, $expected, $t);
    }
}

$t='Calling do() with non-DML placeholder works';
$sth->finish();
$dbh->commit();
eval {
  $dbh->do(q{SET search_path TO ?}, undef, 'pg_catalog');
};
is ($@, q{}, $t);
$dbh->rollback();

$t='Calling do() with DML placeholder works';
$dbh->commit();
eval {
  $dbh->do(q{SELECT ?::text}, undef, 'public');
};
is ($@, q{}, $t);

$t='Calling do() with invalid crowded placeholders fails cleanly';
$dbh->commit();
eval {
    $dbh->do(q{SELECT ??}, undef, 'public', 'error');
};
is ($dbh->state, '42601', $t);

$t='Prepare/execute with non-DML placeholder works';
$dbh->commit();
eval {
  $sth = $dbh->prepare(q{SET search_path TO ?});
  $sth->execute('pg_catalog');
};
is ($@, q{}, $t);
$dbh->rollback();

$t='Prepare/execute does not allow geometric operators';
eval {
    $sth = $dbh->prepare(q{SELECT ?- lseg '(1,0),(1,1)'});
    $sth->execute();
};
like ($@, qr{unbound placeholder}, $t);

$t='Prepare/execute allows geometric operator ?- when dollaronly is set';
$dbh->commit();
$dbh->{pg_placeholder_dollaronly} = 1;
eval {
    $sth = $dbh->prepare(q{SELECT ?- lseg '(1,0),(1,1)'});
    $sth->execute();
    $sth->finish();
};
is ($@, q{}, $t);

$t='Prepare/execute allows geometric operator ?# when dollaronly set';
$dbh->commit();
eval {
    $sth = $dbh->prepare(q{SELECT lseg'(1,0),(1,1)' ?# lseg '(2,3),(4,5)'});
    $sth->execute();
    $sth->finish();
};
is ($@, q{}, $t);

$t=q{Value of placeholder_dollaronly can be retrieved};
is ($dbh->{pg_placeholder_dollaronly}, 1, $t);

$t=q{Prepare/execute does not allow use of raw ? and :foo forms};
$dbh->{pg_placeholder_dollaronly} = 0;
eval {
    $sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1});
    $sth->execute();
    $sth->finish();
};
like ($@, qr{mix placeholder}, $t);

$t='Prepare/execute allows use of raw ? and :foo forms when dollaronly set';
$dbh->{pg_placeholder_dollaronly} = 1;
eval {
    $sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1}, {pg_placeholder_dollaronly => 1});
    $sth->{pg_placeholder_dollaronly} = 1;
    $sth->execute();
    $sth->finish();
};
like ($@, qr{unbound placeholder}, $t);

$t='Prepare works with pg_placeholder_dollaronly';
$dbh->{pg_placeholder_dollaronly} = 0;
eval {
    $sth = $dbh->prepare(q{SELECT uno ?: dos ? tres :foo bar $1}, {pg_placeholder_dollaronly => 1});
    $sth->execute();
    $sth->finish();
};
like ($@, qr{unbound placeholder}, $t);

$t=q{Value of placeholder_nocolons defaults to 0};
is ($dbh->{pg_placeholder_nocolons}, 0, $t);

$t='Simple array slices do not get picked up as placeholders';
$SQL = q{SELECT argh[1:2] FROM dbd_pg_test_geom WHERE id = ?};
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute(1);
    $sth->finish();
};
is ($@, q{}, $t);

$t='Without placeholder_nocolons, queries with array slices fail';
$SQL = q{SELECT argh[1 :2] FROM dbd_pg_test_geom WHERE id = ?};
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute(1);
    $sth->finish();
};
like ($@, qr{Cannot mix placeholder styles}, $t);

$t='Use of statement level placeholder_nocolons allows use of ? placeholders while ignoring :';
eval {
    $sth = $dbh->prepare($SQL, {pg_placeholder_nocolons => 1});
    $sth->execute(1);
    $sth->finish();
};
is ($@, q{}, $t);

$t='Use of database level placeholder_nocolons allows use of ? placeholders while ignoring :';
$dbh->{pg_placeholder_nocolons} = 1;
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute(1);
    $sth->finish();
};
is ($@, q{}, $t);

$t=q{Value of placeholder_nocolons can be retrieved};
is ($dbh->{pg_placeholder_nocolons}, 1, $t);

$t='Use of statement level placeholder_nocolons allows use of $ placeholders while ignoring :';
$dbh->{pg_placeholder_nocolons} = 0;
$SQL = q{SELECT argh[1:2] FROM dbd_pg_test_geom WHERE id = $1};
eval {
    $sth = $dbh->prepare($SQL, {pg_placeholder_nocolons => 1});
    $sth->execute(1);
    $sth->finish();
};
is ($@, q{}, $t);

$t='Use of database level placeholder_nocolons allows use of $ placeholders while ignoring :';
$dbh->{pg_placeholder_nocolons} = 1;
eval {
    $sth = $dbh->prepare($SQL);
    $sth->execute(1);
    $sth->finish();
};
is ($@, q{}, $t);
$dbh->{pg_placeholder_nocolons} = 0;

$t='Prepare works with identical named placeholders';
eval {
    $sth = $dbh->prepare(q{SELECT :row, :row, :row, :yourboat});
    $sth->finish();
};
is ($@, q{}, $t);

$t='Prepare works with placeholders after double slashes';
eval {
    $dbh->do(q{CREATE OPERATOR // ( PROCEDURE=bit, LEFTARG=int, RIGHTARG=int )});
    $sth = $dbh->prepare(q{SELECT ? // ?});
    $sth->execute(1,2);
    $sth->finish();
};
is ($@, q{}, $t);

$t='Dollar quotes starting with a number are not treated as valid identifiers';
eval {
    $sth = $dbh->prepare(q{SELECT $123$  $123$});
    $sth->execute(1);
    $sth->finish();
};
like ($@, qr{Invalid placeholders}, $t);

$t='Dollar quotes with invalid characters are not parsed as identifiers';
for my $char (qw!+ / : @ [ `!) { ## six characters
    eval {
        $sth = $dbh->prepare(qq{SELECT \$abc${char}\$ 123 \$abc${char}\$});
        $sth->execute();
        $sth->finish();
    };
    like ($@, qr{syntax error}, "$t: char=$char");
}

$t='Dollar quotes with valid characters are parsed as identifiers';
$dbh->rollback();
for my $char (qw{0 9 A Z a z}) { ## six letters
    eval {
        $sth = $dbh->prepare(qq{SELECT \$abc${char}\$ 123 \$abc${char}\$});
        $sth->execute();
        $sth->finish();
    };
    is ($@, q{}, $t);
}

SKIP: {
    my $server_encoding = $dbh->selectrow_array('SHOW server_encoding');
    my $client_encoding = $dbh->selectrow_array('SHOW client_encoding');
    skip "Cannot test non-ascii dollar quotes with server_encoding='$server_encoding' (need UTF8 or SQL_ASCII)", 3,
        unless $server_encoding =~ /\A(?:UTF8|SQL_ASCII)\z/;

    skip 'Cannot test non-ascii dollar quotes unless client_encoding is UTF8', 3
        if $client_encoding ne 'UTF8';

    for my $ident (qq{\x{5317}}, qq{abc\x{5317}}, qq{_cde\x{5317}}) { ## hi-bit chars
        eval {
            $sth = $dbh->prepare(qq{SELECT \$$ident\$ 123 \$$ident\$});
            $sth->execute();
            $sth->finish();
        };
        is ($@, q{}, $t);
    }
}

SKIP: {
    skip 'Cannot run backslash_quote test on Postgres < 8.2', 1 if $pgversion < 80200;

    $t='Backslash quoting inside double quotes is parsed correctly';
    $dbh->do(q{SET backslash_quote = 'on'});
    $dbh->commit();
    eval {
        $sth = $dbh->prepare(q{SELECT * FROM "\" WHERE a=?});
        $sth->execute(1);
        $sth->finish();
    };
    like ($@, qr{relation ".*" does not exist}, $t);
}

$dbh->rollback();

SKIP: {
    skip 'Cannot adjust standard_conforming_strings for testing on this version of Postgres', 4 if $pgversion < 80200;
    $t='Backslash quoting inside single quotes is parsed correctly with standard_conforming_strings off';
    $dbh->do(q{SET standard_conforming_strings = 'off'});
    eval {
        local $dbh->{Warn} = '';
        $sth = $dbh->prepare(q{SELECT '\', ?});
        $sth->execute();
        $sth->finish();
    };
    like ($@, qr{unterminated quoted string}, $t);
    $dbh->rollback();

    $t=q{Backslash quoting inside E'' is parsed correctly with standard_conforming_strings = 'off'};
    eval {
        $sth = $dbh->prepare(q{SELECT E'\'?'});
        $sth->execute();
        $sth->finish;
    };
    is ($@, q{}, $t);
    $dbh->rollback();

    $t='Backslash quoting inside single quotes is parsed correctly with standard_conforming_strings on';
    eval {
        $dbh->do(q{SET standard_conforming_strings = 'on'});
        $sth = $dbh->prepare(q{SELECT '\', ?::int});
        $sth->execute(1);
        $sth->finish();
    };
    is ($@, q{}, $t);

    $t=q{Backslash quoting inside E'' is parsed correctly with standard_conforming_strings = 'on'};
    eval {
        $sth = $dbh->prepare(q{SELECT E'\'?'});
        $sth->execute();
        $sth->finish;
    };
    is ($@, q{}, $t);
}


$t='Valid integer works when quoting with SQL_INTEGER';
my $val;
$val = $dbh->quote('123', SQL_INTEGER);
is ($val, 123, $t);

$t='Invalid integer fails to pass through when quoting with SQL_INTEGER';
$val = -1;
eval {
    $val = $dbh->quote('123abc', SQL_INTEGER);
};
like ($@, qr{Invalid integer}, $t);
is ($val, -1, $t);

my $prefix = 'Valid float value works when quoting with SQL_FLOAT';
for my $float ('123','0.00','0.234','23.31562', '1.23e04','6.54e+02','4e-3','NaN','Infinity','-infinity') {
    $t = "$prefix (value=$float)";
    $val = -1;
    eval { $val = $dbh->quote($float, SQL_FLOAT); };
    is ($@, q{}, $t);
    is ($val, $float, $t);

    next unless $float =~ /\w/;

    my $lcfloat = lc $float;
    $t = "$prefix (value=$lcfloat)";
    $val = -1;
    eval { $val = $dbh->quote($lcfloat, SQL_FLOAT); };
    is ($@, q{}, $t);
    is ($val, $lcfloat, $t);

    my $ucfloat = uc $float;
    $t = "$prefix (value=$ucfloat)";
    $val = -1;
    eval { $val = $dbh->quote($ucfloat, SQL_FLOAT); };
    is ($@, q{}, $t);
    is ($val, $ucfloat, $t);
}

$prefix = 'Invalid float value fails when quoting with SQL_FLOAT';
for my $float ('3abc','123abc','','NaNum','-infinitee') {
    $t = "$prefix (value=$float)";
    $val = -1;
    eval { $val = $dbh->quote($float, SQL_FLOAT); };
    like ($@, qr{Invalid float}, $t);
    is ($val, -1, $t);
}

$dbh->rollback();

## Test placeholders plus binding
$t='Bound placeholders enforce data types when not using server side prepares';
$dbh->trace(0);
$dbh->{pg_server_prepare} = 0;
$sth = $dbh->prepare('SELECT (1+?+?)::integer');
$sth->bind_param(1, 1, SQL_INTEGER);
eval {
    $sth->execute('10foo',20);
};
like ($@, qr{Invalid integer}, 'Invalid integer test 2');

## Test quoting of the "name" type
$prefix = q{The 'name' data type does correct quoting};

for my $word (qw/User user USER trigger Trigger user-user/) {
    $t = qq{$prefix for the word "$word"};
    my $got = $dbh->quote($word, { pg_type => PG_NAME });
    $expected = qq{"$word"};
    is ($got, $expected, $t);
}

for my $word (qw/auser userz/) {
    $t = qq{$prefix for the word "$word"};
    my $got = $dbh->quote($word, { pg_type => PG_NAME });
    $expected = qq{$word};
    is ($got, $expected, $t);
}

## Test quoting of booleans

my %booltest = ( ## no critic (Lax::ProhibitLeadingZeros::ExceptChmod, ValuesAndExpressions::ProhibitLeadingZeros, ValuesAndExpressions::ProhibitDuplicateHashKeys)
undef         => 'NULL',
't'           => 'TRUE',
'T'           => 'TRUE',
'true'        => 'TRUE',
'TRUE'        => 'TRUE',
1             => 'TRUE',
01            => 'TRUE',
'1'           => 'TRUE',
'0E0'         => 'TRUE',
'0e0'         => 'TRUE',
'0 but true'  => 'TRUE',
'0 BUT TRUE'  => 'TRUE',
'f'           => 'FALSE',
'F'           => 'FALSE',
0             => 'FALSE',
00            => 'FALSE',
'0'           => 'FALSE',
'false'       => 'FALSE',
'FALSE'       => 'FALSE',
12            => 'ERROR',
'01'          => 'ERROR',
'00'          => 'ERROR',
' false'      => 'ERROR',
' TRUE'       => 'ERROR',
'FALSEY'      => 'ERROR',
'trueish'     => 'ERROR',
'0E0E0'       => 'ERROR', ## Jungle love...
'0 but truez' => 'ERROR',
);

while (my ($name,$res) = each %booltest) {
    $name = undef if $name eq 'undef';
    $t = sprintf 'Boolean quoting of %s',
        defined $name ? qq{"$name"} : 'undef';
    eval { $result = $dbh->quote($name, {pg_type => PG_BOOL}); };
    if ($@) {
        if ($res eq 'ERROR' and $@ =~ /Invalid boolean/) {
            pass ($t);
        }
        else {
            fail ("Failure at $t: $@");
        }
        $dbh->rollback();
    }
    else {
        is ($result, $res, $t);
    }
}

## Test of placeholder escaping. Enabled by default, so let's jump right in
$t = q{Basic placeholder escaping works via backslash-question mark for \?};

## But first, we need some operators
$dbh->do('create operator ? (leftarg=int,rightarg=int,procedure=int4eq)');
$dbh->commit();
$dbh->do('create operator ?? (leftarg=text,rightarg=text,procedure=texteq)');
$dbh->commit();

## This is necessary to "reset" the var so we can test the modification properly
undef $SQL;

$SQL = qq{SELECT count(*) FROM dbd_pg_test WHERE id \\? ?}; ## no critic
my $original_sql = "$SQL"; ## Need quotes because we don't want a shallow copy!
$sth = $dbh->prepare($SQL);
eval {
    $count = $sth->execute(123);
};
is ($@, '', $t);
$sth->finish();

$t = q{Basic placeholder escaping does NOT modify the original string}; ## RT 114000
is ($SQL, $original_sql, $t);

$t = q{Basic placeholder escaping works via backslash-question mark for \?\?};
$SQL = qq{SELECT count(*) FROM dbd_pg_test WHERE pname \\?\\? ?}; ## no critic
$sth = $dbh->prepare($SQL);
eval {
    $count = $sth->execute('foobar');
};
is ($@, '', $t);
$sth->finish();

## This is an emergency hatch only. Hopefully will never be used in the wild!
$dbh->{pg_placeholder_escaped} = 0;
$t = q{Basic placeholder escaping fails when pg_placeholder_escaped is set to false};
$SQL = qq{SELECT count(*) FROM dbd_pg_test WHERE pname \\?\\? ?}; ## no critic
$sth = $dbh->prepare($SQL);
eval {
    $count = $sth->execute('foobar');
};
like ($@, qr{execute}, $t);
$sth->finish();

## The space before the colon is significant here
$SQL = q{SELECT testarray [1 :5] FROM dbd_pg_test WHERE pname = :foo};
$sth = $dbh->prepare($SQL);
eval {
    $sth->bind_param(':foo', 'abc');
    $count = $sth->execute();
};
like ($@, qr{execute}, $t);
$sth->finish();

$t = q{Placeholder escaping works for colons};
$dbh->{pg_placeholder_escaped} = 1;
$SQL = q{SELECT testarray [1 \:5] FROM dbd_pg_test WHERE pname = :foo};
$sth = $dbh->prepare($SQL);
eval {
    $sth->bind_param(':foo', 'abc');
    $count = $sth->execute();
};
is ($@, '', $t);
$sth->finish();


## Begin custom type testing

$dbh->rollback();

cleanup_database($dbh,'test');
$dbh->disconnect();
