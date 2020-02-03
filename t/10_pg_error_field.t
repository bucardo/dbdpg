#!perl

## Test of $dbh->pg_error_field

use 5.008001;
use strict;
use warnings;
use Test::More;
use lib 't','.';
require 'dbdpg_test_setup.pl';
select(($|=1,select(STDERR),$|=1)[1]);

my $dbh = connect_database();

if (! $dbh) {
    plan skip_all => 'Connection to database failed, cannot continue testing';
}

my $t='Connect to database for pg_error_field testing';
isnt ($dbh, undef, $t);

my ($result, $SQL, $qresult);

$t = 'Call to pg_error_field gives a usage error if no specific field given';
eval {
    $dbh->pg_error_field;
};
like ($@, qr{Usage: }, $t);

$t = 'Call to pg_error_field gives an error if a null field is given';
eval {
    no warnings;
    $dbh->pg_error_field(undef);
};
like ($@, qr{Invalid error field}, $t);

eval {
    $dbh->pg_error_field('');
};
like ($@, qr{Invalid error field}, $t);

my $test_table = 'dbdpg_error_field_test';

my $fields = qq{
pg_diag_severity_nonlocalized        | 100001 | undef | ERROR             | ERROR | ERROR | ERROR
pg_diag_severity                     | 70400  | undef | ERROR             | ERROR | ERROR | ERROR
pg_diag_sqlstate,state               | 70400  | undef | 22012             | 42703 | 23514 | undef
pg_diag_message_primary              | 70400  | undef | division by zero  | column "foobar" does not exist | violates check constraint "rainbow" | undef
pg_diag_message_detail,detail        | 90200  | undef | undef             | undef | Failing row contains | undef
pg_diag_message_hint,hint            | 70400  | undef | undef             | undef | undef | undef
pg_diag_statement_position           | 70400  | undef | undef             | 8     | undef | undef
pg_diag_internal_position            | 70400  | undef | undef             | undef | undef | undef
pg_diag_internal_query               | 70400  | undef | undef             | undef | undef | undef
pg_diag_context                      | 70400  | undef | undef             | undef | undef | undef
pg_diag_schema_name,schema           | 90300  | undef | undef             | undef | dbd_pg_testschema | undef
pg_diag_table_name,table             | 90300  | undef | undef             | undef | $test_table | undef
pg_diag_column_name,column           | 90300  | undef | undef             | undef | undef | undef
pg_diag_datatype_name,datatype,type  | 90300  | undef | undef             | undef | undef | undef
pg_diag_constraint_name,constraint   | 90400  | undef | undef             | undef | rainbow | undef
pg_diag_source_file                  | 70400  | undef | int.c             | parse_ | execMain.c | undef
pg_diag_source_line                  | 70400  | undef | number            | number | number | undef
pg_diag_source_function              | 70400  | undef | int4div           | Column | ExecConstraints | undef
};

$dbh->do("CREATE TABLE $test_table (id int, constraint rainbow check(id < 10) )");
$dbh->commit();

my $pgversion = $dbh->{pg_server_version};
for my $loop (1..5) {
    if (2==$loop) { eval { $dbh->do('SELECT 1/0'); }; }
    if (3==$loop) { eval { $dbh->do('SELECT foobar FROM pg_class'); }; }
    if (4==$loop) {
        eval { $dbh->do("INSERT INTO $test_table VALUES (123)"); };
    }
    if (5==$loop) {
        my $sth = $dbh->prepare("INSERT INTO $test_table VALUES (?)");
        eval { $sth->execute(234); };
    }

    for (split /\n/ => $fields) {
        next unless /pg/;
        my ($lfields,$minversion,@error) = split /\s+\|\s+/;
        next if $pgversion < $minversion;
       for my $field (split /,/ => $lfields) {
            my $expected = $error[5==$loop ? 3 : $loop-1];
            $expected = undef if $expected eq 'undef';
            if (defined $expected) {
                $expected = ($expected eq 'number') ? qr/^\d+$/ : qr/$expected/;
            }
            $t = "(query $loop) Calling pg_error_field returns expected value for field $field";
            my $actual = $dbh->pg_error_field($field);
            defined $expected ? like ($actual, $expected, $t) : is($actual, undef, $t);

            $field = uc $field;
            $t = "(query $loop) Calling pg_error_field returns expected value for field $field";
            $actual = $dbh->pg_error_field($field);
            defined $expected ? like ($actual, $expected, $t) : is($actual, undef, $t);

            if ($field =~ s/PG_DIAG_//) {
                $t = "(query $loop) Calling pg_error_field returns expected value for field $field";
                $actual = $dbh->pg_error_field($field);
                defined $expected ? like ($actual, $expected, $t) : is($actual, undef, $t);
            }
        }
    }
    $dbh->rollback();
}

$dbh->do("DROP TABLE $test_table");
$dbh->commit();
$dbh->disconnect();

done_testing();

