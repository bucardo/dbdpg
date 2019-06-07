#!perl

## Test of $dbh->pg_error_field

use 5.006;
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
pg_diag_severity_nonlocalized        | undef | ERROR             | ERROR | ERROR | ERROR
pg_diag_severity                     | undef | ERROR             | ERROR | ERROR | ERROR
pg_diag_sqlstate,state               | undef | 22012             | 42703 | 23514 | undef
pg_diag_message_primary              | undef | division by zero  | column "foobar" does not exist | violates check constraint "rainbow" | undef
pg_diag_message_detail,detail        | undef | undef             | undef | Failing row contains | undef
pg_diag_message_hint,hint            | undef | undef             | undef | undef | undef
pg_diag_statement_position           | undef | undef             | 8     | undef | undef
pg_diag_internal_position            | undef | undef             | undef | undef | undef
pg_diag_internal_query               | undef | undef             | undef | undef | undef
pg_diag_context                      | undef | undef             | undef | undef | undef
pg_diag_schema_name,schema           | undef | undef             | undef | dbd_pg_testschema | undef
pg_diag_table_name,table             | undef | undef             | undef | $test_table | undef
pg_diag_column_name,column           | undef | undef             | undef | undef | undef
pg_diag_datatype_name,datatype,type  | undef | undef             | undef | undef | undef
pg_diag_constraint_name,constraint   | undef | undef             | undef | rainbow | undef
pg_diag_source_file                  | undef | int.c             | parse_relation.c | execMain.c | undef
pg_diag_source_line                  | undef | number            | number | number | undef
pg_diag_source_function              | undef | int4div           | errorMissingColumn | ExecConstraints | undef
};

$dbh->do("CREATE TABLE $test_table (id int, constraint rainbow check(id < 10) )");
$dbh->commit();

for my $loop (1..5) {
    if (2==$loop) { eval { $dbh->do('SELECT 1/0'); }; }
    if (3==$loop) { eval { $dbh->do('SELECT foobar FROM pg_class'); }; }
    if (4==$loop) {
        eval { $dbh->do("INSERT INTO $test_table VALUES (123)"); }
    }
    if (5==$loop) {
        my $sth = $dbh->prepare("INSERT INTO $test_table VALUES (?)");
        eval { $sth->execute(234); };
    }

    for (split /\n/ => $fields) {
        next unless /pg/;
        my ($fields,@error) = split /\s+\|\s+/ => $_;
        for my $field (split /,/ => $fields) {
            my $expected = $error[$loop==5 ? 3 : $loop-1];
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

done_testing();

$dbh->disconnect();
