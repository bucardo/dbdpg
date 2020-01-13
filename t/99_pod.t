#!perl

## Check our Pod, requires Test::Pod
## Also done if available: Test::Pod::Coverage

use 5.006;
use strict;
use warnings;
use Test::More;
select(($|=1,select(STDERR),$|=1)[1]);

if (! $ENV{AUTHOR_TESTING}) {
    plan (skip_all =>  'Test skipped unless environment variable AUTHOR_TESTING is set');
}

plan tests => 19;

my $PODVERSION = '0.95';
eval {
    require Test::Pod;
    Test::Pod->import;
};

my @pm_files = qw{
Pg.pm
lib/Bundle/DBD/Pg.pm
t/lib/App/Info.pm
t/lib/App/Info/RDBMS/PostgreSQL.pm
t/lib/App/Info/Util.pm
t/lib/App/Info/Request.pm
t/lib/App/Info/Handler.pm
t/lib/App/Info/Handler/Prompt.pm
t/lib/App/Info/RDBMS.pm
};

SKIP: {
    if ($@ or $Test::Pod::VERSION < $PODVERSION) {
        skip ("Test::Pod $PODVERSION is required", 9);
    }
    for my $filename (@pm_files) {
        pod_file_ok($filename);
    }
}

## We won't require everyone to have this, so silently move on if not found
my $PODCOVERVERSION = '1.04';
eval {
    require Test::Pod::Coverage;
    Test::Pod::Coverage->import;
};
SKIP: {

    if ($@ or $Test::Pod::Coverage::VERSION < $PODCOVERVERSION) {
        skip ("Test::Pod::Coverage $PODCOVERVERSION is required", 1);
    }

    my $trusted_names  =
        [
         qr{^CLONE$},
         qr{^driver$},
         qr{^constant$},
         ## Auto-generated from types.c:
         qr{PG_ACLITEM},
         qr{PG_ACLITEMARRAY},
         qr{PG_ANY},
         qr{PG_ANYARRAY},
         qr{PG_ANYELEMENT},
         qr{PG_ANYENUM},
         qr{PG_ANYNONARRAY},
         qr{PG_ANYRANGE},
         qr{PG_BIT},
         qr{PG_BITARRAY},
         qr{PG_BOOL},
         qr{PG_BOOLARRAY},
         qr{PG_BOX},
         qr{PG_BOXARRAY},
         qr{PG_BPCHAR},
         qr{PG_BPCHARARRAY},
         qr{PG_BYTEA},
         qr{PG_BYTEAARRAY},
         qr{PG_CHAR},
         qr{PG_CHARARRAY},
         qr{PG_CID},
         qr{PG_CIDARRAY},
         qr{PG_CIDR},
         qr{PG_CIDRARRAY},
         qr{PG_CIRCLE},
         qr{PG_CIRCLEARRAY},
         qr{PG_CSTRING},
         qr{PG_CSTRINGARRAY},
         qr{PG_DATE},
         qr{PG_DATEARRAY},
         qr{PG_DATERANGE},
         qr{PG_DATERANGEARRAY},
         qr{PG_EVENT_TRIGGER},
         qr{PG_FDW_HANDLER},
         qr{PG_FLOAT4},
         qr{PG_FLOAT4ARRAY},
         qr{PG_FLOAT8},
         qr{PG_FLOAT8ARRAY},
         qr{PG_GTSVECTOR},
         qr{PG_GTSVECTORARRAY},
         qr{PG_INDEX_AM_HANDLER},
         qr{PG_INET},
         qr{PG_INETARRAY},
         qr{PG_INT2},
         qr{PG_INT2ARRAY},
         qr{PG_INT2VECTOR},
         qr{PG_INT2VECTORARRAY},
         qr{PG_INT4},
         qr{PG_INT4ARRAY},
         qr{PG_INT4RANGE},
         qr{PG_INT4RANGEARRAY},
         qr{PG_INT8},
         qr{PG_INT8ARRAY},
         qr{PG_INT8RANGE},
         qr{PG_INT8RANGEARRAY},
         qr{PG_INTERNAL},
         qr{PG_INTERVAL},
         qr{PG_INTERVALARRAY},
         qr{PG_JSON},
         qr{PG_JSONARRAY},
         qr{PG_JSONB},
         qr{PG_JSONBARRAY},
         qr{PG_JSONPATH},
         qr{PG_JSONPATHARRAY},
         qr{PG_LANGUAGE_HANDLER},
         qr{PG_LINE},
         qr{PG_LINEARRAY},
         qr{PG_LSEG},
         qr{PG_LSEGARRAY},
         qr{PG_MACADDR},
         qr{PG_MACADDR8},
         qr{PG_MACADDR8ARRAY},
         qr{PG_MACADDRARRAY},
         qr{PG_MONEY},
         qr{PG_MONEYARRAY},
         qr{PG_NAME},
         qr{PG_NAMEARRAY},
         qr{PG_NUMERIC},
         qr{PG_NUMERICARRAY},
         qr{PG_NUMRANGE},
         qr{PG_NUMRANGEARRAY},
         qr{PG_OID},
         qr{PG_OIDARRAY},
         qr{PG_OIDVECTOR},
         qr{PG_OIDVECTORARRAY},
         qr{PG_OPAQUE},
         qr{PG_PATH},
         qr{PG_PATHARRAY},
         qr{PG_PG_ATTRIBUTE},
         qr{PG_PG_CLASS},
         qr{PG_PG_DDL_COMMAND},
         qr{PG_PG_DEPENDENCIES},
         qr{PG_PG_LSN},
         qr{PG_PG_LSNARRAY},
         qr{PG_PG_MCV_LIST},
         qr{PG_PG_NDISTINCT},
         qr{PG_PG_NODE_TREE},
         qr{PG_PG_PROC},
         qr{PG_PG_TYPE},
         qr{PG_POINT},
         qr{PG_POINTARRAY},
         qr{PG_POLYGON},
         qr{PG_POLYGONARRAY},
         qr{PG_RECORD},
         qr{PG_RECORDARRAY},
         qr{PG_REFCURSOR},
         qr{PG_REFCURSORARRAY},
         qr{PG_REGCLASS},
         qr{PG_REGCLASSARRAY},
         qr{PG_REGCONFIG},
         qr{PG_REGCONFIGARRAY},
         qr{PG_REGDICTIONARY},
         qr{PG_REGDICTIONARYARRAY},
         qr{PG_REGNAMESPACE},
         qr{PG_REGNAMESPACEARRAY},
         qr{PG_REGOPER},
         qr{PG_REGOPERARRAY},
         qr{PG_REGOPERATOR},
         qr{PG_REGOPERATORARRAY},
         qr{PG_REGPROC},
         qr{PG_REGPROCARRAY},
         qr{PG_REGPROCEDURE},
         qr{PG_REGPROCEDUREARRAY},
         qr{PG_REGROLE},
         qr{PG_REGROLEARRAY},
         qr{PG_REGTYPE},
         qr{PG_REGTYPEARRAY},
         qr{PG_TABLE_AM_HANDLER},
         qr{PG_TEXT},
         qr{PG_TEXTARRAY},
         qr{PG_TID},
         qr{PG_TIDARRAY},
         qr{PG_TIME},
         qr{PG_TIMEARRAY},
         qr{PG_TIMESTAMP},
         qr{PG_TIMESTAMPARRAY},
         qr{PG_TIMESTAMPTZ},
         qr{PG_TIMESTAMPTZARRAY},
         qr{PG_TIMETZ},
         qr{PG_TIMETZARRAY},
         qr{PG_TRIGGER},
         qr{PG_TSM_HANDLER},
         qr{PG_TSQUERY},
         qr{PG_TSQUERYARRAY},
         qr{PG_TSRANGE},
         qr{PG_TSRANGEARRAY},
         qr{PG_TSTZRANGE},
         qr{PG_TSTZRANGEARRAY},
         qr{PG_TSVECTOR},
         qr{PG_TSVECTORARRAY},
         qr{PG_TXID_SNAPSHOT},
         qr{PG_TXID_SNAPSHOTARRAY},
         qr{PG_UNKNOWN},
         qr{PG_UUID},
         qr{PG_UUIDARRAY},
         qr{PG_VARBIT},
         qr{PG_VARBITARRAY},
         qr{PG_VARCHAR},
         qr{PG_VARCHARARRAY},
         qr{PG_VOID},
         qr{PG_XID},
         qr{PG_XIDARRAY},
         qr{PG_XML},
         qr{PG_XMLARRAY},

        ];

    my $t='DBD::Pg pod coverage okay';
    pod_coverage_ok ('DBD::Pg', {trustme => $trusted_names}, $t);
}

## Now some things that are not covered by the above tests

for my $filename (@pm_files) {
    open my $fh, '<', $filename or die qq{Could not open "$filename": $!\n};
    while (<$fh>) {
        last if /^=/;
    }
    next if ! defined $_; ## no critic
    ## Assume the rest is POD.
    my $passed = 1;
    while (<$fh>) {
        if (/C<[^<].+[<>].+[^>]>\b/) {
            $passed = 0;
            diag "Failed POD escaping on line $. of $filename\n";
            diag $_;
        }
    }
    close $fh or warn qq{Could not close "$filename": $!\n};
    if ($passed) {
        pass ("File $filename has no POD errors");
    }
    else {
        fail ("File $filename had at least one POD error");
    }
}
