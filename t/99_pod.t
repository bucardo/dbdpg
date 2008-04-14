#!perl

## Check our Pod, requires Test::Pod

use strict;
use warnings;
use Test::More;
select(($|=1,select(STDERR),$|=1)[1]);

plan tests => 3;

my $PODVERSION = '0.95';
eval {
	require Test::Pod;
	Test::Pod->import;
};

SKIP: {
	if ($@ or $Test::Pod::VERSION < $PODVERSION) {
		skip "Test::Pod $PODVERSION is required", 2;
	}
	pod_file_ok('Pg.pm');
	pod_file_ok('lib/Bundle/DBD/Pg.pm');
}

## We won't require everyone to have this, so silently move on if not found
my $PODCOVERVERSION = '1.04';
eval {
	require Test::Pod::Coverage;
	Test::Pod::Coverage->import;
};
SKIP: {

	if ($@ or $Test::Pod::Coverage::VERSION < $PODCOVERVERSION) {
		skip "Test::Pod::Coverage $PODCOVERVERSION is required", 1;
	}

	my $trusted_names  =
		[
		 qr{^CLONE$},
		 qr{^driver$},
		 qr{^constant$},
		 ## Auto-generated from types.c:
		 qr{PG_ABSTIME},
		 qr{PG_ABSTIMEARRAY},
		 qr{PG_ACLITEM},
		 qr{PG_ACLITEMARRAY},
		 qr{PG_ANY},
		 qr{PG_ANYARRAY},
		 qr{PG_ANYELEMENT},
		 qr{PG_ANYENUM},
		 qr{PG_ANYNONARRAY},
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
		 qr{PG_FLOAT4},
		 qr{PG_FLOAT4ARRAY},
		 qr{PG_FLOAT8},
		 qr{PG_FLOAT8ARRAY},
		 qr{PG_GTSVECTOR},
		 qr{PG_GTSVECTORARRAY},
		 qr{PG_INET},
		 qr{PG_INETARRAY},
		 qr{PG_INT2},
		 qr{PG_INT2ARRAY},
		 qr{PG_INT2VECTOR},
		 qr{PG_INT2VECTORARRAY},
		 qr{PG_INT4},
		 qr{PG_INT4ARRAY},
		 qr{PG_INT8},
		 qr{PG_INT8ARRAY},
		 qr{PG_INTERNAL},
		 qr{PG_INTERVAL},
		 qr{PG_INTERVALARRAY},
		 qr{PG_LANGUAGE_HANDLER},
		 qr{PG_LINE},
		 qr{PG_LINEARRAY},
		 qr{PG_LSEG},
		 qr{PG_LSEGARRAY},
		 qr{PG_MACADDR},
		 qr{PG_MACADDRARRAY},
		 qr{PG_MONEY},
		 qr{PG_MONEYARRAY},
		 qr{PG_NAME},
		 qr{PG_NAMEARRAY},
		 qr{PG_NUMERIC},
		 qr{PG_NUMERICARRAY},
		 qr{PG_OID},
		 qr{PG_OIDARRAY},
		 qr{PG_OIDVECTOR},
		 qr{PG_OIDVECTORARRAY},
		 qr{PG_OPAQUE},
		 qr{PG_PATH},
		 qr{PG_PATHARRAY},
		 qr{PG_PG_ATTRIBUTE},
		 qr{PG_PG_CLASS},
		 qr{PG_PG_PROC},
		 qr{PG_PG_TYPE},
		 qr{PG_POINT},
		 qr{PG_POINTARRAY},
		 qr{PG_POLYGON},
		 qr{PG_POLYGONARRAY},
		 qr{PG_RECORD},
		 qr{PG_REFCURSOR},
		 qr{PG_REFCURSORARRAY},
		 qr{PG_REGCLASS},
		 qr{PG_REGCLASSARRAY},
		 qr{PG_REGCONFIG},
		 qr{PG_REGCONFIGARRAY},
		 qr{PG_REGDICTIONARY},
		 qr{PG_REGDICTIONARYARRAY},
		 qr{PG_REGOPER},
		 qr{PG_REGOPERARRAY},
		 qr{PG_REGOPERATOR},
		 qr{PG_REGOPERATORARRAY},
		 qr{PG_REGPROC},
		 qr{PG_REGPROCARRAY},
		 qr{PG_REGPROCEDURE},
		 qr{PG_REGPROCEDUREARRAY},
		 qr{PG_REGTYPE},
		 qr{PG_REGTYPEARRAY},
		 qr{PG_RELTIME},
		 qr{PG_RELTIMEARRAY},
		 qr{PG_SMGR},
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
		 qr{PG_TINTERVAL},
		 qr{PG_TINTERVALARRAY},
		 qr{PG_TRIGGER},
		 qr{PG_TSQUERY},
		 qr{PG_TSQUERYARRAY},
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
	pod_coverage_ok('DBD::Pg', {trustme => $trusted_names}, 'DBD::Pg pod coverage okay');
}
