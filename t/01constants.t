#!/usr/bin/perl -w

use strict;
use Test;

BEGIN { plan tests => 22 };

use DBD::Pg qw(:pg_types);

ok(1);

ok(PG_BOOL      == 16);
ok(PG_BOOL      == 16);
ok(PG_BYTEA     == 17);
ok(PG_CHAR      == 18);
ok(PG_INT8      == 20);
ok(PG_INT2      == 21);
ok(PG_INT4      == 23);
ok(PG_TEXT      == 25);
ok(PG_OID       == 26);
ok(PG_FLOAT4    == 700);
ok(PG_FLOAT8    == 701);
ok(PG_ABSTIME   == 702);
ok(PG_RELTIME   == 703);
ok(PG_TINTERVAL == 704);
ok(PG_BPCHAR    == 1042);
ok(PG_VARCHAR   == 1043);
ok(PG_DATE      == 1082);
ok(PG_TIME      == 1083);
ok(PG_DATETIME  == 1184);
ok(PG_TIMESPAN  == 1186);
ok(PG_TIMESTAMP == 1296);
