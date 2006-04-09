use strict;
use Test::More tests => 28;

use DBD::Pg qw(:pg_types);

## Should match the list in Pg.xs

ok(PG_BOOL      == 16,   'PG_BOOL returns a correct value');
ok(PG_BYTEA     == 17,   'PG_BYTEA returns a correct value');

ok(PG_INT2      == 21,   'PG_INT2 returns a correct value');
ok(PG_INT4      == 23,   'PG_INT4 returns a correct value');
ok(PG_INT8      == 20,   'PG_INT8 returns a correct value');
ok(PG_FLOAT4    == 700,  'PG_FLOAT4 returns a correct value');
ok(PG_FLOAT8    == 701,  'PG_FLOAT8 returns a correct value');

ok(PG_BPCHAR    == 1042, 'PG_BPCHAR returns a correct value');
ok(PG_CHAR      == 18,   'PG_CHAR returns a correct value');
ok(PG_VARCHAR   == 1043, 'PG_VARCHAR returns a correct value');
ok(PG_TEXT      == 25,   'PG_TEXT returns a correct value');

ok(PG_ABSTIME   == 702,  'PG_ABSTIME returns a correct value');
ok(PG_RELTIME   == 703,  'PG_RELTIME returns a correct value');
ok(PG_TINTERVAL == 704,  'PG_TINTERVAL returns a correct value');
ok(PG_DATE      == 1082, 'PG_DATE returns a correct value');
ok(PG_TIME      == 1083, 'PG_TIME returns a correct value');
ok(PG_DATETIME  == 1184, 'PG_DATETIME returns a correct value');
ok(PG_TIMESPAN  == 1186, 'PG_TIMESPAN returns a correct value');
ok(PG_TIMESTAMP == 1296, 'PG_TIMESTAMP returns a correct value');

ok(PG_POINT     == 600,  'PG_PONT returns a correct value');
ok(PG_LINE      == 628,  'PG_LINE returns a correct value');
ok(PG_LSEG      == 601,  'PG_LSEG returns a correct value');
ok(PG_BOX       == 603,  'PG_BOX returns a correct value');
ok(PG_PATH      == 602,  'PG_PATH returns a correct value');
ok(PG_POLYGON   == 604,  'PG_POLYGON returns a correct value');
ok(PG_CIRCLE    == 718,  'PG_CIRCLE returns a correct value');

ok(PG_OID       == 26,   'PG_OID returns a correct value');
ok(PG_TID       == 27,   'PG_TID returns a correct value');

