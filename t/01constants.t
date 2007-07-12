use strict;
use Test::More tests => 31;

use DBD::Pg qw(:pg_types :async);

## Should match the list in Pg.xs

is(PG_BOOL      , 16,   'PG_BOOL returns a correct value');
is(PG_BYTEA     , 17,   'PG_BYTEA returns a correct value');

is(PG_INT2      , 21,   'PG_INT2 returns a correct value');
is(PG_INT4      , 23,   'PG_INT4 returns a correct value');
is(PG_INT8      , 20,   'PG_INT8 returns a correct value');
is(PG_FLOAT4    , 700,  'PG_FLOAT4 returns a correct value');
is(PG_FLOAT8    , 701,  'PG_FLOAT8 returns a correct value');

is(PG_BPCHAR    , 1042, 'PG_BPCHAR returns a correct value');
is(PG_CHAR      , 18,   'PG_CHAR returns a correct value');
is(PG_VARCHAR   , 1043, 'PG_VARCHAR returns a correct value');
is(PG_TEXT      , 25,   'PG_TEXT returns a correct value');

is(PG_ABSTIME   , 702,  'PG_ABSTIME returns a correct value');
is(PG_RELTIME   , 703,  'PG_RELTIME returns a correct value');
is(PG_TINTERVAL , 704,  'PG_TINTERVAL returns a correct value');
is(PG_DATE      , 1082, 'PG_DATE returns a correct value');
is(PG_TIME      , 1083, 'PG_TIME returns a correct value');
is(PG_DATETIME  , 1184, 'PG_DATETIME returns a correct value');
is(PG_TIMESPAN  , 1186, 'PG_TIMESPAN returns a correct value');
is(PG_TIMESTAMP , 1296, 'PG_TIMESTAMP returns a correct value');

is(PG_POINT     , 600,  'PG_PONT returns a correct value');
is(PG_LINE      , 628,  'PG_LINE returns a correct value');
is(PG_LSEG      , 601,  'PG_LSEG returns a correct value');
is(PG_BOX       , 603,  'PG_BOX returns a correct value');
is(PG_PATH      , 602,  'PG_PATH returns a correct value');
is(PG_POLYGON   , 604,  'PG_POLYGON returns a correct value');
is(PG_CIRCLE    , 718,  'PG_CIRCLE returns a correct value');

is(PG_OID       , 26,   'PG_OID returns a correct value');
is(PG_TID       , 27,   'PG_TID returns a correct value');

is(PG_ASYNC,           1, 'PG_ASYNC returns a correct value');
is(PG_OLDQUERY_CANCEL, 2, 'PG_OLDQUERY_CANCEL returns a correct value');
is(PG_OLDQUERY_WAIT,   4, 'PG_OLDQUERY_WAIT returns a correct value');

