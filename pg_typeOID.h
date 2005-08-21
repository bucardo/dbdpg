/*

$Id$

To update this file, simply run:
perl -x pg_typeOID.h "path-to-pgsql-source"

#!perl

my $arg = shift || die "Usage: $0 path-to-pgsql-source\n";

-d $arg or die qq{Sorry, but "$arg" is not a directory!\n};

my $file = "$arg/src/include/catalog/pg_type.h";

open(F, $file) or die qq{Could not open file "$file": $!\n};
my %oid;
my $maxlen = 1;
while(<F>) {
	next unless /^#define\s+([A-Z0-9_]*OID)\s+(\d+)/o;
	$oid{$1} = $2;
	length($1) > $maxlen and $maxlen = length($1);
}
close(F);

my @self;
seek(DATA,0,0);
while(<DATA>) {
	push @self, $_;
	last if m#^\*\/#o;
}

open(SELF, ">$0") or die qq{Could not write to "$0": $!\n};
print SELF @self;
print SELF "\n\n";

## We sort alphabetically because it is easier to read that way,
## and we don't really care that much about the numbers
for (sort { $a cmp $b } keys %oid) {
	printf SELF "#define %${maxlen}s  $oid{$_}\n", $_;
}
close(SELF);

exit;
__DATA__
*/


#define               ABSTIMEOID  702
#define               ACLITEMOID  1033
#define              ANYARRAYOID  2277
#define            ANYELEMENTOID  2283
#define                   ANYOID  2276
#define                   BITOID  1560
#define                  BOOLOID  16
#define                   BOXOID  603
#define                BPCHAROID  1042
#define                 BYTEAOID  17
#define                  CASHOID  790
#define                  CHAROID  18
#define                   CIDOID  29
#define                  CIDROID  650
#define                CIRCLEOID  718
#define               CSTRINGOID  2275
#define                  DATEOID  1082
#define                FLOAT4OID  700
#define                FLOAT8OID  701
#define                  INETOID  869
#define                  INT2OID  21
#define            INT2VECTOROID  22
#define             INT4ARRAYOID  1007
#define                  INT4OID  23
#define                  INT8OID  20
#define              INTERNALOID  2281
#define              INTERVALOID  1186
#define      LANGUAGE_HANDLEROID  2280
#define                  LINEOID  628
#define                  LSEGOID  601
#define               MACADDROID  829
#define                  NAMEOID  19
#define               NUMERICOID  1700
#define                   OIDOID  26
#define             OIDVECTOROID  30
#define                OPAQUEOID  2282
#define                  PATHOID  602
#define PG_ATTRIBUTE_RELTYPE_OID  75
#define     PG_CLASS_RELTYPE_OID  83
#define      PG_PROC_RELTYPE_OID  81
#define      PG_TYPE_RELTYPE_OID  71
#define                 POINTOID  600
#define               POLYGONOID  604
#define                RECORDOID  2249
#define             REFCURSOROID  1790
#define              REGCLASSOID  2205
#define           REGOPERATOROID  2204
#define               REGOPEROID  2203
#define          REGPROCEDUREOID  2202
#define               REGPROCOID  24
#define               REGTYPEOID  2206
#define               RELTIMEOID  703
#define                  TEXTOID  25
#define                   TIDOID  27
#define                  TIMEOID  1083
#define             TIMESTAMPOID  1114
#define           TIMESTAMPTZOID  1184
#define                TIMETZOID  1266
#define             TINTERVALOID  704
#define               TRIGGEROID  2279
#define               UNKNOWNOID  705
#define                VARBITOID  1562
#define               VARCHAROID  1043
#define                  VOIDOID  2278
#define                   XIDOID  28
