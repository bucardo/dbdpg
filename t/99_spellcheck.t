#!perl

## Spellcheck as much as we can
## Requires TEST_SPELL to be set

use strict;
use warnings;
use Test::More;
select(($|=1,select(STDERR),$|=1)[1]);

my (@testfiles, $fh);

if (!$ENV{TEST_SPELL}) {
	plan skip_all => 'Set the environment variable TEST_SPELL to enable this test';
}
elsif (!eval { require Text::SpellChecker; 1 }) {
	plan skip_all => 'Could not find Text::SpellChecker';
}
else {
	opendir my $dir, 't' or die qq{Could not open directory 't': $!\n};
	@testfiles = map { "t/$_" } grep { /^.+\.(t|pl)$/ } readdir $dir;
	closedir $dir or die qq{Could not closedir "$dir": $!\n};
	plan tests => 18+@testfiles;
}

my %okword;
my $file = 'Common';
while (<DATA>) {
	if (/^## (.+):/) {
		$file = $1;
		next;
	}
	next if /^#/ or ! /\w/;
	for (split) {
		$okword{$file}{$_}++;
	}
}


sub spellcheck {
	my ($desc, $text, $file) = @_;
	my $check = Text::SpellChecker->new(text => $text);
	my %badword;
	while (my $word = $check->next_word) {
		next if $okword{Common}{$word} or $okword{$file}{$word};
		$badword{$word}++;
	}
	my $count = keys %badword;
	if (! $count) {
		pass ("Spell check passed for $desc");
		return;
	}
	fail ("Spell check failed for $desc. Bad words: $count");
	for (sort keys %badword) {
		diag "$_\n";
	}
	return;
}


## First, the plain ol' textfiles
for my $file (qw/README Changes TODO README.dev README.win32/) {
	if (!open $fh, '<', $file) {
		fail (qq{Could not find the file "$file"!});
	}
	else {
		{ local $/; $_ = <$fh>; }
		close $fh or warn qq{Could not close "$file": $!\n};
		if ($file eq 'Changes') {
			s{\b(?:from|by) [A-Z][\w \.]+[<\[\n]}{}gs;
			s{\b[Tt]hanks to ([A-Z]\w+\W){1,3}}{}gs;
			s{Abhijit Menon-Sen}{}gs;
			s{eg/lotest.pl}{};
			s{\[.+?\]}{}gs;
			s{\S+\@\S+\.\S+}{}gs;
		}
		elsif ($file eq 'README.dev') {
			s/^\t\$.+//gsm;
		}
		spellcheck ($file => $_, $file);
	}
}

## Now the embedded POD
SKIP: {
	if (!eval { require Pod::Spell; 1 }) {
		skip ('Need Pod::Spell to test the spelling of embedded POD', 2);
	}

	for my $file (qw{Pg.pm lib/Bundle/DBD/Pg.pm}) {
		if (! -e $file) {
			fail (qq{Could not find the file "$file"!});
		}
		my $string = qx{podspell $file};
		spellcheck ("POD from $file" => $string, $file);
	}
}

## Now the comments
SKIP: {
	if (!eval { require File::Comments; 1 }) {
		skip ('Need File::Comments to test the spelling inside comments', 2);
	}
    {
		## For XS files...
		package File::Comments::Plugin::Catchall; ## no critic
		use strict;
		use warnings;
		require File::Comments::Plugin;
		require File::Comments::Plugin::C;

		our @ISA     = qw(File::Comments::Plugin::C);

		sub applicable {
			my($self) = @_;
			return 1;
		}
	}


	my $fc = File::Comments->new();

	my @files;
	for (sort @testfiles) {
		push @files, "$_";
	}


	for my $file (@testfiles, qw{Makefile.PL Pg.xs Pg.pm lib/Bundle/DBD/Pg.pm
        dbdimp.c dbdimp.h types.c quote.c quote.h Pg.h types.h}) {
		## Tests as well?
		if (! -e $file) {
			fail (qq{Could not find the file "$file"!});
		}
		my $string = $fc->comments($file);
		if (! $string) {
			fail (qq{Could not get comments from file $file});
			next;
		}
		$string = join "\n" => @$string;
		$string =~ s/=head1.+//sm;
		spellcheck ("comments from $file" => $string, $file);
	}


}


__DATA__
## These words are okay

## Common:

ActiveKids
adbin
adsrc
AIX
API
archlib
arith
arrayref
arrayrefs
async
attr
attrib
attribs
authtype
autocommit
AutoCommit
AutoEscape
backend
backtrace
bitmask
blib
bool
booleans
bools
bt
BUFSIZE
Bunce
bytea
BYTEA
CachedKids
cancelled
carlos
Checksum
checksums
Checksums
ChildHandles
chopblanks
ChopBlanks
cmd
CMD
CompatMode
conf
config
conformant
coredump
Coredumps
cpan
CPAN
cpansign
cperl
creat
CursorName
cvs
cx
datatype
Datatype
DATEOID
datetime
DATETIME
dbd
DBD
dbdimp
dbdpg
DBDPG
dbgpg
dbh
dbi
DBI
DBI's
dbivport
DBIXS
dbmethod
dbname
DDL
deallocate
Debian
decls
Deepcopy
dequote
dequoting
dev
devel
Devel
dHTR
dir
dirname
discon
distcheck
disttest
DML
dollaronly
dollarsign
dr
drh
DSN
dv
emacs
endcopy
enum
env
ENV
ErrCount
errorlevel
errstr
externs
ExtUtils
fe
FetchHashKeyName
fh
filename
FreeBSD
func
funct
GBorg
gcc
GCCDEBUG
gdb
getcopydata
getfd
getline
greg
GSM
HandleError
HandleSetErr
hashref
hba
html
http
ifdefs
ifndefs
InactiveDestroy
includedir
IncludingOptionalDependencies
initdb
inout
installarchlib
installsitearch
INSTALLVENDORBIN
IP
ish
Kbytes
largeobject
lcrypto
ld
LD
ldconfig
LDDFLAGS
len
lgcc
libpq
libpqswip
linux
LOBs
localhost
LongReadLen
LongTruncOk
lpq
LSEG
lssl
Makefile
MakeMaker
malloc
MCPAN
md
Mdevel
Mergl
metadata
mis
Momjian
Mullane
multi
ness
Newz
nntp
nonliteral
noprefix
noreturn
nosetup
notused
NULLABLE
num
numbound
ODBC
ODBCVERSION
oid
OID
onerow
param
params
ParamTypes
ParamValues
parens
passwd
patchlevel
pch
perl
perlcritic
perlcriticrc
perldocs
PGBOOLOID
PgBouncer
pgbuiltin
PGCLIENTENCODING
pgend
pglibpq
pglogin
PGP
PGPORT
pgprefix
PGRES
PGresult
pgsql
pgstart
pgtype
pgver
php
pid
PID
PlanetPostgresql
postgres
postgresql
PostgreSQL
powf
PQconnectdb
PQconsumeInput
PQexec
PQexecParams
PQexecPrepared
PQprepare
PQprotocolVersion
PQresultErrorField
PQsendQuery
PQserverVersion
PQsetErrorVerbosity
pragma
pragmas
pre
preparse
preparser
prepending
PrintError
PrintWarn
projdisplay
putcopydata
putcopyend
putline
pv
pwd
qual
qw
RaiseError
RDBMS
README
realclean
RedHat
relcheck
requote
RowCache
RowCacheSize
RowsInCache
runtime
Sabino
safemalloc
savepoint
savepoints
Savepoints
sbin
schemas
SCO
Sep
SGI
sha
ShowErrorStatement
sitearch
skipcheck
smethod
snprintf
Solaris
sprintf
sql
SQL
sqlstate
SQLSTATE
SSL
sslmode
stderr
STDERR
STDIN
STDOUT
sth
strcpy
strdup
strerror
stringification
strlen
STRLEN
strncpy
structs
submitnews
Sv
svn
tablename
tablespace
tablespaces
TaintIn
TaintOut
TCP
testdatabase
testname
tf
TID
TIMEOID
timestamp
TIMESTAMP
TIMESTAMPTZ
tmp
TMP
TODO
TraceLevel
tuple
TUPLES
turnstep
txn
typename
uid
undef
unix
UNKNOWNOID
userid
username
Username
usr
UTC
utf
UTF
Util
valgind
valgrind
varchar
VARCHAR
VARCHAROID
VER
versioning
veryverylongplaceholdername
Waggregate
Wbad
Wcast
Wchar
Wcomment
Wconversion
Wdeclaration
Wdisabled
weeklynews
Wendif
Wextra
wfile
Wfloat
Wformat
whitespace
Wimplicit
Winit
Winline
Winvalid
Wmain
Wmissing
Wnested
Wnonnull
Wpacked
Wpadded
Wparentheses
Wpointer
Wredundant
Wreturn
writeable
Wsequence
Wshadow
Wsign
Wstrict
Wswitch
Wsystem
Wtraditional
Wtrigraphs
Wundef
Wuninitialized
Wunknown
Wunreachable
Wunused
Wwrite
www
xs
xsi
xst
XSUB
yaml
YAML
yml

## TODO:
struct


## README.dev:
leaktester
mak
mathinline
nCompiled
nServer

## README:
BOOTCHECK
DynaLoader
Eisentraut
Gcc
Landgren
Lauterbach
cabrion
conf
danla
david
drnoble
engsci
fe
freenode
gmx
hdf
irc
jmore
landgren
pc
sandia
sco
sl
sv
turnstep

## Changes:
destringifying
spellcheck
gborg
n's
Pg's
qw

## Pg.pm:
lseg
afterwards

## Pg.xs:
PQexec
struct

## dbdimp.c:
ABCD
AvARRAY
BegunWork
COPYing
Deallocate
Deallocation
ExecStatusType
INV
NULLs
Oid
PGRES
backend's
backslashed
boolean
cancelling
copypv
copystate
currph
currpos
dashdash
deallocating
defaultval
delim
dereference
destringify
dollarquote
dollarstring
firstword
inerror
login
mortalize
myval
n'egative
nullable
numphs
numrows
ok
p'ositive
paramTypes
ph
preparable
proven
quickexec
recv'd
reprepare
repreparing
req
scs
sectionstop
slashslash
slashstar
starslash
stringify
sv
topav
topdollar
tuples
typedef
unescaped
untrace
versa
xPID

## dbdimp.h:
SSP
funcs
implementor
ph

## quote.c:
estring
SVs
compat

## types.c:
ASYNC
Autogenerate
BIGINT
BOOLOID
DESCR
LONGVARCHAR
OLDQUERY
PASSBYVAL
SMALLINT
SV
TINYINT
VARBINARY
arg
arrayout
binin
binout
boolout
bpchar
chr
cid
cmp
delim
delimeter
dq
elsif
lseg
maxlen
mv
newfh
oct
ok
oldfh
pos
printf
qq
slashstar
starslash
sqlc
sqltype
src
struct
svtype
tcase
tdTHX
tdefault
textin
textout
thisname
tid
timestamptz
tswitch
typarray
typedef
typelem
typrelid
uc

## types.h:
Nothing

## Pg.h:
preprocessors

## Makefile.PL:
prereqs

## t/07copy.t:
copystate

## t/03dbmethod.t:
CamelCase
Multi
arrayref
fk
fktable
intra
odbcversion
pktable
selectall
selectcol
selectrow

## t/01constants.t:
RequireUseWarnings
TSQUERY

## t/99_yaml.t:
YAMLiciousness

## t/02attribs.t:
INV
NUM
PARAMS
encodings
lc
uc
unicode

## t/03smethod.t:
ArrayTupleFetch
SSP
arg
fetchall
fetchrow
undefs

## t/12placeholders.t:
encodings
https

## t/99_spellcheck.t:
Spellcheck
ol
textfiles

## README.win32:
DRV
ITHREADS
MSVC
MULTI
SYS
VC
Vc
cd
exe
gz
libpg
mak
mkdir
myperl
nmake
rc
src
vcvars
xcopy
GF
Gf
ITHREADS
MSVC
MULTI
SYS
VC
Vc
cd
exe
gz
libpg
mak
mkdir
myperl
nmake
rc
src
vcvars
xcopy
