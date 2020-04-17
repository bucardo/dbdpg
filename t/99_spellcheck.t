#!perl

## Spellcheck as much as we can

use 5.008001;
use strict;
use warnings;
use Test::More;
use utf8; ## no critic (TooMuchCode::ProhibitUnnecessaryUTF8Pragma)
select(($|=1,select(STDERR),$|=1)[1]);

my (@testfiles, $fh);

if (! $ENV{AUTHOR_TESTING}) {
    plan (skip_all =>  'Test skipped unless environment variable AUTHOR_TESTING is set');
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
    my ($desc, $text, $filename) = @_;
    my $check = Text::SpellChecker->new(text => $text, lang => 'en_US');
    my %badword;
    while (my $word = $check->next_word) {
        next if $okword{Common}{$word} or $okword{$filename}{$word};
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
            s{\b[Tt]hanks to (?:[A-Z]\w+\W){1,3}}{}gs;
            s{Abhijit Menon-Sen}{}gs;
            s{eg/lotest.pl}{};
            s{\[.+?\]}{}gs;
            s{\S+\@\S+\.\S+}{}gs;
            s{git commit [a-f0-9]+}{git commit}gs;
            s{B.*lint Szilakszi}{}gs;
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
        skip ('Need File::Comments to test the spelling inside comments', 11+@testfiles);
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
AutoInactiveDestroy
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
DBIx
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
eval
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
GH
github
greg
grokbase
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
JSON
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
machack
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
nullable
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
POSIX
postgres
Postgres
POSTGRES
postgresql
PostgreSQL
powf
PQclear
PQconnectdb
PQconsumeInput
PQexec
PQexecParams
PQexecPrepared
PQprepare
PQprotocolVersion
PQresultErrorField
PQsend
PQsendQuery
PQsendQueryParams
PQsendQueryPrepared
PQserverVersion
PQsetErrorVerbosity
PQsetSingleRowMode
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
quickexec
qw
RaiseError
RDBMS
README
ReadOnly
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
tempdir
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
unicode
unix
UNKNOWNOID
unowned
userid
username
Username
usr
UTC
utf
UTF
Util
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
ala
bucardo
cpantesters
github
goto
hashrefs
hstore
https
rowtypes
struct

## README.dev:
abc
bucardo
Conway's
cpantesters
dbix
distro
DProf
dprofpp
gpl
GPL
json
leaktester
mak
mathinline
MDevel
MYMETA
nCompiled
nServer
NYTProf
nytprofhtml
pgp
profiler
pulldown
repo
scsys
shortid
spellcheck
SvTRUE
testallversions
testfile
testme
txt
uk
XSubPPtmpAAAA

## README:
BOOTCHECK
bucardo
cabrion
CentOS
conf
cryptographically
danla
david
dll
dllname
dlltool
dmake
drnoble
DynaLoader
Eisentraut
engsci
exe
EXTRALIBS
fe
freenode
Gcc
gmx
hdf
https
irc
jmore
landgren
Landgren
Lauterbach
LDLOADLIBS
MAKEFILE
mingw
MinGW
osdn
pc
pexports
PROGRA
sandia
sco
sl
sv
testdb
turnstep
Ubuntu

## Changes:
alex
Bálint
bigint
BIGINT
BMP
boolean
bpchar
bucardo
destringifying
dTHX
easysoft
elspicyjack
evans
expr
Garamond
gborg
Github
gmail
Hofmann
ints
ivan
jsontable
Kai
largeobjects
localtime
lviv
marshalling
morni
nl
NOSUCH
n's
oids
optimizations
pc
Perlish
perls
PGINITDB
Pg's
Pieter
pilosoft
qw
rdovira
Refactor
regex
reinstalling
repo
signalling
spclocation
spellcheck
SvNVs
Szilakszi
tableinfo
ua
uc
VC

## Pg.pm:
afterwards
hashrefs
lseg
Oid
onwards
PGSERVICE
PGSYSCONFDIR
pseudotype

## Pg.xs:
PQexec
stringifiable
struct

## dbdimp.c:
ABCD
alphanum
AvARRAY
backend's
backslashed
BegunWork
boolean
Bytea
cancelling
COPYing
copypv
copystate
coredumps
currph
currpos
dashdash
DBIc
Deallocate
deallocating
Deallocation
defaultval
DefaultValue
dereference
destringify
dollarquote
dollarstring
encodings
ExecStatusType
fallthrough
firstword
getcom
inerror
INV
login
mortalize
myval
n'egative
nullable
NULLs
numphs
numrows
Oid
ok
paramTypes
Perlish
PGRES
ph
pos
p'ositive
PQoids
PQstatus
pqtype
PQvals
preparable
proven
quickexec
recv'd
reprepare
repreparing
req
rescan
resultset
ROK
scs
sectionstop
slashslash
slashstar
sqlclient
starslash
StartTransactionCommand
stringify
sv
SvPV
SvUTF
topav
topdollar
tuples
typedef
unescaped
untrace
versa
xPID

## dbdimp.h:
funcs
implementor
ph
PQ
SSP

## quote.c:
arg
Autogenerate
compat
downcase
elsif
estring
gotlist
kwlist
lsegs
maxlen
mv
ofile
qq
src
strcmp
strtof
SVs
tempfile

## types.c:
arg
arrayout
ASYNC
Autogenerate
basename
BIGINT
binin
binout
BOOLOID
boolout
bpchar
chr
cid
cmp
dat
delim
descr
DESCR
dq
elsif
LONGVARCHAR
lseg
maxlen
mv
ndone
newfh
oct
ok
oldfh
OLDQUERY
ParseData
ParseHeader
PASSBYVAL
pos
printf
qq
slashstar
SMALLINT
sqlc
sqltype
src
starslash
struct
SV
svtype
tcase
tdefault
tdTHX
textin
textout
thisname
tid
timestamptz
TINYINT
tswitch
typarray
typdelim
typedef
typefile
typelem
typinput
typname
typoutput
typrecv
typrelid
typsend
uc
VARBINARY

## types.h:
Nothing

## Pg.h:
cpansearch
DBILOGFP
DBIS
ocitrace
PGfooBar
preprocessors
PYTHIAN
src
THEADER
xxh

## Makefile.PL:
prereqs
subdirectory

## t/07copy.t:
copystate

## t/03dbmethod.t:
arrayref
CamelCase
fk
fktable
intra
Multi
odbcversion
pktable
selectall
selectcol
selectrow
untrace

## t/01constants.t:
RequireUseWarnings
TSQUERY

## t/99_yaml.t:
YAMLiciousness

## t/02attribs.t:
encodings
INV
lc
msg
NUM
PARAMS
uc

## t/03smethod.t:
arg
ArrayTupleFetch
fetchall
fetchrow
SSP
undefs

## t/12placeholders.t:
encodings
https
LEFTARG
RIGHTARG

## t/99_spellcheck.t:
gsm
ol
sm
Spellcheck
textfiles

## README.win32:
cd
cd
DRV
exe
exe
frs
Gf
GF
gz
gz
ITHREADS
ITHREADS
libpg
libpg
mak
mak
mkdir
mkdir
MSVC
MSVC
MULTI
MULTI
myperl
myperl
nmake
nmake
pgfoundry
rc
rc
src
src
SYS
SYS
Vc
Vc
VC
VC
vcvars
vcvars
xcopy
xcopy
