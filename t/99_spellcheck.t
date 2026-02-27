#!perl

## Spell check as much as we can

use 5.008001;
use strict;
use warnings;
use Test::More;
use utf8; ## no critic (TooMuchCode::ProhibitUnnecessaryUTF8Pragma)
select(($|=1,select(STDERR),$|=1)[1]);

my (@testfiles, @alltestfiles, $fh);

if (! $ENV{AUTHOR_TESTING}) {
    plan (skip_all =>  'Test skipped unless environment variable AUTHOR_TESTING is set');
}
elsif (!eval { require Text::SpellChecker; 1 }) {
    plan skip_all => 'Could not find Text::SpellChecker'; ## nospellcheck
}
else {
    opendir my $dir, 't' or die qq{Could not open directory 't': $!\n};
    @testfiles = map { "t/$_" } grep { ! /spellcheck|lint/ } grep { /^.+\.(t|pl)$/ } readdir $dir;
    rewinddir $dir;
    @alltestfiles = map { "t/$_" } grep { /^.+\.(t|pl)$/ } readdir $dir;
    closedir $dir or die qq{Could not closedir "$dir": $!\n};
}

my %okword;
while (<DATA>) {
    next if /^#/ or ! /\w/;
    for (split) {
        $okword{$_}++;
    }
}


sub spellcheck {

    my ($item, $text, $filename, $summarize) = @_;

    $summarize ||= 0;

    my $check = Text::SpellChecker->new(text => $text, lang => 'en_US');
    my %badword;
    while (my $word = $check->next_word) {
        next if $okword{$word};
        $badword{$word}++;
    }
    my $count = keys %badword;
    if ($summarize) {
        return join ' ' => sort keys %badword;
    }

    if (! $count) {
        pass ("Spell check passed for $item");
        return 0;
    }
    fail ("Spell check failed for $item. Bad words: $count");
    for (sort keys %badword) {
        diag "$_\n";
    }
    return $count;
}


## The test names
my $testmorewords = qr{(?:is|ok|cmp_ok|isa_ok|isnt|like|unlike|pass|fail|skip|skip_all|is_deeply|diag|BAIL_OUT)};
for my $file (@alltestfiles) {

    next if $file =~ /dbdpg_test_setup/;

    if (!open $fh, '<', $file) {
        fail (qq{Could not find the file "$file"!});
        next;
    }

    binmode($fh, ':encoding(UTF-8)');
    my $line = 0;
    my $failures = 0;
    while (<$fh>) {
        $line++;
        next if / nospellcheck/;
        my $string = '';
        if (/\$t=q\{(.+)\}/ or /\$t=['"](.+)['"]/) {
            $string = $1;
        }
        elsif (/\b$testmorewords\b.+?(['"])(.+)\1/) {
            $string = $2;
        }

        next unless $string;

        next if $string eq 'eq';

        $string =~ s{\\t}{ }g;
        $string =~ s{\\n}{ }g;

        $string =~ s/casetest/ /gi;
        my $value = spellcheck("$file" => $string, $file, 1);

        if (length $value) {
            diag "test name from $file at line $line: $value";
            $failures++;
        }


    }

    if ($failures) {
        fail ("Spell check failed for strings in test file $file");
     }
    else {
        pass ("Spell check passed for strings in test file $file");
    }

}

## The plain ol' textfiles
for my $file (qw/README Changes TODO README.dev README.win32 CONTRIBUTING.md/) {

    if (!open $fh, '<', $file) {
        fail (qq{Could not find the file "$file"!});
    }
    else {
        binmode($fh, ':encoding(UTF-8)');
        { local $/; $_ = <$fh>; }
        close $fh or warn qq{Could not close "$file": $!\n};
        if ($file eq 'Changes') {
            ## Too many proper names to worry about here:
            s{\[.+?\]}{}gs;
            s{\b[Tt]hanks to (?:[A-Za-z]\w+\W){1,3}}{}gs;
            s{\bpatch from (?:[A-Z]\w+\W){1,3}}{}gs;
            s{\b[Rr]eported by (?:[A-Z]\w+\W){1,3}}{}gs;
            s{\breport from (?:[A-Z]\w+\W){1,3}}{}gs;
            s{\b[Ss]uggested by (?:[A-Z]\w+\W){1,3}}{}gs;
            s{\bSpotted by (?:[A-Z]\w+\W){1,3}}{}gs;

            ## Emails are not going to be in dictionaries either:
            s{<.+?>}{}gs;

        }
        elsif ($file eq 'README.dev') {
            s/^\t\$.+//gsm;
        }
        spellcheck ($file => $_, $file);
    }
}

## The embedded POD
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

## The comments
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
        dbdimp.c dbdimp.h quote.c quote.h Pg.h types.h dbdpg_test_postgres_versions.pl}) {
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


done_testing();


__DATA__
## These words are okay

abc
ABCD
ActiveKids
adbin
adsrc
AIX
alphanum
archlib
arg
args
arith
arrayout
arrayref
arrayrefs
ArrayTupleFetch
ASC
ascii
async
ASYNC
Async
attr
attrib
attribs
authtype
autocommit
AutoCommit
autodie
AutoEscape
Autogenerate
AutoInactiveDestroy
AvARRAY
Backcountry
backend
backend's
backslashed
backtrace
baldrick
basename
BegunWork
bigint
BIGINT
bitmask
blib
BMP
bool
boolean
booleans
boolout
bools
BOOTCHECK
bpchar
bt
bucardo
BUFSIZE
Bunce
bytea
Bytea
BYTEA
cabrion
CachedKids
CamelCase
cancelled
cancelling
CARDINALITY
carlos
cd
CentOS
Checksum
checksums
Checksums
ChildHandles
chopblanks
ChopBlanks
chr
cid
CMD
cmd
cmdtaglist
cmp
compat
CompatMode
conf
config
conformant
consrc
const
Conway's
copydata
COPYing
copypv
copystate
coredump
coredumps
Coredumps
cpan
CPAN
cpansearch
cpansign
cpantesters
cperl
cryptographically
currph
currpos
CursorName
cvs
cx
danla
darwin
dashdash
dat
datatype
Datatype
DATEOID
datetime
david
dbd
DBD
dbdimp
dbdpg
DBDPG
dbgpg
dbh
dbi
DBI
DBIc
DBICTEST
DBILOGFP
DBIS
dbivport
dbix
DBIx
DBIXS
dbmethod
dbname
DDL
deallocate
deallocates
Deallocate
DEALLOCATE
deallocating
deallocation
Deallocation
Debian
decls
Deepcopy
defaultval
DefaultValue
delim
dequote
dequoting
dereference
descr
DESCR
destringify
destringifying
dev
devel
Devel
dHTR
dir
dirname
discon
distcheck
disttest
dll
dllname
dlltool
dmake
DML
dollaronly
dollarquote
dollarsign
dollarstring
downcase
DProf
dprofpp
dq
dr
drh
drnoble
DRV
DSN
dTHX
dv
DYLD
dylib
DynaLoader
edmund
eg
Eisentraut
el
elsif
emacs
endcopy
engsci
EnterpriseDB
enum
env
ENV
ErrCount
errorlevel
errstr
estring
eval
exe
ExecStatusType
externs
EXTRALIBS
ExtUtils
fallthrough
fe
fetchall
FetchHashKeyName
fetchrow
fh
filename
firstword
fk
FreeBSD
fulltest
func
funcs
funct
gborg
GBorg
gcc
Gcc
gdb
ge
getcom
getcopydata
getfd
getline
Gf
GF
GH
github
Github
gmx
gotlist
goto
GPG
gpl
GPL
greg
grokbase
gz
HandleError
HandleSetErr
hashref
hashrefs
hba
hdf
hstore
html
http
https
ifdefs
implementor
InactiveDestroy
includedir
IncludingOptionalDependencies
inerror
initdb
init
inout
installarchlib
installsitearch
INSTALLVENDORBIN
intra
ints
INV
IP
IRC
irc
ish
ITHREADS
jmore
json
JSON
jsonb
jsontable
Kbytes
kwlist
landgren
Landgren
largeobject
largeobjects
Lauterbach
lc
lcrypto
ld
LD
ldconfig
LDDFLAGS
leaktester
LEFTARG
len
lgcc
libera
libpg
libpq
linux
LOBs
localhost
localtime
login
LongReadLen
LongTruncOk
LONGVARCHAR
lotest
lowercased
lpq
lseg
LSEG
lsegs
lssl
lt
mak
Makefile
MAKEFILE
MakeMaker
malloc
maxlen
maxrows
MaxRows
MCPAN
md
MDevel
Mergl
metacpan
metadata
mingw
MinGW
minversion
mis
mkdir
Momjian
mortalize
msg
MSVC
Mullane
multi
Multi
MULTI
MYMETA
myperl
myval
Compiled
ndone
ne
ness
netstat
newfh
newSVpv
Newz
nmake
nntp
nohead
nonliteral
noprefix
noreturn
nosetup
NOSUCH
Server
nullable
NULLABLE
NULLs
num
NUM
numbound
numphs
NYTProf
nytprofhtml
ocitrace
oct
ODBC
odbcversion
ODBCVERSION
ofile
oid
Oid
OID
oids
OIDS
ok
oldfh
OLDQUERY
onerow
onwards
optimizations
osdn
param
params
PARAMS
ParamTypes
ParamValues
parens
ParseData
ParseHeader
PASSBYVAL
passwd
patchlevel
pc
pch
perl
perlcritic
perlcriticrc
perldocs
Perlish
perls
pexports
PGBOOLOID
pgbouncer
PgBouncer
pgBouncer
PGCLIENTENCODING
PGDATABASE
pgend
PGfooBar
PGINITDB
pglibpq
pglogin
pgp
PGP
PGPORT
pgprefix
PGRES
PGresult
PGSERVICE
PGSERVICEFILE
pgsql
pgstart
PGSYSCONFDIR
pgtype
pgver
ph
php
pid
PID
pos
POSIX
postgres
Postgres
POSTGRES
postgresdir
postgresql
PostgreSQL
postgresteam
powf
PQ
PQchangePassword
PQclear
PQclosePrepared
PQconnectdb
PQconnectPoll
PQconnectStart
PQconsumeInput
PQerrorMessage
PQexec
PQexecParams
PQexecPrepared
PQoids
PQprepare
PQprotocolVersion
PQresultErrorField
PQsend
PQsendPrepare
PQsendQuery
PQsendQueryParams
PQsendQueryPrepared
PQserverVersion
PQsetErrorVerbosity
PQsetSingleRowMode
PQstatus
pqtype
PQvals
pragma
pragmas
pre
preparable
preparse
preparser
prepending
preprocessors
prereqs
PrintError
printf
PrintWarn
profiler
PROGRA
projdisplay
proven
pseudotype
pTHX
pulldown
putcopydata
putcopyend
putline
pv
pwd
PYTHIAN
qq
qual
quickexec
qw
Rainer
RaiseError
rc
RDBMS
README
ReadOnly
realclean
recv'd
RedHat
Refactor
regex
reinstalling
relcheck
relkind
reltuples
repo
reprepare
repreparing
RequireUseWarnings
requote
rescan
resultset
RIGHTARG
ROK
RowCache
RowCacheSize
RowsInCache
rowtypes
Sabino
safemalloc
sandia
savepoint
savepoints
Savepoints
sbin
schemas
sco
SCO
scs
scsys
sectionstop
selectall
selectcol
selectrow
SGI
sha
shortid
ShowErrorStatement
sitearch
skipcheck
sl
slashstar
SMALLINT
smethod
snprintf
Solaris
spclocation
spellcheck
sprintf
sql
SQL
sqlc
sqlclient
sqlstate
SQLSTATE
sqltype
src
ss
SSL
sslmode
starslash
StartTransactionCommand
stderr
STDERR
STDIN
STDOUT
sth
strcmp
strcpy
strdup
strerror
stringifiable
stringification
stringify
strlen
STRLEN
strncpy
strtod
struct
structs
subdirectory
submitnews
substr
sudo
sv
Sv
SV
svn
SvNVs
SvPV
SVs
SvTRUE
svtype
SvUTF
SYS
tableinfo
tablename
tablespace
tablespaces
TaintIn
TaintOut
Tammer
tcop
TCP
tempfile
testdatabase
testdb
testfile
testme
testname
textout
tf
THEADER
thisname
tid
TID
TIMEOID
timestamp
TIMESTAMP
timestamptz
TIMESTAMPTZ
TINYINT
tmp
TMP
TMPDIR
TODO
topav
topdollar
TraceLevel
TSQUERY
tty
tuple
tuples
TUPLES
turnstep
txn
txt
typarray
typdelim
typedef
typefile
typelem
typename
typinput
typname
typoutput
typrecv
typrelid
typsend
Ubuntu
uc
uid
uk
undef
undefs
unescaped
unicode
UNKNOWNOID
unreferenced
untrace
userid
username
Username
usr
utf
UTF
Util
valgrind
vals
VARBINARY
varchar
VARCHAR
VARCHAROID
Vc
VC
vcvars
VER
versa
versioning
veryverylongplaceholdername
Waggregate
Wbad
Wcast
Wchar
Wcomment
Wconversion
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
xcopy
xPID
xs
xsi
XSLoader
xst
XSUB
XSubPPtmpAAAA
xxh
yaml
YAML
YAMLiciousness
yml
