#!perl

## Simply test that we can load the DBI and DBD::Pg modules
## If either fails, we bail out of all testing
## Check that DBD::Pg gives a sane VERSION number

use 5.008001;
use strict;
use warnings;
use lib 'blib/lib', 'blib/arch', 't';
use Test::More tests => 3;
select(($|=1,select(STDERR),$|=1)[1]);

BEGIN {

    use_ok ('DBI') or BAIL_OUT 'Cannot continue without DBI';

    ## If we cannot load DBD::Pg, output some compiler information for debugging:
    if (! use_ok ('DBD::Pg')) {
        my $file = 'Makefile';
        if (! -e $file) {
            $file = '../Makefile';
        }
        my $fh;
        if (open $fh, '<', $file) { ## no critic (CompileTime)
            { local $/; $_ = <$fh>; }
            close $fh or die qq{Could not close file "$file" $!\n}; ## no critic (CompileTime)
            for my $keyword (qw/ CCFLAGS INC LIBS /) {
                if (/^#\s+$keyword => (.+)/m) {
                    diag "$keyword: $1";
                }
            }
        }

        diag 'If the error mentions libpq.so, please see the troubleshooting section of the README file';

        BAIL_OUT 'Cannot continue without DBD::Pg';
    }
}
use DBD::Pg;
like ($DBD::Pg::VERSION, qr/^v?[0-9]+\.[0-9]+\.[0-9]+(?:_[0-9]+)?$/, qq{Found DBD::Pg::VERSION as "$DBD::Pg::VERSION"});
