#!perl

## Test that our SIGNATURE file is valid - requires TEST_SIGNATURE env

use strict;
use warnings;
use Test::More tests => 1;
select(($|=1,select(STDERR),$|=1)[1]);

if (!$ENV{TEST_SIGNATURE}) {
	plan skip_all => 'Set the environment variable TEST_SIGNATURE to enable this test';
}
elsif (!eval { require Module::Signature; 1 }) {
	fail 'Could not find Module::Signature';
}
elsif ( !-e 'SIGNATURE' ) {
	fail 'SIGNATURE file was not found';
}
elsif ( ! -s 'SIGNATURE') {
	fail 'SIGNATURE file was empty';
}
else {
	my $ret = Module::Signature::verify();
	if ($ret eq Module::Signature::SIGNATURE_OK()) {
		pass 'Valid SIGNATURE file';
	}
	else {
		fail 'Invalid SIGNATURE file';
	}
}
