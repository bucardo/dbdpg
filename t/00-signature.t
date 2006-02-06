#!perl -w

## Test that our SIGNATURE file is valid

use Test::More;
use strict;
$|=1;

if (!eval { require Module::Signature; 1 }) {
	plan skip_all => 
		"Please install Module::Signature so that you can verify ".
			"the integrity of this and other distributions.";
}
elsif ( !-e 'SIGNATURE' ) {
	plan skip_all => "SIGNATURE file was not found";
}
elsif ( -s 'SIGNATURE' == 0 ) {
	plan skip_all => "SIGNATURE file was empty";
}
elsif (!eval { require Socket; Socket::inet_aton('pgp.mit.edu') }) {
	plan skip_all => "Cannot connect to the keyserver to check module signature";
}
else {
	plan tests => 1;
}

my $ret = Module::Signature::verify();
SKIP: {
	skip "Module::Signature cannot verify", 1 
		if $ret eq Module::Signature::CANNOT_VERIFY();
	cmp_ok $ret, '==', Module::Signature::SIGNATURE_OK(), "Valid signature";
}

