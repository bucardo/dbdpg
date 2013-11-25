package App::Info::Handler::Print;

=head1 NAME

App::Info::Handler::Print - Print App::Info event messages

=head1 SYNOPSIS

  use App::Info::Category::FooApp;
  use App::Info::Handler::Print;

  my $stdout = App::Info::Handler::Print->new( fh => 'stdout' );
  my $app = App::Info::Category::FooApp->new( on_info => $stdout );

  # Or...
  my $app = App::Info::Category::FooApp->new( on_error => 'stderr' );

=head1 DESCRIPTION

App::Info::Handler::Print objects handle App::Info events by printing their
messages to a filehandle. This means that if you want event messages to print
to a file or to a system filehandle, you can easily do it with this class.
You'll find, however, that App::Info::Handler::Print is most effective for
info and error events; unknown and prompt events are better handled by event
handlers that know how to prompt users for data. See
L<App::Info::Handler::Prompt|App::Info::Handler::Prompt> for an example of
that functionality.

Upon loading, App::Info::Handler::Print registers itself with
App::Info::Handler, setting up a couple of strings that can be passed to an
App::Info concrete subclass constructor. These strings are shortcuts that
tell App::Info how to create the proper App::Info::Handler::Print object
for handling events. The registered strings are:

=over 4

=item stdout

Prints event messages to C<STDOUT>.

=item stderr

Prints event messages to C<STDERR>.

=back

See the C<new()> constructor below for how to have App::Info::Handler::Print
print event messages to different filehandle.

=cut

use strict;
use App::Info::Handler;
use vars qw($VERSION @ISA);
$VERSION = '0.57';
@ISA = qw(App::Info::Handler);

# Register ourselves.
for my $c (qw(stderr stdout)) {
    App::Info::Handler->register_handler
      ($c => sub { __PACKAGE__->new( fh => $c ) } );
}

=head1 INTERFACE

=head2 Constructor

=head3 new

  my $stderr_handler = App::Info::Handler::Print->new;
  $stderr_handler = App::Info::Handler::Print->new( fh => 'stderr' );
  my $stdout_handler = App::Info::Handler::Print->new( fh => 'stdout' );
  my $fh = FileHandle->new($file);
  my $fh_handler = App::Info::Handler::Print->new( fh => $fh );

Constructs a new App::Info::Handler::Print and returns it. It can take a
single parameterized argument, C<fh>, which can be any one of the following
values:

=over 4

=item stderr

Constructs a App::Info::Handler::Print object that prints App::Info event
messages to C<STDERR>.

=item stdout

Constructs a App::Info::Handler::Print object that prints App::Info event
messages to C<STDOUT>.

=item FileHandle

=item GLOB

Pass in a reference and App::Info::Handler::Print will assume that it's a
filehandle reference that it can print to. Note that passing in something that
can't be printed to will trigger an exception when App::Info::Handler::Print
tries to print to it.

=back

If the C<fh> parameter is not passed, C<new()> will default to creating an
App::Info::Handler::Print object that prints App::Info event messages to
C<STDOUT>.

=cut

sub new {
    my $pkg = shift;
    my $self = $pkg->SUPER::new(@_);
    if (!defined $self->{fh} || $self->{fh} eq 'stderr') {
        # Create a reference to STDERR.
        $self->{fh} = \*STDERR;
    } elsif ($self->{fh} eq 'stdout') {
        # Create a reference to STDOUT.
        $self->{fh} = \*STDOUT;
    } elsif (!ref $self->{fh}) {
        # Assume a reference to a filehandle or else it's invalid.
        Carp::croak("Invalid argument to new(): '$self->{fh}'");
    }
    # We're done!
    return $self;
}

##############################################################################

=head3 handler

This method is called by App::Info to print out the message from events.

=cut

sub handler {
    my ($self, $req) = @_;
    print {$self->{fh}} $req->message, "\n";
    # Return true to indicate that we've handled the request.
    return 1;
}

1;
__END__

=head1 SUPPORT

This module is stored in an open L<GitHub
repository|http://github.com/theory/app-info/>. Feel free to fork and
contribute!

Please file bug reports via L<GitHub
Issues|http://github.com/theory/app-info/issues/> or by sending mail to
L<bug-App-Info@rt.cpan.org|mailto:bug-App-Info@rt.cpan.org>.

=head1 AUTHOR

David E. Wheeler <david@justatheory.com>

=head1 SEE ALSO

L<App::Info|App::Info> documents the event handling interface.

L<App::Info::Handler::Carp|App::Info::Handler::Carp> handles events by
passing their messages Carp module functions.

L<App::Info::Handler::Prompt|App::Info::Handler::Prompt> offers event handling
more appropriate for unknown and confirm events.

L<App::Info::Handler|App::Info::Handler> describes how to implement custom
App::Info event handlers.

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2002-2011, David E. Wheeler. Some Rights Reserved.

This module is free software; you can redistribute it and/or modify it under the
same terms as Perl itself.

=cut
