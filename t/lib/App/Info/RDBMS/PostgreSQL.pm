package App::Info::RDBMS::PostgreSQL;

=head1 NAME

App::Info::RDBMS::PostgreSQL - Information about PostgreSQL

=head1 SYNOPSIS

  use App::Info::RDBMS::PostgreSQL;

  my $pg = App::Info::RDBMS::PostgreSQL->new;

  if ($pg->installed) {
      print "App name: ", $pg->name, "\n";
      print "Version:  ", $pg->version, "\n";
      print "Bin dir:  ", $pg->bin_dir, "\n";
  } else {
      print "PostgreSQL is not installed. :-(\n";
  }

=head1 DESCRIPTION

App::Info::RDBMS::PostgreSQL supplies information about the PostgreSQL
database server installed on the local system. It implements all of the
methods defined by App::Info::RDBMS. Methods that trigger events will trigger
them only the first time they're called (See L<App::Info|App::Info> for
documentation on handling events). To start over (after, say, someone has
installed PostgreSQL) construct a new App::Info::RDBMS::PostgreSQL object to
aggregate new meta data.

Some of the methods trigger the same events. This is due to cross-calling of
shared subroutines. However, any one event should be triggered no more than
once. For example, although the info event "Executing `pg_config --version`"
is documented for the methods C<name()>, C<version()>, C<major_version()>,
C<minor_version()>, and C<patch_version()>, rest assured that it will only be
triggered once, by whichever of those four methods is called first.

=cut

use strict;
use App::Info::RDBMS;
use App::Info::Util;
use vars qw(@ISA $VERSION);
@ISA = qw(App::Info::RDBMS);
$VERSION = '0.57';
use constant WIN32 => $^O eq 'MSWin32';

my $u = App::Info::Util->new;
my @EXES = qw(postgres createdb createlang createuser dropdb droplang
              dropuser initdb pg_dump pg_dumpall pg_restore postmaster
              vacuumdb psql);

=head1 INTERFACE

=head2 Constructor

=head3 new

  my $pg = App::Info::RDBMS::PostgreSQL->new(@params);

Returns an App::Info::RDBMS::PostgreSQL object. See L<App::Info|App::Info> for
a complete description of argument parameters.

When it called, C<new()> searches the file system for an executable named for
the list returned by C<search_exe_names()>, usually F<pg_config>, in the list
of directories returned by C<search_bin_dirs()>. If found, F<pg_config> will
be called by the object methods below to gather the data necessary for
each. If F<pg_config> cannot be found, then PostgreSQL is assumed not to be
installed, and each of the object methods will return C<undef>.

C<new()> also takes a number of optional parameters in addition to those
documented for App::Info. These parameters allow you to specify alternate
names for PostgreSQL executables (other than F<pg_config>, which you specify
via the C<search_exe_names> parameter). These parameters are:

=over

=item search_postgres_names

=item search_createdb_names

=item search_createlang_names

=item search_createuser_names

=item search_dropd_names

=item search_droplang_names

=item search_dropuser_names

=item search_initdb_names

=item search_pg_dump_names

=item search_pg_dumpall_names

=item search_pg_restore_names

=item search_postmaster_names

=item search_psql_names

=item search_vacuumdb_names

=back

B<Events:>

=over 4

=item info

Looking for pg_config

=item confirm

Path to pg_config?

=item unknown

Path to pg_config?

=back

=cut

sub new {
    # Construct the object.
    my $self = shift->SUPER::new(@_);

    # Find pg_config.
    $self->info("Looking for pg_config");

    my @paths = $self->search_bin_dirs;
    my @exes = $self->search_exe_names;

    if (my $cfg = $u->first_cat_exe(\@exes, @paths)) {
        # We found it. Confirm.
        $self->{pg_config} = $self->confirm( key      => 'path to pg_config',
                                             prompt   => "Path to pg_config?",
                                             value    => $cfg,
                                             callback => sub { -x },
                                             error    => 'Not an executable');
    } else {
        # Handle an unknown value.
        $self->{pg_config} = $self->unknown( key      => 'path to pg_config',
                                             prompt   => "Path to pg_config?",
                                             callback => sub { -x },
                                             error    => 'Not an executable');
    }

    # Set up search defaults.
    for my $exe (@EXES) {
        my $attr = "search_$exe\_names";
        if (exists $self->{$attr}) {
            $self->{$attr} = [$self->{$attr}] unless ref $self->{$attr} eq 'ARRAY';
        } else {
            $self->{$attr} = [];
        }
    }

    return $self;
}

# We'll use this code reference as a common way of collecting data.
my $get_data = sub {
    return unless $_[0]->{pg_config};
    $_[0]->info(qq{Executing `"$_[0]->{pg_config}" $_[1]`});
    my $info = `"$_[0]->{pg_config}" $_[1]`;
    chomp $info;
    return $info;
};

##############################################################################

=head2 Class Method

=head3 key_name

  my $key_name = App::Info::RDBMS::PostgreSQL->key_name;

Returns the unique key name that describes this class. The value returned is
the string "PostgreSQL".

=cut

sub key_name { 'PostgreSQL' }

##############################################################################

=head2 Object Methods

=head3 installed

  print "PostgreSQL is ", ($pg->installed ? '' : 'not '), "installed.\n";

Returns true if PostgreSQL is installed, and false if it is not.
App::Info::RDBMS::PostgreSQL determines whether PostgreSQL is installed based
on the presence or absence of the F<pg_config> application on the file system
as found when C<new()> constructed the object. If PostgreSQL does not appear
to be installed, then all of the other object methods will return empty
values.

=cut

sub installed { return $_[0]->{pg_config} ? 1 : undef }

##############################################################################

=head3 name

  my $name = $pg->name;

Returns the name of the application. App::Info::RDBMS::PostgreSQL parses the
name from the system call C<`pg_config --version`>.

B<Events:>

=over 4

=item info

Executing `pg_config --version`

=item error

Failed to find PostgreSQL version with `pg_config --version`

Unable to parse name from string

Unable to parse version from string

Failed to parse PostgreSQL version parts from string

=item unknown

Enter a valid PostgreSQL name

=back

=cut

# This code reference is used by name(), version(), major_version(),
# minor_version(), and patch_version() to aggregate the data they need.
my $get_version = sub {
    my $self = shift;
    $self->{'--version'} = 1;
    my $data = $get_data->($self, '--version');
    unless ($data) {
        $self->error("Failed to find PostgreSQL version with ".
                     "`$self->{pg_config} --version`");
            return;
    }

    chomp $data;
    my ($name, $version) =  split /\s+/, $data, 2;

    # Check for and assign the name.
    $name ?
      $self->{name} = $name :
      $self->error("Unable to parse name from string '$data'");

    # Parse the version number.
    if ($version) {
        my ($x, $y, $z) = $version =~ /^(\d+)\.(\d+)\.(\d+)/;
        if (defined $x and defined $y and defined $z) {
            # Pre-v10 normal releases
            @{$self}{qw(version major minor patch)} =
              ($version, $x, $y, $z);
        }
        elsif ($version =~ /^(\d)\.(\d+)/) {        # < v10
            # New versions, such as "7.4", are treated as patch level "0"
            @{$self}{qw(version major minor patch)} =
                ($version, $1, $2, 0);
        }
        elsif ($version =~ /^(\d{2,})\.(\d+)/) {    # >= v10
            @{$self}{qw(version major minor patch)} =
                ($version, $1, 0, $2); # from v10 onwards, $2 will be patch level
        }
        elsif ($version =~ /^(\d{2,})(devel|beta|rc|alpha)/) {
            # Beta/devel/release candidates are treated as minor/patch level "0"
            @{$self}{qw(version major minor patch)} =
                ($version, $1, 0, 0);
        }
        else {
            $self->error("Failed to parse PostgreSQL version parts from string '$version'");
        }
    }
    else {
        $self->error("Unable to parse version from string '$data'");
    }
};

sub name {
    my $self = shift;
    return unless $self->{pg_config};

    # Load data.
    $get_version->($self) unless $self->{'--version'};

    # Handle an unknown name.
    $self->{name} ||= $self->unknown( key => 'postgres name' );

    # Return the name.
    return $self->{name};
}

##############################################################################

=head3 version

  my $version = $pg->version;

Returns the PostgreSQL version number. App::Info::RDBMS::PostgreSQL parses the
version number from the system call C<`pg_config --version`>.

B<Events:>

=over 4

=item info

Executing `pg_config --version`

=item error

Failed to find PostgreSQL version with `pg_config --version`

Unable to parse name from string

Unable to parse version from string

Failed to parse PostgreSQL version parts from string

=item unknown

Enter a valid PostgreSQL version number

=back

=cut

sub version {
    my $self = shift;
    return unless $self->{pg_config};

    # Load data.
    $get_version->($self) unless $self->{'--version'};

    # Handle an unknown value.
    unless ($self->{version}) {
        # Create a validation code reference.
        my $chk_version = sub {
            # Try to get the version number parts.
            my ($x, $y, $z);
            if ( /^(\d{2,})/) {
                ($x, $y, $z ) = ($1, 0, 0);             #  >= v10
            }
            else {
                ($x, $y, $z) = /^(\d)\.(\d+).(\d+)$/;   #  <  v10
            }
            # Return false if we didn't get all three.
            return unless $x and defined $y and defined $z;
            # Save all three parts.
            @{$self}{qw(major minor patch)} = ($x, $y, $z);
            # Return true.
            return 1;
        };
        $self->{version} = $self->unknown( key     => 'postgres version number',
                                           callback => $chk_version);
    }

    return $self->{version};
}

##############################################################################

=head3 major version

  my $major_version = $pg->major_version;

Returns the PostgreSQL major version number. App::Info::RDBMS::PostgreSQL
parses the major version number from the system call C<`pg_config --version`>.
For example, if C<version()> returns "7.1.2", then this method returns "7".

B<Events:>

=over 4

=item info

Executing `pg_config --version`

=item error

Failed to find PostgreSQL version with `pg_config --version`

Unable to parse name from string

Unable to parse version from string

Failed to parse PostgreSQL version parts from string

=item unknown

Enter a valid PostgreSQL major version number

=back

=cut

# This code reference is used by major_version(), minor_version(), and
# patch_version() to validate a version number entered by a user.
my $is_int = sub { /^\d+$/ };

sub major_version {
    my $self = shift;
    return unless $self->{pg_config};
    # Load data.
    $get_version->($self) unless exists $self->{'--version'};
    # Handle an unknown value.
    $self->{major} = $self->unknown( key      => 'postgres major version number',
                                     callback => $is_int)
      unless $self->{major};
    return $self->{major};
}

##############################################################################

=head3 minor version

  my $minor_version = $pg->minor_version;

Returns the PostgreSQL minor version number. App::Info::RDBMS::PostgreSQL
parses the minor version number from the system call C<`pg_config --version`>.
For example, if C<version()> returns "7.1.2", then this method returns "2".

B<Events:>

=over 4

=item info

Executing `pg_config --version`

=item error

Failed to find PostgreSQL version with `pg_config --version`

Unable to parse name from string

Unable to parse version from string

Failed to parse PostgreSQL version parts from string

=item unknown

Enter a valid PostgreSQL minor version number

=back

=cut

sub minor_version {
    my $self = shift;
    return unless $self->{pg_config};
    # Load data.
    $get_version->($self) unless exists $self->{'--version'};
    # Handle an unknown value.
    $self->{minor} = $self->unknown( key      => 'postgres minor version number',
                                     callback => $is_int)
      unless defined $self->{minor};
    return $self->{minor};
}

##############################################################################

=head3 patch version

  my $patch_version = $pg->patch_version;

Returns the PostgreSQL patch version number. App::Info::RDBMS::PostgreSQL
parses the patch version number from the system call C<`pg_config --version`>.
For example, if C<version()> returns "7.1.2", then this method returns "1".

B<Events:>

=over 4

=item info

Executing `pg_config --version`

=item error

Failed to find PostgreSQL version with `pg_config --version`

Unable to parse name from string

Unable to parse version from string

Failed to parse PostgreSQL version parts from string

=item unknown

Enter a valid PostgreSQL minor version number

=back

=cut

sub patch_version {
    my $self = shift;
    return unless $self->{pg_config};
    # Load data.
    $get_version->($self) unless exists $self->{'--version'};
    # Handle an unknown value.
    $self->{patch} = $self->unknown( key      => 'postgres patch version number',
                                     callback => $is_int)
      unless defined $self->{patch};
    return $self->{patch};
}

##############################################################################

=head3 executable

  my $exe = $pg->executable;

Returns the full path to the PostgreSQL server executable, which is named
F<postgres>.  This method does not use the executable names returned by
C<search_exe_names()>; those executable names are used to search for
F<pg_config> only (in C<new()>).

When it called, C<executable()> checks for an executable named F<postgres> in
the directory returned by C<bin_dir()>.

Note that C<executable()> is simply an alias for C<postgres()>.

B<Events:>

=over 4

=item info

Looking for postgres executable

=item confirm

Path to postgres executable?

=item unknown

Path to postgres executable?

=back

=cut

my $find_exe = sub  {
    my ($self, $key) = @_;
    my $exe = $key . (WIN32 ? '.exe' : '');
    my $meth = "search_$key\_names";

    # Find executable.
    $self->info("Looking for $key");

    unless ($self->{$key}) {
        my $bin = $self->bin_dir or return;
        if (my $exe = $u->first_cat_exe([$self->$meth(), $exe], $bin)) {
            # We found it. Confirm.
            $self->{$key} = $self->confirm(
                key      => "path to $key",
                prompt   => "Path to $key executable?",
                value    => $exe,
                callback => sub { -x },
                error    => 'Not an executable'
            );
        } else {
            # Handle an unknown value.
            $self->{$key} = $self->unknown(
                key      => "path to $key",
                prompt   => "Path to $key executable?",
                callback => sub { -x },
                error    => 'Not an executable'
            );
        }
    }

    return $self->{$key};
};

for my $exe (@EXES) {
    no strict 'refs';
    *{$exe} = sub { shift->$find_exe($exe) };
    *{"search_$exe\_names"} = sub { @{ shift->{"search_$exe\_names"} } }
}

*executable = \&postgres;

##############################################################################

=head3 bin_dir

  my $bin_dir = $pg->bin_dir;

Returns the PostgreSQL binary directory path. App::Info::RDBMS::PostgreSQL
gathers the path from the system call C<`pg_config --bindir`>.

B<Events:>

=over 4

=item info

Executing `pg_config --bindir`

=item error

Cannot find bin directory

=item unknown

Enter a valid PostgreSQL bin directory

=back

=cut

# This code reference is used by bin_dir(), lib_dir(), and so_lib_dir() to
# validate a directory entered by the user.
my $is_dir = sub { -d };

sub bin_dir {
    my $self = shift;
    return unless $self->{pg_config};
    unless (exists $self->{bin_dir} ) {
        if (my $dir = $get_data->($self, '--bindir')) {
            $self->{bin_dir} = $dir;
        } else {
            # Handle an unknown value.
            $self->error("Cannot find bin directory");
            $self->{bin_dir} = $self->unknown( key      => 'postgres bin dir',
                                               callback => $is_dir)
        }
    }

    return $self->{bin_dir};
}

##############################################################################

=head3 inc_dir

  my $inc_dir = $pg->inc_dir;

Returns the PostgreSQL include directory path. App::Info::RDBMS::PostgreSQL
gathers the path from the system call C<`pg_config --includedir`>.

B<Events:>

=over 4

=item info

Executing `pg_config --includedir`

=item error

Cannot find include directory

=item unknown

Enter a valid PostgreSQL include directory

=back

=cut

sub inc_dir {
    my $self = shift;
    return unless $self->{pg_config};
    unless (exists $self->{inc_dir} ) {
        if (my $dir = $get_data->($self, '--includedir')) {
            $self->{inc_dir} = $dir;
        } else {
            # Handle an unknown value.
            $self->error("Cannot find include directory");
            $self->{inc_dir} = $self->unknown( key      => 'postgres include dir',
                                               callback => $is_dir)
        }
    }

    return $self->{inc_dir};
}

##############################################################################

=head3 lib_dir

  my $lib_dir = $pg->lib_dir;

Returns the PostgreSQL library directory path. App::Info::RDBMS::PostgreSQL
gathers the path from the system call C<`pg_config --libdir`>.

B<Events:>

=over 4

=item info

Executing `pg_config --libdir`

=item error

Cannot find library directory

=item unknown

Enter a valid PostgreSQL library directory

=back

=cut

sub lib_dir {
    my $self = shift;
    return unless $self->{pg_config};
    unless (exists $self->{lib_dir} ) {
        if (my $dir = $get_data->($self, '--libdir')) {
            $self->{lib_dir} = $dir;
        } else {
            # Handle an unknown value.
            $self->error("Cannot find library directory");
            $self->{lib_dir} = $self->unknown( key      => 'postgres library dir',
                                               callback => $is_dir)
        }
    }

    return $self->{lib_dir};
}

##############################################################################

=head3 so_lib_dir

  my $so_lib_dir = $pg->so_lib_dir;

Returns the PostgreSQL shared object library directory path.
App::Info::RDBMS::PostgreSQL gathers the path from the system call
C<`pg_config --pkglibdir`>.

B<Events:>

=over 4

=item info

Executing `pg_config --pkglibdir`

=item error

Cannot find shared object library directory

=item unknown

Enter a valid PostgreSQL shared object library directory

=back

=cut

# Location of dynamically loadable modules.
sub so_lib_dir {
    my $self = shift;
    return unless $self->{pg_config};
    unless (exists $self->{so_lib_dir} ) {
        if (my $dir = $get_data->($self, '--pkglibdir')) {
            $self->{so_lib_dir} = $dir;
        } else {
            # Handle an unknown value.
            $self->error("Cannot find shared object library directory");
            $self->{so_lib_dir} =
              $self->unknown( key      => 'postgres so directory',
                              callback => $is_dir)
        }
    }

    return $self->{so_lib_dir};
}

##############################################################################

=head3 configure options

  my $configure = $pg->configure;

Returns the options with which the PostgreSQL server was
configured. App::Info::RDBMS::PostgreSQL gathers the configure data from the
system call C<`pg_config --configure`>.

B<Events:>

=over 4

=item info

Executing `pg_config --configure`

=item error

Cannot find configure information

=item unknown

Enter PostgreSQL configuration options

=back

=cut

sub configure {
    my $self = shift;
    return unless $self->{pg_config};
    unless (exists $self->{configure} ) {
        if (my $conf = $get_data->($self, '--configure')) {
            $self->{configure} = $conf;
        } else {
            # Configure can be empty, so just make sure it exists and is
            # defined. Don't prompt.
            $self->{configure} = '';
        }
    }

    return $self->{configure};
}

##############################################################################

=head3 home_url

  my $home_url = $pg->home_url;

Returns the PostgreSQL home page URL.

=cut

sub home_url { "http://www.postgresql.org/" }

##############################################################################

=head3 download_url

  my $download_url = $pg->download_url;

Returns the PostgreSQL download URL.

=cut

sub download_url { "http://www.postgresql.org/mirrors-ftp.html" }

##############################################################################

=head3 search_exe_names

  my @search_exe_names = $app->search_exe_names;

Returns a list of possible names for F<pg_config> executable. By default, only
F<pg_config> is returned (or F<pg_config.exe> on Win32).

Note that this method is not used to search for the PostgreSQL server
executable, only F<pg_config>.

=cut

sub search_exe_names {
    my $self = shift;
    my $exe = 'pg_config';
    $exe .= '.exe' if WIN32;
    return ($self->SUPER::search_exe_names, $exe);
}

##############################################################################

=head3 search_bin_dirs

  my @search_bin_dirs = $app->search_bin_dirs;

Returns a list of possible directories in which to search an executable. Used
by the C<new()> constructor to find an executable to execute and collect
application info. The found directory will also be returned by the C<bin_dir>
method.

The list of directories by default consists of the path as defined by
C<< File::Spec->path >>, as well as the following directories:

=over 4

=item $ENV{POSTGRES_HOME}/bin (if $ENV{POSTGRES_HOME} exists)

=item $ENV{POSTGRES_LIB}/../bin (if $ENV{POSTGRES_LIB} exists)

=item /usr/local/pgsql/bin

=item /usr/local/postgres/bin

=item /opt/pgsql/bin

=item /usr/local/bin

=item /usr/local/sbin

=item /usr/bin

=item /usr/sbin

=item /bin

=item C:\Program Files\PostgreSQL\bin

=back

=cut

sub search_bin_dirs {
    return shift->SUPER::search_bin_dirs,
      ( exists $ENV{POSTGRES_HOME}
          ? ($u->catdir($ENV{POSTGRES_HOME}, "bin"))
          : ()
      ),
      ( exists $ENV{POSTGRES_LIB}
          ? ($u->catdir($ENV{POSTGRES_LIB}, $u->updir, "bin"))
          : ()
      ),
      $u->path,
      qw(/usr/local/pgsql/bin
         /usr/local/postgres/bin
         /usr/lib/postgresql/bin
         /opt/pgsql/bin
         /usr/local/bin
         /usr/local/sbin
         /usr/bin
         /usr/sbin
         /bin),
      'C:\Program Files\PostgreSQL\bin';
}

##############################################################################

=head2 Other Executable Methods

These methods function just like the C<executable()> method, except that they
return different executables. PostgreSQL comes with a fair number of them; we
provide these methods to provide a path to a subset of them. Each method, when
called, checks for an executable in the directory returned by C<bin_dir()>.
The name of the executable must be one of the names returned by the
corresponding C<search_*_names> method.

The available executable methods are:

=over

=item postgres

=item createdb

=item createlang

=item createuser

=item dropdb

=item droplang

=item dropuser

=item initdb

=item pg_dump

=item pg_dumpall

=item pg_restore

=item postmaster

=item psql

=item vacuumdb

=back

And the corresponding search names methods are:

=over

=item search_postgres_names

=item search_createdb_names

=item search_createlang_names

=item search_createuser_names

=item search_dropd_names

=item search_droplang_names

=item search_dropuser_names

=item search_initdb_names

=item search_pg_dump_names

=item search_pg_dumpall_names

=item search_pg_restore_names

=item search_postmaster_names

=item search_psql_names

=item search_vacuumdb_names

=back

B<Events:>

=over 4

=item info

Looking for executable

=item confirm

Path to executable?

=item unknown

Path to executable?

=back

=cut

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

David E. Wheeler <david@justatheory.com> based on code by Sam Tregar
<sam@tregar.com>.

=head1 SEE ALSO

L<App::Info|App::Info> documents the event handling interface.

L<App::Info::RDBMS|App::Info::RDBMS> is the App::Info::RDBMS::PostgreSQL
parent class.

L<DBD::Pg|DBD::Pg> is the L<DBI|DBI> driver for connecting to PostgreSQL
databases.

L<http://www.postgresql.org/> is the PostgreSQL home page.

=head1 COPYRIGHT AND LICENSE

Copyright (c) 2002-2011, David E. Wheeler. Some Rights Reserved.

This module is free software; you can redistribute it and/or modify it under the
same terms as Perl itself.

=cut
