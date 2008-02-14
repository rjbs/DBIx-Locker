use strict;
use warnings;
use 5.008;

package ICG::Locker;
our $VERSION = '0.002';

use DBI;
use Data::GUID ();
use ICG::Exceptions;
use ICG::Handy ();
use ICG::Locker::Lock;
use JSON::XS ();
use Sys::Hostname ();

=head1 NAME

ICG::Locker - locks for db resources that might not be totally insane

=head1 METHODS

=head2 new

  my $locker = ICG::Locker->new(\%arg);

This returns a new locker. 

Valid arguments are:

  dbh      - a database handle to use for locking
  dbi_args - an arrayref of args to pass to DBI->connect to reconnect to db
  table    - the table for locks

=cut

sub new {
  my ($class, $arg) = @_;

  my $guts = {
    dbh      => $arg->{dbh},
    dbi_args => ($arg->{dbi_args} || $class->default_dbi_args),
    table    => ($arg->{table}    || $class->default_table),
  };

  return bless $guts => $class;
}

=head2 default_dbi_args

=head2 default_table

These methods may be defined in subclasses to provide defaults to be used when
constructing a new locker.

=cut

sub default_dbi_args { X->throw('dbi_args not given and no default defined') }
sub default_table    { X->throw('table not given and no default defined') }

=head2 dbh

This method returns the locker's dbh.

=cut

sub dbh {
  my ($self) = @_;
  return $self->{dbh} if $self->{dbh} and eval { $self->{dbh}->ping };

  X::Unavailable->throw("couldn't connect to database: $DBI::errstr")
    unless my $dbh = DBI->connect(@{ $self->{dbi_args} });

  return $self->{dbh} = $dbh;
}

=head2 table

This method returns the name of the table in the database in which locks are
stored.

=cut

sub table {
  return $_[0]->{table}
}

=head2 lock

  my $lock = $locker->lock($identifier, \%arg);

This method attempts to return a new ICG::Locker::Lock.

=cut

my $JSON;
BEGIN { $JSON = JSON::XS->new->canonical(1)->space_after(1); }

sub lock {
  my ($self, $ident, $arg) = @_;
  $arg ||= {};

  X::BadValue->throw('must provide a lockstring')
    unless defined $ident and length $ident;

  my $expires = $arg->{expires} ||= 3600;

  X::BadValue->throw('expires must be a positive integer')
    unless $expires > 0 and $expires == int $expires;

  $expires = time + $expires;

  my $locked_by = {
    host => Sys::Hostname::hostname(),
    guid => Data::GUID->new->as_string,
    pid  => $$,
  };

  my $table = $self->table;
  my $dbh   = $self->dbh;

  local $dbh->{RaiseError} = 0;
  local $dbh->{PrintError} = 0;

  my $rows  = $dbh->do(
    "INSERT INTO $table (lockstring, created, expires, locked_by)
    VALUES (?, ?, ?, ?)",
    undef,
    $ident,
    ICG::Handy::now14,
    ICG::Handy::then14(localtime($expires)),
    $JSON->encode($locked_by),
  );

  X::Unavailable->throw('could not lock resource') unless $rows and $rows == 1;

  my $lock = ICG::Locker::Lock->new({
    locker    => $self,
    lock_id   => $dbh->last_insert_id(undef, undef, $table, 'id'),
    expires   => $expires,
    locked_by => $locked_by,
  });

  return $lock;
}

=head2 purge_expired_locks

This method deletes expired semaphores.

=cut

sub purge_expired_locks {
  my ($self) = @_;

  my $dbh = $self->dbh;
  local $dbh->{RaiseError} = 0;
  local $dbh->{PrintError} = 0;

  my $table = $self->table;

  my $rows = $dbh->do(
    "DELETE FROM $table WHERE expires < ?",
    undef,
    ICG::Handy::now14,
  );
}

=head1 COPYRIGHT AND LICENSE

Proprietary.  Copyright 2008 IC Group

All rights reserved.

=cut

1;
