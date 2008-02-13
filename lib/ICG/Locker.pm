use strict;
use warnings;
use 5.008;

package ICG::Locker;
our $VERSION = '0.001';

use ICG::Exceptions;
use ICG::Handy ();
use ICG::Locker::Lock;
use JSON::XS ();
use Data::GUID ();

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
    dbi_args => ($arg->{dbi_args} || Carp::confess 'arg dbi_args is required'),
    table    => ($arg->{table}    || Carp::confess 'arg table is required'),
  };

  return bless $guts => $class;
}

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
    guid => Data::GUID->new->as_string,
    pid  => $$,
    for  => $arg->{for},
  };

  my $table = $self->table;
  my $dbh   = $self->dbh;
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

=head1 COPYRIGHT AND LICENSE

Proprietary.  Copyright 2008 IC Group

All rights reserved.

=cut

1;
