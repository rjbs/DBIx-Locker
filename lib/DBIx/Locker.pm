use strict;
use warnings;
use 5.008;

package DBIx::Locker;
# ABSTRACT: locks for db resources that might not be totally insane

use DBI;
use Data::GUID ();
use DBIx::Locker::Lock;
use JSON::XS ();
use Sys::Hostname ();

=method new

  my $locker = DBIx::Locker->new(\%arg);

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

=method default_dbi_args

=method default_table

These methods may be defined in subclasses to provide defaults to be used when
constructing a new locker.

=cut

sub default_dbi_args { X->throw('dbi_args not given and no default defined') }
sub default_table    { X->throw('table not given and no default defined') }

=method dbh

This method returns the locker's dbh.

=cut

sub dbh {
  my ($self) = @_;
  return $self->{dbh} if $self->{dbh} and eval { $self->{dbh}->ping };

  die("couldn't connect to database: $DBI::errstr")
    unless my $dbh = DBI->connect(@{ $self->{dbi_args} });

  return $self->{dbh} = $dbh;
}

=method table

This method returns the name of the table in the database in which locks are
stored.

=cut

sub table {
  return $_[0]->{table}
}

=method lock

  my $lock = $locker->lock($identifier, \%arg);

This method attempts to return a new DBIx::Locker::Lock.

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
    $self->_time_to_string,
    $self->_time_to_string([ localtime($expires) ]),
    $JSON->encode($locked_by),
  );

  die('could not lock resource') unless $rows and $rows == 1;

  my $lock = DBIx::Locker::Lock->new({
    locker    => $self,
    lock_id   => $dbh->last_insert_id(undef, undef, $table, 'id'),
    expires   => $expires,
    locked_by => $locked_by,
  });

  return $lock;
}

sub _time_to_string {
  my ($self, $time) = @_;

  $time = [ localtime ] unless $time;
  return sprintf '%s-%s-%s %s:%s:%s',
    $time->[5] + 1900, $time->[4]+1, $time->[3],
    $time->[2], $time->[1], $time->[0];
}

=method purge_expired_locks

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
    $self->_time_to_string,
  );
}

1;
