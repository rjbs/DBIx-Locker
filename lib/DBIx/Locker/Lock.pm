use strict;
use warnings;
use 5.008;
# ABSTRACT: a live resource lock

package DBIx::Locker::Lock;

use Carp ();

=method new

B<Calling this method is a very, very stupid idea.>  This method is called by
L<DBIx::Locker> to create locks.  Since you are not a locker, you should not
call this method.  Seriously.

  my $locker = DBIx::Locker::Lock->new(\%arg);

This returns a new lock. 

  locker    - the locker creating the lock
  lock_id   - the id of the lock in the lock table
  expires   - the time (in epoch seconds) at which the lock will expire
  locked_by - a hashref of identifying information

=cut

sub new {
  my ($class, $arg) = @_;

  my $guts = {
    locker    => $arg->{locker},
    lock_id   => $arg->{lock_id},
    expires   => $arg->{expires},
    locked_by => $arg->{locked_by},
  };

  return bless $guts => $class;
}

=method locker

=method lock_id

=method locked_by

These are accessors for data supplied to L</new>.

=cut

BEGIN {
  for my $attr (qw(locker lock_id locked_by)) {
    Sub::Install::install_sub({
      code => sub {
        Carp::confess("$attr is read-only") if @_ > 1;
        $_[0]->{$attr}
      },
      as   => $attr,
    });
  }
}

=method expires

This method returns the expiration time (as a unix timestamp) as provided to
L</new> -- unless expiration has been changed.  Expiration can be changed by
using this method as a mutator:

  # expire one hour from now, no matter what initial expiration was
  $lock->expired(time + 3600);

When updating the expiration time, if the given expiration time is not a valid
unix time, or if the expiration cannot be updated, an exception will be raised.

=cut

sub expires {
  my $self = shift;
  return $self->{expires} unless @_;

  my $new_expiry = shift;

  Carp::confess("new expiry must be a Unix epoch time")
    unless $new_expiry =~ /\A\d+\z/;

  my $dbh   = $self->locker->dbh;
  my $table = $self->locker->table;

  my $rows  = $dbh->do(
    "UPDATE $table SET expires = ? WHERE id = ?",
    undef,
    $new_expiry,
    $self->lock_id,
  );

  my $str = defined $rows ? $rows : 'undef';
  Carp::confess("error updating expiry: UPDATE returned $str") if $rows != 1;

  $self->{expires} = $new_expiry;

  return $new_expiry;
}

=method guid

This method returns the lock's globally unique id.

=cut

sub guid { $_[0]->locked_by->{guid} }

=method unlock

This method unlocks the lock, deleting the semaphor record.  This method is
automatically called when locks are garbage collected.

=cut

sub unlock {
  my ($self) = @_;

  my $dbh   = $self->locker->dbh;
  my $table = $self->locker->table;

  my $rows = $dbh->do("DELETE FROM $table WHERE id=?", undef, $self->lock_id);

  Carp::confess('error releasing lock') unless $rows == 1;
}

sub DESTROY {
  my ($self) = @_;
  local $@;
  return unless $self->locked_by->{pid} == $$;
  $self->unlock;
}

1;
