use strict;
use warnings;
use 5.008;
# ABSTRACT: a live resource lock

package DBIx::Locker::Lock;

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

=method expires

=method locked_by

These are accessors for data supplied to L</new>.

=cut

BEGIN {
  for my $attr (qw(locker lock_id expires locked_by)) {
    Sub::Install::install_sub({
      code => sub { $_[0]->{$attr} },
      as   => $attr,
    });
  }
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

  die('error releasing lock') unless $rows == 1;
}

=method update_expiry

This method updates the expiration time for the given lock. It accepts a Unix
epoch time as an integer.

=cut

sub update_expiry {
  my ($self, $new_expiry) = @_;

  die "new expiry must be a Unix epoch time" unless $new_expiry =~ /\A\d+\Z/;

  my $dbh   = $self->locker->dbh;
  my $table = $self->locker->table;

  my $rows  = $dbh->do(
    "UPDATE $table SET expires = ? WHERE id = ?",
    undef,
    $new_expiry,
    $self->lock_id,
  );

  die('error updating expiry time') unless $rows == 1;

  $self->{expires} = $new_expiry;
}

sub DESTROY {
  my ($self) = @_;
  local $@;
  return unless $self->locked_by->{pid} == $$;
  $self->unlock;
}

1;
