use strict;
use warnings;

use Test::More tests => 8;

use DBI;
use DBIx::Locker;

unlink 'test.db';

my @conn = ('dbi:SQLite:dbname=test.db', undef, undef, {});

{
  my $dbh = DBI->connect(@conn);
  $dbh->do('CREATE TABLE locks (
    id INTEGER PRIMARY KEY,
    lockstring varchar(128) UNIQUE,
    created varchar(14) NOT NULL,
    expires varchar(14) NOT NULL,
    locked_by varchar(1024)
  )');
}

my $locker = DBIx::Locker->new({
  dbi_args => \@conn,
  table    => 'locks',
});

isa_ok($locker, 'DBIx::Locker');

my $guid;

{
  my $lock = $locker->lock('Zombie Soup');
  isa_ok($lock, 'DBIx::Locker::Lock', 'obtained lock');

  my $id = $lock->lock_id;
  like($id, qr/\A\d+\z/, "we got a numeric lock id");

  $guid = $lock->guid;

  eval { $locker->lock('Zombie Soup'); };
  # isa_ok($@, 'X::Unavailable', "can't lock already-locked resources");
  ok($@);
}

{
  my $lock = $locker->lock('Zombie Soup');
  isa_ok($lock, 'DBIx::Locker::Lock', 'newly obtained lock');

  isnt($lock->guid, $guid, "new lock guid is not the old lock guid");

  my $lock_2 = $locker->lock('Zombie Cola');
  isa_ok($lock_2, 'DBIx::Locker::Lock', 'third lock');
  isnt($lock->lock_id, $lock_2->lock_id, 'two locks, two distinct id values');
}
