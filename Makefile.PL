use strict;
use warnings;
use inc::Module::Install;

name 'DBIx-Locker';
all_from 'lib/DBIx/Locker.pm';

requires 'DBI';
requires 'Data::GUID';
requires 'JSON::XS';
requires 'Sys::Hostname';

install_script('bin/dbix_locker_purge');

WriteAll;
