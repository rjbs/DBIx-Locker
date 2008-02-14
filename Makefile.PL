use strict;
use warnings;
use inc::Module::Install;

name 'ICG-Locker';
all_from 'lib/ICG/Locker.pm';

requires 'DBI';
requires 'Data::GUID';
requires 'ICG::CLI';
requires 'ICG::Exceptions';
requires 'ICG::Handy';
requires 'JSON::XS';

install_script('bin/icg_locker_purge');

WriteAll;
