use strict;
use warnings;
use inc::Module::Install;

name 'DBIx-Locker';
all_from 'lib/DBIx/Locker.pm';

requires 'DBI';
requires 'Data::GUID';
requires 'Getopt::Long::Descriptive';
requires 'JSON::XS';
requires 'Log::Dispatchouli';
requires 'Sys::Hostname';

install_script('bin/dbix_locker_purge');

WriteAll;
