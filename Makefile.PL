use strict;
use warnings;
use inc::Module::Install;

name 'ICG-Locker';
all_from 'lib/ICG/Locker.pm';

requires 'DBI';
requires 'Data::GUID';
requires 'JSON::XS';

WriteAll;
