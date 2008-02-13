use strict;
use warnings;
use inc::Module::Install;

name 'ObjectDB-Locker';
all_from 'lib/ObjectDB/Locker.pm';

requires 'ObjectDB';

WriteAll;
