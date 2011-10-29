use inc::Module::Install;
use Module::Install::ReadmeFromPod;
use Module::Install::Repository;
name 'Bloomd-Server';
all_from 'lib/Bloomd/Server.pm';
readme_from 'lib/Bloomd/Server.pm';

# requires '';

tests_recursive 't';
author_tests 'xt';

test_requires 'Test::More';
test_requires 'Test::LoadAllModules';
configure_requires 'Module::Install::ReadmeFromPod';
auto_set_repository;
WriteAll;
