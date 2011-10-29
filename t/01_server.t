use strict;
use warnings;
use Test::TCP;
use Test::More;
use AnyEvent;
use Bloomd::Server;
use IO::Socket::INET;

my $server = Test::TCP->new(
    code => sub {
        my $port = shift;
        my $cv = AnyEvent->condvar;
        my $server = Bloomd::Server->new(port => $port);
        $server->run;
        $cv->recv;
    },
);

ok 1, "start server ok";

my $client = IO::Socket::INET->new(
    PeerAddr => sprintf("localhost:%d", $server->port),
    Proto => 'tcp',
)
    or die "Can't connect to server: $!";

ok 1, "client connect to server";

subtest "set command test" => sub {
    my $ret = print $client "set foo\r\n";
    ok $ret, "write to sock OK";

    my $line = <$client>;

    is $line, "OK\r\n", "set response should be OK"
        or diag($!);

};
done_testing;

