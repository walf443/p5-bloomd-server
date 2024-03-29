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

$SIG{ALRM} = sub {
    die "timeout";
};
alarm 5;

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

subtest "check command check" => sub {
    subtest "case that check is true" => sub {
        my $ret = print $client "check foo\r\n";
        ok $ret, "write to sock OK";

        my $line = <$client>;
        is $line, "CHECK foo 1\r\n", "foo should be 1"
            or diag($!);
        $line = <$client>;
        is $line, "END\r\n", "response should end"
            or diag($!);
    };

    subtest "case that check is false" => sub {
        my $ret = print $client "check bar\r\n";
        ok $ret, "write to sock OK";

        my $line = <$client>;
        is $line, "CHECK bar 0\r\n", "bar should be 0"
            or diag($!);
        $line = <$client>;
        is $line, "END\r\n", "response should end"
            or diag($!);
    };

    subtest "multiple keys" => sub {
        my $ret = print $client "check foo bar baz\r\n";
        ok $ret, "write to sock OK";

        my $line = <$client>;
        is $line, "CHECK foo 1\r\n", "foo should be 1"
            or diag($!);
        $line = <$client>;
        is $line, "CHECK bar 0\r\n", "bar should be 0"
            or diag($!);
        $line = <$client>;
        is $line, "CHECK baz 0\r\n", "baz should be 0"
            or diag($!);

        $line = <$client>;
        is $line, "END\r\n", "response should end"
            or diag($!);
    };

};

done_testing;

