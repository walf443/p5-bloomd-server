package Bloomd::Server;

use strict;
use warnings;
use AnyEvent;
use AnyEvent::Socket;
use AnyEvent::Handle;
use Bloom::Filter;
use Log::Minimal qw/infof warnf debugf/;
our $VERSION = '0.01';

sub new {
    my ($class, %args) = @_;
    $args{port} ||= 26006;
    $args{clients} = [];
    $args{bloom} = Bloom::Filter->new(capacity => 10000, error_rate => .001 );
    bless \%args, $class;
}

sub run {
    my ($self, ) = @_;
    AnyEvent::Socket::tcp_server undef, $self->{port}, sub {
        my ($fh,$host, $port) = @_
            or die "Can't connect to server";

        my $ah = AnyEvent::Handle->new(
            fh => $fh,
            on_error => sub {
                my ($ah, $fatal, $msg) = @_;
                critf($msg);
                $ah->destroy;
            },
        );
        $self->{clients}->[fileno($fh)] = {
            handle => $ah,
        };

        $ah->on_read(sub {
            shift->push_read(line => sub {
                my ($ah, $line) = @_;
                my ($cmd, @args) = split /\t/, $line;
                $ah->push_write("OK\n");
            });
        });

    }, sub {
        infof("starting Bloomd::Server port: @{[ $self->{port} ]}");
    };
}

1;
__END__

=head1 NAME

Bloomd::Server -

=head1 SYNOPSIS

  use Bloomd::Server;
  my $server = Bloomd::Server->new;
  $server->run;

=head1 DESCRIPTION

Bloomd::Server is

=head1 AUTHOR

Keiji Yoshimi E<lt>walf443 at gmail dot comE<gt>

=head1 SEE ALSO

=head1 LICENSE

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself.

=cut
