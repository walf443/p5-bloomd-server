package Bloomd::Server;
use strict;
use warnings;
use v5.10;
use AnyEvent;
use AnyEvent::Socket;
use AnyEvent::Handle;
use Bloom::Faster;
use Log::Minimal qw/infof warnf debugf critf/;
our $VERSION = '0.01';

sub new {
    my ($class, %args) = @_;
    $args{port} ||= 26006;
    $args{clients} = [];
    $args{capacity} ||= 100_000;
    $args{error_rate} ||= .001;
    $args{stats} = {};
    $args{stats}->{pid} = $$;
    $args{stats}->{capacity} = $args{capacity};
    $args{stats}->{error_rate} = $args{error_rate};
    $args{stats}->{cmd_set} = 0;
    $args{stats}->{cmd_check} = 0;
    $args{bloom} = Bloom::Faster->new({ n => $args{capacity}, e => $args{error_rate}});
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
                $ah->destroy;
            },
        );
        $self->{clients}->[fileno($fh)] = {
            handle => $ah,
        };

        $ah->on_read(sub {
            shift->push_read(line => sub {
                my ($ah, $line) = @_;
                my ($cmd, @args) = split /\s+/, $line;
                if ( $cmd ) {
                    given ($cmd) {
                        when ( "set" ) {
                            $self->{stats}->{cmd_set}++;

                            if ( @args >= 1 ) {

                                $self->{bloom}->add(@args);
                                $ah->push_write("OK\r\n");
                            } else {
                                $ah->push_write("ERROR\r\n");
                            }
                        }
                        when ( "check" ) {
                            $self->{stats}->{cmd_check}++;

                            if ( @args >= 1 ) {
                                for my $arg ( @args ) {
                                    my $ret = $self->{bloom}->check($arg) ? 1 : 0;
                                    $ah->push_write("CHECK $arg $ret\r\n");
                                }
                                $ah->push_write("OK\r\n");
                            }
                        }
                        when ( "stats" ) {
                            for my $key ( sort keys %{ $self->{stats} } ) {
                                $ah->push_write("STAT $key @{[ $self->{stats}->{$key} ]}\r\n");
                            }
                            $ah->push_write("END\r\n");
                        }
                        default {
                            $ah->push_write("ERROR\r\n");
                        }
                    }
                } else {
                }
            });
        });

    }, sub {
        infof("starting Bloomd::Server port: @{[ $self->{port} ]}");
    };
}

sub DESTROY {
    infof("shutdown Bloomd::Server");
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
