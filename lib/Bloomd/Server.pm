package Bloomd::Server;
use strict;
use warnings;
use v5.10;
use AnyEvent;
use AnyEvent::Socket;
use AnyEvent::Handle;
use Bloom::Faster;
use Time::HiRes qw(gettimeofday);
use Log::Minimal qw/infof warnf debugf critf/;
our $VERSION = '0.01';

sub new {
    my ($class, %args) = @_;
    $args{port} ||= 26006;
    $args{clients} = [];
    $args{capacity} ||= 100_000;
    $args{error_rate} ||= .001;
    $args{server_id} ||= 1;

    $args{stats} = {};
    $args{stats}->{pid} = $$;
    $args{stats}->{server_id} = $args{server_id};
    $args{stats}->{capacity} = $args{capacity};
    $args{stats}->{error_rate} = $args{error_rate};
    $args{stats}->{cmd_set} = 0;
    $args{stats}->{cmd_check} = 0;
    $args{stats}->{uptime} = 0;
    $args{stats}->{server_time} = gettimeofday * 10_0000;
    $args{master_port} ||= 26006;
    if ( $args{from_snapshot} ) {
        $args{bloom} = Bloom::Faster->new($args{from_snapshot});
    } else {
        $args{bloom} = Bloom::Faster->new({ n => $args{capacity}, e => $args{error_rate}});
    }
    bless \%args, $class;
}

sub run {
    my ($self, ) = @_;

    $self->{timer} = AnyEvent->timer(after => 1, interval => 1, cb => sub {
        $self->{stats}->{uptime}++;
        $self->{stats}->{server_time} = gettimeofday * 10_0000;
    });

    if ( $self->{ulog} ) {
        $self->{current_ulog} = $self->{ulog} . "/ulog-" . $self->{server_id} . "-0001";
        open my $fh, '>', $self->{current_ulog}
            or die "Can't open file: $!";

        $self->{ulog_handle} = AnyEvent::Handle->new(
            fh => $fh,
            on_error => sub {
                my ($ah, $fatal, $msg) = @_;
                critf($msg);
                $ah->destroy;
            },
        );
    }

    if ( $self->{master_host} ) {
        AnyEvent::Socket::tcp_connect $self->{master_host}, $self->{master_port}, sub {
            my ($fh, ) = @_;

            infof("starting replication thread to master: %s:%d", $self->{master_host}, $self->{master_port});
            my $ah = AnyEvent::Handle->new(
                fh => $fh,
                on_error => sub {
                    my ($ah, $fatal, $msg) = @_;
                    $ah->destroy;
                }
            );
            $self->{slave_thread} = $ah;
            $self->repl_thread($ah, 0);
        }
    }

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
                                $self->set($self->{server_id}, @args);
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
                                $ah->push_write("END\r\n");
                            }
                        }
                        when ( "stats" ) {
                            for my $key ( sort keys %{ $self->{stats} } ) {
                                $ah->push_write("STAT $key @{[ $self->{stats}->{$key} ]}\r\n");
                            }
                            $ah->push_write("STAT key_count @{[ $self->{bloom}->key_count ]}\r\n");
                            $ah->push_write("STAT vector_size @{[ $self->{bloom}->get_vectorsize ]}\r\n");
                            $ah->push_write("END\r\n");
                        }
                        when ( "backup" ) {
                            if ( $self->{backupdir} ) {
                                $self->{stats}->{server_time} = gettimeofday * 10_0000;
                                my $pid = fork;
                                if ( defined $pid ) {
                                    if ( $pid ) {
                                        $ah->push_write("OK\r\n");
                                    } else {
                                        my $backup_file = $self->{backupdir} . "/snapshot." . $self->{stats}->{server_time};
                                        $self->{bloom}->to_file($backup_file);
                                        exit;
                                    }
                                } else {
                                    critf("Can't fork: $!") unless defined $pid;
                                    $ah->push_write("ERROR\r\n");
                                }
                            } else {
                                $ah->push_write("ERROR\r\n");
                            }
                        }
                        when ( "slave" ) {
                            my ($timestamp, ) = @args;
                            $timestamp ||= 0;
                            if ( $self->{ulog} ) {
                                open my $fh, "<", $self->{ulog} . "/ulog-" . $self->{server_id} . "-0001"
                                    or critf("Can't open ulog: $!");
                                my $ulog_handle = AnyEvent::Handle->new(
                                    fh => $fh,
                                    on_error => sub {
                                        my ($ah, $fatal, $msg) = @_;
                                        $ah->destroy;
                                    }
                                );
                                $ulog_handle->on_read(sub {
                                    shift->push_read(line => sub {
                                        my ($ulog_handle, $line) = @_;
                                        my ($ts, ) = split(/\t/, $line);
                                        if ( $ts >= $timestamp ) {
                                            $ah->push_write("SLAVE\t$line\r\n");
                                        }
                                    });
                                });
                                $ulog_handle->on_eof(sub {
                                    $ah->push_write("END\r\n");
                                    $self->{clients}->[fileno($fh)] = undef;
                                    shift->destroy;
                                });
                                $self->{clients}->[fileno($fh)] = $ulog_handle;
                            } else {
                                $ah->push_write("ERROR\r\n");
                            }
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

sub set {
    my ($self, $server_id, @keys) = @_;

    for my $key ( @keys ) {
        $self->{bloom}->add($key);
    }
    if ( $self->{ulog_handle} ) {
        $self->{stats}->{server_time} = gettimeofday * 10_0000;
        $self->{ulog_handle}->push_write(sprintf("%s\t%d\t%s\t%s\r\n",
            $self->{stats}->{server_time}, 
            $server_id,
            "set",
            join "\t", @keys
        ));
    }
}

sub update_timestamp {
    my ($self, $timestamp) = @_;

    open my $tx_handle, '>', "./slave.rts"
        or die "Can't open file: $!";

    $self->{timestamp_handle} = AnyEvent::Handle->new(
        fh => $tx_handle,
        on_error => sub {
            my ($ah, $fatal, $msg) = @_;
            $ah->destroy;
        }
    );
    $self->{stats}->{repl_timestamp} = $timestamp;
    $self->{timestamp_handle}->push_write("$timestamp\r\n")
}

sub repl_thread {
    my ($self, $ah, $timestamp) = @_;

    $ah->push_write(sprintf("slave %d\r\n", $timestamp || 0 ));
    $ah->on_read(sub {
        shift->push_read(line => sub {
            my ($ah, $line) = @_;
            if ( $line eq "END" ) {
                $ah->push_write(sprintf("slave %d\r\n", $self->{stats}->{repl_timestamp} || 0));
            } else {
                my ($slave, $tx, $server_id, $cmd, @keys) = split /\t/, $line;
                if ( $slave eq "SLAVE" ) {
                    if ( $server_id != $self->{server_id} ) {
                        $self->$cmd($server_id, @keys);
                    }
                    $self->update_timestamp($tx);
                } else {
                    warn $line;
                }
            }
        });
    });
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
