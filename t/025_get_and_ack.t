use strict;
use warnings;

use Test::Most;
use Test::Exception;

use FindBin qw/ $Bin /;
use lib $Bin;
use Net::AMQP::RabbitMQ::PP::Test;

my $host = $ENV{MQHOST};

my $basename     = 'perl_test_get_and_ack';
my $exchangename = $basename . '_exchange';
my $routing_key  = $basename . '_key';
my $channel      = 1;
my $payload      = 'Magic Transient Payload';

my %credentials = (
    host     => $host,
    username => 'guest',
    password => 'guest',
);

use_ok('Net::AMQP::RabbitMQ::PP');

ok(
    my $mq = Net::AMQP::RabbitMQ::PP->new(),
    'new Net::AMQP::RabbitMQ::PP object'
);

lives_ok {
    $mq->connect( %credentials );
}
'connect';

lives_ok {
    $mq->channel_open( channel => $channel, );
}
'channel_open';

my $queuename = '';
lives_ok {
    $queuename = $mq->queue_declare(
        channel     => 1,
        queue       => '',
        durable     => 0,
        exclusive   => 0,
        auto_delete => 1,
    )->queue;
}
'queue_declare';

my %queueparams = (
    channel => $channel,
    queue   => $queuename,
);

my %exchangeparams = (
    channel  => $channel,
    exchange => $exchangename,
);

lives_ok {
    $mq->exchange_declare(
        %exchangeparams,
        exchange_type => 'direct',
    );
}
'exchange_declare';

lives_ok {    ## make sure the queue is empty.
    $mq->queue_purge( %queueparams );
}
'queue_purge';

lives_ok {
    $mq->queue_bind(
        %queueparams,
        %exchangeparams,
        routing_key => $routing_key,
    );
}
'queue_bind';

my $getr;
lives_ok {
    $getr = $mq->basic_get( %queueparams );
}
'basic_get';

is_deeply( $getr, undef, 'basic_get should return empty before publish' );

lives_ok {
    $mq->basic_publish(
        %exchangeparams,
        routing_key => $routing_key,
        payload     => $payload,
    );
}
'basic_publish';

lives_ok {
    $getr = $mq->basic_get( %queueparams );
}
'basic_get after publish';

my $expected_message = {
    content_header_frame => Net::AMQP::Frame::Header->new(
        body_size    => length $payload,
        weight       => 0,
        payload      => '',
        type_id      => 2,
        class_id     => 60,
        channel      => 1,
        header_frame => Net::AMQP::Protocol::Basic::ContentHeader->new(),
    ),
    delivery_tag => 1,
    payload      => $payload,
};

is_deeply(    ## check message 1
    $getr,
    $expected_message,
    'basic_get should see message'
);

lives_ok {
    $mq->disconnect;
}
'disconnect without ack. Message should requeue';

lives_ok {
    $mq->connect( %credentials );
}
're connect';

lives_ok {
    $mq->channel_open( channel => $channel );
}
'channel_open';

lives_ok {
    $mq->exchange_declare(
        %exchangeparams,
        exchange_type => 'direct',
    );
}
'exchange_declare';

lives_ok {
    $mq->queue_bind(
        %queueparams,
        %exchangeparams,
        routing_key  => $routing_key,
    );
}
"queue_bind";

lives_ok {
    $getr = $mq->basic_get( %queueparams );
}
'basic_get after reconnect';

is_deeply(
    $getr,
    $expected_message,
    'basic_get should still see the same message'
);

lives_ok {
    $mq->basic_ack(
        channel      => $channel,
        delivery_tag => $getr->{delivery_tag},
    );
}
'basic_ack';

lives_ok {
    $getr = $mq->basic_get(
        channel => 1,
        queue   => $queuename,
    );
}
'basic_get after ack';

is_deeply( $getr, undef, 'basic_get should return empty after ack' );

## Clean up:
lives_ok {
    $mq->queue_unbind(
        %queueparams,
        %exchangeparams,
        routing_key => $routing_key,
    );
}
'queue_unbind';

lives_ok {
    $mq->exchange_delete( %exchangeparams );
}
'exchange_delete';

lives_ok {
    $mq->queue_delete( %queueparams );
}
'queue_delete';

done_testing();
exit;
