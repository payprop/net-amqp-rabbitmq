use strict;
use warnings;

use Test::Most;
use Test::Exception;
use English qw( -no_match_vars );

use FindBin qw/ $Bin /;
use lib $Bin;
use Net::AMQP::RabbitMQ::PP::Test;

my $host = $ENV{'MQHOST'};
my $username = "guest";
my $password = "guest";

my $default_frame_max = 131072;

use_ok( 'Net::AMQP::RabbitMQ::PP' );

ok( my $mq = Net::AMQP::RabbitMQ::PP->new() );

lives_ok {
	$mq->connect(
		host => $host,
		username => $username,
		password => $password,
		frame_max => $default_frame_max - 8,
	);
} 'frame_max lowered';
eval { $mq->disconnect(); };

lives_ok {
	$mq->connect(
		host => $host,
		username => $username,
		password => $password,
		frame_max => $default_frame_max + 8,
	);
} 'frame_max increased';
eval { $mq->disconnect(); };

my $exception;
if( $OSNAME =~ /MSWin32/ ) {
	$exception = qr/An existing connection was forcibly closed/;
}
else {
	$exception = qr/Connection reset by peer/;
}

# Connection frame must be > 4096
throws_ok {
	$mq->connect(
		host => $host,
		username => $username,
		password => $password,
		frame_max => 2048,
	);
} $exception, 'frame_max too small';

done_testing();
