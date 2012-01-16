require "logstash/inputs/base"
require "logstash/namespace"
require "ffi-rzmq"
require "timeout"
require "logstash/util/zmq"

# Read events over a 0MQ socket.
#
# You need to have the 0mq 2.1.x library installed to be able to use
# this input plugin. You can download it at
# [zeromq.org](http://www.zeromq.org/intro:get-the-software)
#
# The default settings will create a subscriber binding to tcp://127.0.0.1:2120 
# waiting for connecting publishers.
#
class LogStash::Inputs::Zmq < LogStash::Inputs::Base

  config_name "zmq"
  plugin_status "experimental"

  # 0mq socket address to connect or bind to
  ## TODO(sissel): zmq lets you bind to multiple addresses, expose this feature?
  config :address, :validate => :string, :default => "tcp://127.0.0.1:2120"

  # 0mq queue size (See ZMQ_HWM in
  # [zmq_setsockopt(3)](http://api.zeromq.org/2-1:zmq-setsockopt))
  #
  # Quoting the zmq docs linked above:
  #
  # > The ZMQ_HWM option shall set the high water mark for the specified socket.
  # > The high water mark is a hard limit on the maximum number of outstanding
  # > messages ØMQ shall queue in memory for any single peer that the specified
  # > socket is communicating with.
  config :queue_size, :validate => :number, :default => 1000

  # 0mq topic to subscribe to. This is only valid for 'pubsub' message pattern.
  # (See ZMQ_SUBSCRIBE in 
  # [zmq_setsockopt(3)](http://api.zeromq.org/2-1:zmq-setsockopt))
  config :queue, :validate => :string, :default => "" # default all

  # Whether to bind ("server") or connect ("client") to the socket
  config :mode, :validate => [ "server", "client"], :default => "client"

  # The message pattern to use: "pubsub" or "pushpull"
  #
  # * pubsub is publisher-subscriber. This input will be a subscriber.
  # * pushpull is a one-way message 
  #
  # To learn more about these, check the [zeromq
  # guide](http://zguide.zeromq.org/page:all)
  config :message_pattern, :validate => ["pubsub", "pushpull"], :default => "pubsub"

  public
  def register
    self.class.send(:include, LogStash::Util::Zmq)

    case @message_pattern
    when "pubsub"
      @subscriber = context.socket(ZMQ::SUB)
    when "pushpull"
      @subscriber = context.socket(ZMQ::PULL)

    error_check(@subscriber.setsockopt(ZMQ::HWM, @queue_size))
    error_check(@subscriber.setsockopt(ZMQ::SUBSCRIBE, @queue))
    error_check(@subscriber.setsockopt(ZMQ::LINGER, 1))
    setup(@subscriber, @address)
  end # def register

  def teardown
    error_check(@subscriber.close)
  end # def teardown

  def server?
    @mode == "server"
  end # def server?

  def run(output_queue)
    begin
      loop do
        msg = ''
        rc = @subscriber.recv_string(msg)
        error_check(rc)
        @logger.debug("0mq: receiving", :event => msg)
        e = self.to_event(msg, @source)
        if e
          output_queue << e
        end
      end
    rescue => e
      @logger.debug("ZMQ Error", :subscriber => @subscriber,
                    :exception => e, :backtrace => e.backtrace)
    rescue Timeout::Error
      @logger.debug("Read timeout", subscriber => @subscriber)
    end # begin
  end # def run
end # class LogStash::Inputs::Zmq
