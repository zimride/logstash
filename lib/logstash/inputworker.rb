require "logstash/namespace"
require "logstash/logging"
require "logstash/plugin"
require "logstash/config/mixin"
require "logstash/util"

class LogStash::InputWorker < LogStash::Plugin
  attr_accessor :logger
  attr_reader :input

  def initialize(input, output_queue)
    @input = input
    @output_queue = output_queue
    @shutdown_requested = false
  end # def initialize

  def run
    done = false
    while !done
      begin
        @input.run(@output_queue)
        done = true
        input.finished
      rescue => e
        @logger.warn("Input thread exception", :plugin => input,
                     :exception => e, :backtrace => e.backtrace)
        @logger.error("Restarting input due to exception", :plugin => input)
        sleep(1)
        retry # This jumps to the top of the 'begin'
      end
    end # while !done
  end
end # class LogStash::InputWorker
