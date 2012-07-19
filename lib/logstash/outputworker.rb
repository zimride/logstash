require "logstash/namespace"
require "logstash/logging"
require "logstash/plugin"
require "logstash/config/mixin"
require "logstash/util"

class LogStash::OutputWorker < LogStash::Plugin
  attr_accessor :logger
  attr_reader :output

  public
  def initialize(output, input_queue)
    @output = output
    @input_queue = input_queue
  end # def initialize

  public
  def run
    begin
      while event = @input_queue.pop
        @output.handle(event)
        break if @output.finished?
      end
    rescue Exception => e
      @logger.warn("Output thread exception", :plugin => @output,
                   :exception => e, :backtrace => e.backtrace)
      sleep(1)
      retry
    end # begin/rescue
  end # def run
end # class LogStash::OutputWorker
