$: << "lib"
require "logstash/config/file"

class Pipeline
  class ShutdownSignal; end

  def initialize(configstr)
    # hacks for now to parse a config string
    config = LogStash::Config::File.new(nil, configstr)
    agent = LogStash::Agent.new
    @inputs, @filters, @outputs = agent.instance_eval { parse_config(config) }

    @inputs.collect(&:register)
    @filters.collect(&:register)
    @outputs.collect(&:register)

    @input_to_filter = SizedQueue(16)
    @filter_to_output = SizedQueue(16)

    # If no filters, pipe inputs to outputs
    if @filters.empty?
      input_to_filter = filter_to_output
    end
  end

  def run
    @input_threads = @inputs.collect do |input|
      # one thread per input
      Thread.new(input) do |input|
        inputworker(input)
      end
    end

    # one filterworker thread, for now
    @filter_thread = Thread.new(@filters) do |filters|
      filterworker(filters)
    end

    # one outputworker thread
    @output_thread = Thread.new(@outputs) do |outputs|
      outputworker(outputs)
    end

    # Now monitor input threads state
    # if all inputs are terminated, send shutdown signal to @input_to_filter
  end

  def inputworker(plugin)
    begin
      # TODO(sissel): Update to use plugin.run { |event| @input_to_filter << event }
      plugin.run(@input_to_filter)
    rescue ShutdownSignal
      plugin.teardown
    rescue => e
      # TODO(sissel): Race condition? What if we get a shutdown signal right now?
      @logger.error("Exception in plugin #{plugin.class}, restarting plugin.",
                    "plugin" => plugin.inspect, "exception" => e)
      plugin.teardown
      
      retry
    end
  end # def 

  def filterworker
    begin
      while true
        event << @input_to_filter
        break if event == ShutdownSignal

        @filters.each do |filter|
          begin
            if filter.filter?(event)
              success = filter.filter(event) do |newevent|
                # if we get a new event here, it's assumed a success
                # this will occur in filters like 'split' which turn
                # a multiline message into multiple events.
                filter.filter_matched(newevent) 
                @filter_to_output << newevent
              end
              if success
                # Apply any add_tag/add_field stuff if the filter was
                # successful.
                filter.filter_matched(event)
              else
                # Break filter iteration if the event was cancelled.
                if event.cancelled?
                  break
                end
              end
            end # if filter.filter?
          rescue => e
            @logger.warn("An error occurred inside the #{plugin.config_name}" \
                         "filter. LogStash has recovered from this and will " \
                         "continue operating normally. However, this error " \
                         "may be due to a bug, so feel free to file a ticket " \
                         "with this full error message at " \
                         "http://logstash.jira.com/",
                         "plugin" => plugin.inspect, "event" => event,
                         "exception" => e)
          end # begin/rescue
        end # @filters.each

        # Ship the event down the pipeline
        if !event.cancelled?
          @filter_to_output << event
        end
      end
    rescue => e
      @logger.error("Exception in plugin #{plugin.class}",
                    "plugin" => plugin.inspect, "exception" => e)
    end

    @filters.each(&:teardown)
  end # def filterworker

  def outputworker
    begin
      while true
        event << @filter_to_output
        break if event == :shutdown
        @outputs.each do |output|
          output.receive(event) if output.output?(event)
        end
      end
    rescue => e
      @logger.error("Exception in plugin #{plugin.class}",
                    "plugin" => plugin.inspect, "exception" => e)
    end
    @outputs.each(&:teardown)
  end # def filterworker
end # class Pipeline

trap("INT") do
  puts "SIGINT"; shutdown(input_threads, filter_thread, output_thread)
  exit 1
end

