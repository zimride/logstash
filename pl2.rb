$: << "lib"
require "logstash/config/file"
require "thread"
require "cabin"

class Pipeline
  class ShutdownSignal; end

  def initialize(configstr)
    @logger = Cabin::Channel.get
    @logger.subscribe(STDOUT)
    @logger.level = :info

    # hacks for now to parse a config string
    config = LogStash::Config::File.new(nil, configstr)
    @inputs, @filters, @outputs = config.apply

    @inputs.collect(&:register)
    @filters.collect(&:register)
    @outputs.collect(&:register)

    @input_to_filter = SizedQueue.new(16)
    @filter_to_output = SizedQueue.new(16)

    # If no filters, pipe inputs to outputs
    if @filters.empty?
      @input_to_filter = @filter_to_output
    end
  end

  def run
    input_count = @inputs.count
    input_completion_queue = Queue.new

    @input_threads = @inputs.collect do |input|
      # one thread per input
      Thread.new(input) do |input|
        Thread.current[:name] = input.class
        inputworker(input)
        input_completion_queue << true
      end
    end

    # one filterworker thread, for now
    if @filters.any?
      @filter_thread = Thread.new(@filters, @input_to_filter, @filter_to_output) do |filters, input_queue, output_queue|
        Thread.current[:name] = "filter"
        filterworker(filters, input_queue, output_queue)
      end
    end

    # one outputworker thread
    @output_thread = Thread.new(@outputs, @filter_to_output) do |outputs, queue|
      Thread.current[:name] = "output"
      outputworker(outputs, queue)
    end

    # Wait for all inputs to finish, then shutdown.
    input_count.times { input_completion_queue.pop }
    shutdown
  end # def run

  def shutdown
    @logger.info("Shutting down")
    # Shutdown all inputs
    @inputs.each(&:teardown)

    # Send shutdown signal to the filters/outputs
    @input_to_filter << ShutdownSignal

    # Wait for everything to die.
    [*@input_threads, @filter_thread, @output_thread].each do |thread|
      next if thread.nil? # skip filter_thread if it doesn't exist.

      # thread.join(<some timeout>) returns nil when the thread is still running
      while thread.join(1).nil?
        @logger.info("Waiting for thread to finish", "thread" => thread[:name])
      end
    end
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

  def filterworker(filters, input_queue, output_queue)
    while true
      event = input_queue.pop
      if event == ShutdownSignal
        # push the signal down the pipeline to the outputs.
        output_queue << event
        break
      end

      filters.each do |filter|
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
          @logger.warn("An error occurred inside the #{filter.class.config_name}" \
                       " filter. LogStash has recovered from this and will " \
                       "continue operating normally. However, this error " \
                       "may be due to a bug, so feel free to file a ticket " \
                       "with this full error message at " \
                       "http://logstash.jira.com/",
                       "plugin" => filter.inspect, "event" => event,
                       "exception" => e)
        end # begin/rescue
      end # @filters.each

      # Ship the event down the pipeline
      if !event.cancelled?
        output_queue << event
      end
    end

    filters.each(&:teardown)
  end # def filterworker

  def outputworker(outputs, queue)
    while true
      event = queue.pop
      break if event == ShutdownSignal
      outputs.each do |output|
        begin
          output.receive(event) if output.output?(event)
        rescue => e
          @logger.error("Exception in plugin #{output.class}",
                        "plugin" => output.inspect, "exception" => e)
        end
      end
    end
    outputs.each(&:teardown)
  end # def filterworker
end # class Pipeline

start = Time.now
Pipeline.new(ARGV[0]).run
puts "duration: #{Time.now - start}"
