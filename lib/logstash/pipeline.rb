require "logstash/filterworker"
require "logstash/inputworker"
require "logstash/multiqueue"
require "logstash/namespace"
require "logstash/outputworker"
require "logstash/sized_queue"
require "logstash/util"

# A logstash event pipeline.
#
# * Inputs (the start of a pipeline, aka faucet, source, reader)
#   Inputs are unordered. There is one thread per input. Inputs inject
#   events into the pipeline.
#
# * Filters (a pipeline processor, aka processor, decorator)
#   Filters are ordered. There are a specified number of "filter worker"
#   threads. Each filter worker thread handles one event at a time,
#   and runs filters (in order) on the event. Filters modify events coming
#   through the pipeline (and may drop them, or generate more events).
#
# * Outputs (the end of a pipeline, aka sink, drain, writer))
#   Outputs are unordered. There is one thread per output.
class LogStash::Pipeline
  attr_accessor :logger
  attr_reader :inputs
  attr_reader :filters
  attr_reader :outputs
  attr_reader :status

  # Constants for our pipeline status
  STATUS_NOTRUNNING = :notrunning
  STATUS_RUNNING = :running
  STATUS_SHUTTING_DOWN = :shutting_down
  STATUS_SHUTDOWN = :shutdown

  # Initialize a new pipeline. Available options:
  #  +name+: name of this pipeline (used in thread titles)
  #  +filter_workers+: number of filter worker threads
  public
  def initialize(opts = {})
    @status = STATUS_NOTRUNNING
    @opts = {
        :name => "pipeline-#{Thread.current.object_id}",
        :filter_workers => 1,
    }.merge(opts)

    if ! @opts[:filter_workers] === Integer
      raise ArgumentError, ":filter_workers must be an Integer"
    end

    @inputs = []
    @filters = []
    @outputs = []
    @logger = Logger.new(STDOUT)

    @plugin_setup_mutex = Mutex.new

    @filter_workers = []
    @input_threads = []
    @output_threads = []
    @filter_queue = LogStash::SizedQueue.new(10 * @opts[:filter_workers])
    @output_queue = LogStash::MultiQueue.new
  end # def initialize

  public
  def add_plugin(type, plugin)
    case type
    when :input
      if ! plugin.is_a?(LogStash::Inputs::Base)
        raise ArgumentError, "#{plugin} is not a LogStash::Inputs::Base"
      end
      @inputs << plugin
    when :filter
      if ! plugin.is_a?(LogStash::Filters::Base)
        raise ArgumentError, "#{plugin} is not a LogStash::Filters::Base"
      end
      @filters << plugin
    when :output
      if ! plugin.is_a?(LogStash::Outputs::Base)
        raise ArgumentError, "#{plugin} is not a LogStash::Outputs::Base"
      end
      @outputs << plugin
    else
      raise ArgumentError, "unknown plugin type #{type.inspect}"
    end
  end # def add_plugin

  # Run the pipeline.  This function will block until the pipeline is over.
  #  - one per input
  #  - N filter workers
  #  - one per output
  #  - a supervisor thread
  public
  def run
    _start_outputs
    _start_filterworkers
    # TODO(petef): LogStash::ThreadWatchdog for filters
    _start_inputs

    # The pipeline is over if:
    #  * our inputs are finished
    #  * someone called Pipeline#shutdown
    while sleep(3)
      @input_threads.delete_if { |i| ! i.alive? }
      if @input_threads.length == 0
        @logger.warn("No input threads, ending pipeline",
                     :pipeline => @opts[:name])
        break
      end
    end

    _full_shutdown
  end # def run

  public
  def shutdown
    if @status != STATUS_SHUTDOWN
      @status = STATUS_SHUTTING_DOWN
  end

  private
  def _register_outputs
    @outputs.each do |output|
      output.logger = @logger
      @plugin_setup_mutex.synchronize { output.register }
    end
  end

  private
  def _start_outputs
    _register_outputs

    @outputs.each do |output|
      queue = LogStash::SizedQueue.new(10 * @opts[:filter_workers])
      @output_queue.add_queue(queue)
      output_worker = LogStash::OutputWorker.new(output, queue)
      output_worker.logger = @logger
      output_thread = Thread.new do
        LogStash::Util::set_thread_name("output|#{output.to_s}")
        output_worker.run
      end
      @output_threads << output_thread
    end
  end

  private
  def _register_filters
    @filters.each do |filter|
      filter.logger = @logger
      @plugin_setup_mutex.synchronize { filter.register }
    end
  end

  private
  def _start_filterworkers
    _register_filters

    if @filters.length == 0
      @logger.debug("no filters defined, not starting any filter workers",
                    :pipeline => @opts[:name])
    end

    # If we have more than one filter worker, make sure our filters
    # are marked as thread safe.
    if @opts[:filter_workers] > 1
      @filters.each do |filter|
        if ! filter.threadsafe?
          raise "#{filter} is not thread-safe; try dropping to 1 worker"
        end
      end
    end

    # Start N filter workers.
    @opts[:filter_workers].times do |n|
      filter_worker = LogStash::FilterWorker.new(@filters, @filter_queue,
                                                  @output_queue)
      filter_worker.logger = @logger
      filter_thread = Thread.new do
        LogStash::Util::set_thread_name("filterworker|#{n}")
        filter_worker.run
      end
      @filter_workers << filter_thread
    end
  end # def _start_filterworkers

  private
  def _register_inputs
    @inputs.each do |input|
      input.logger = @logger
      @plugin_setup_mutex.synchronize { input.register }
    end
  end

  private
  def _start_inputs
    _register_inputs

    output_queue = @filters.length == 0 ? @output_queue : @filter_queue

    @inputs.each do |input|
      input_worker = LogStash::InputWorker.new(input, output_queue)
      input_worker.logger = @logger
      # TODO(petef): @input.threaded support
      input_thread = Thread.new do |*args|
        LogStash::Util::set_thread_name("input|#{input.to_s}")
        input_worker.run
      end
      @input_threads << input_thread
    end
  end # def _start_inputs

  private
  def _full_shutdown
    # Kill the filter workers.
    while @filter_workers.length > 0
      @filter_workers.delete_if { |t| ! t.alive? }
      @filter_queue << LogStash::SHUTDOWN

  end # def _full_shutdown
end # class LogStash::Pipeline
