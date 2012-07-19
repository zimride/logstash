require "logstash/config/file"
require "logstash/filters"
require "logstash/inputs"
require "logstash/logging"
require "logstash/namespace"
require "logstash/outputs"
require "logstash/pipeline"
require "logstash/program"
require "logstash/threadwatchdog"
require "logstash/util"
require "optparse"
require "thread"
require "uri"

# TODO(sissel): only enable this if we are in debug mode.
# JRuby.objectspace=true

# Collect logs, ship them out.
class LogStash::Agent
  include LogStash::Program

  attr_reader :config
  attr_reader :inputs
  attr_reader :outputs
  attr_reader :filters
  attr_accessor :logger

  # flags
  attr_reader :config_path
  attr_reader :logfile
  attr_reader :verbose

  public
  def initialize
    log_to(STDERR)
    @config_path = nil
    @config_string = nil
    @logfile = nil

    # flag/config defaults
    @verbose = 0
    @filterworker_count = 1

    @plugins = {}
    @plugins_mutex = Mutex.new
    @plugin_setup_mutex = Mutex.new
    @outputs = []
    @inputs = []
    @filters = []

    @plugin_paths = []
    @reloading = false

    # Add logstash's plugin path (plugin paths must contain inputs, outputs, filters)
    @plugin_paths << File.dirname(__FILE__)

    # TODO(sissel): Other default plugin paths?

    Thread::abort_on_exception = true
    @is_shutting_down = false
  end # def initialize

  public
  def log_to(target)
    @logger = LogStash::Logger.new(target)
  end # def log_to

  private
  def options(opts)
    opts.on("-f CONFIGPATH", "--config CONFIGPATH",
            "Load the logstash config from a specific file or directory. " \
            "If a direcory is given instead of a file, all files in that " \
            "directory will be concatonated in lexicographical order and " \
            "then parsed as a single config file.") do |arg|
      @config_path = arg
    end # -f / --config

    opts.on("-e CONFIGSTRING",
            "Use the given string as the configuration data. Same syntax as " \
            "the config file. If not input is specified, " \
            "'stdin { type => stdin }' is default. If no output is " \
            "specified, 'stdout { debug => true }}' is default.") do |arg|
      @config_string = arg
    end # -e

    opts.on("-w COUNT", "--filterworkers COUNT", Integer,
            "Run COUNT filter workers (default: 1)") do |arg|
      @filterworker_count = arg
      if @filterworker_count <= 0
        raise ArgumentError, "filter worker count must be > 0"
      end
    end # -w

    opts.on("-l", "--log FILE", "Log to a given path. Default is stdout.") do |path|
      @logfile = path
    end

    opts.on("-v", "Increase verbosity") do
      @verbose += 1
    end

    opts.on("-V", "--version", "Show the version of logstash") do
      require "logstash/version"
      puts "logstash #{LOGSTASH_VERSION}"
      exit(0)
    end

    opts.on("-p PLUGIN_PATH", "--pluginpath PLUGIN_PATH",
            "A colon-delimited path to find plugins in.") do |path|
      path.split(":").each do |p|
        @plugin_paths << p unless @plugin_paths.include?(p)
      end
    end
  end # def options

  # Parse options.
  private
  def parse_options(args)
    @opts = OptionParser.new

    # Step one is to add agent flags.
    options(@opts)

    # TODO(sissel): Check for plugin_path flags, add them to @plugin_paths.
    args.each_with_index do |arg, index|
      next unless arg =~ /^(?:-p|--pluginpath)(?:=(.*))?$/
      path = $1
      if path.nil?
        path = args[index + 1]
      end

      @plugin_paths += path.split(":")
    end # args.each

    # At this point, we should load any plugin-specific flags.
    # These are 'unknown' flags that begin --<plugin>-flag
    # Put any plugin paths into the ruby library path for requiring later.
    @plugin_paths.each do |p|
      @logger.debug("Adding to ruby load path", :path => p)
      $:.unshift p
    end

    # TODO(sissel): Go through all inputs, filters, and outputs to get the flags.
    # Add plugin flags to @opts

    # Load any plugins that we have flags for.
    # TODO(sissel): The --<plugin> flag support currently will load
    # any matching plugins input, output, or filter. This means, for example,
    # that the 'amqp' input *and* output plugin will be loaded if you pass
    # --amqp-foo flag. This might cause confusion, but it seems reasonable for
    # now that any same-named component will have the same flags.
    plugins = []
    args.each do |arg|
      # skip things that don't look like plugin flags
      next unless arg =~ /^--[A-z0-9]+-/
      name = arg.split("-")[2]  # pull the plugin name out

      # Try to load any plugin by that name
      %w{inputs outputs filters}.each do |component|
        @plugin_paths.each do |path|
          plugin = File.join(path, component, name) + ".rb"
          @logger.debug("Plugin flag found; trying to load it",
                        :flag => arg, :plugin => plugin)
          if File.file?(plugin)
            @logger.info("Loading plugin", :plugin => plugin)
            require plugin
            [LogStash::Inputs, LogStash::Filters, LogStash::Outputs].each do |c|
              # If we get flag --foo-bar, check for LogStash::Inputs::Foo
              # and add any options to our option parser.
              klass_name = name.capitalize
              if c.const_defined?(klass_name)
                @logger.debug("Found plugin class", :class => "#{c}::#{klass_name})")
                klass = c.const_get(klass_name)
                # See LogStash::Config::Mixin::DSL#options
                klass.options(@opts)
                plugins << klass
              end # c.const_defined?
            end # each component type (input/filter/outputs)
          end # if File.file?(plugin)
        end # @plugin_paths.each
      end # %{inputs outputs filters}.each

      #if !found
        #@logger.fatal("Flag #{arg.inspect} requires plugin #{name}, but no plugin found.")
        #return false
      #end
    end # @remaining_args.each

    begin
      remainder = @opts.parse(args)
    rescue OptionParser::InvalidOption => e
      @logger.info("Invalid option", :exception => e)
      raise e
    end

    return remainder
  end # def parse_options

  private
  def configure
    if @config_path && @config_string
      @logger.fatal("Can't use -f and -e at the same time")
      raise "Configuration problem"
    elsif (@config_path.nil? || @config_path.empty?) && @config_string.nil?
      @logger.fatal("No config file given. (missing -f or --config flag?)")
      @logger.fatal(@opts.help)
      raise "Configuration problem"
    end

    #if @config_path and !File.exist?(@config_path)
    if @config_path and Dir.glob(@config_path).length == 0
      @logger.fatal("Config file does not exist.", :path => @config_path)
      raise "Configuration problem"
    end

    if @logfile
      logfile = File.open(@logfile, "a")
      STDOUT.reopen(logfile)
      STDERR.reopen(logfile)
    end

    if @verbose >= 3  # Uber debugging.
      @logger.level = :debug
      $DEBUG = true
    elsif @verbose == 2 # logstash debug logs
      @logger.level = :debug
    elsif @verbose == 1 # logstash info logs
      @logger.level = :info
    else # Default log level
      @logger.level = :warn
    end
  end # def configure

  def read_config
    if @config_path
      # Support directory of config files.
      # https://logstash.jira.com/browse/LOGSTASH-106
      if File.directory?(@config_path)
        @logger.debug("Config path is a directory, scanning files",
                      :path => @config_path)
        paths = Dir.glob(File.join(@config_path, "*")).sort
      else
        # Get a list of files matching a glob. If the user specified a single
        # file, then this will only have one match and we are still happy.
        paths = Dir.glob(@config_path)
      end

      concatconfig = []
      paths.each do |path|
        concatconfig << File.new(path).read
      end
      config = LogStash::Config::File.new(nil, concatconfig.join("\n"))
    else # @config_string
      # Given a config string by the user (via the '-e' flag)
      config = LogStash::Config::File.new(nil, @config_string)
    end
    config.logger = @logger
    config
  end

  # Parses a config and returns [inputs, filters, outputs]
  def parse_config(config)
    inputs = []
    filters = []
    outputs = []
    config.parse do |plugin|
      # 'plugin' is a has containing:
      #   :type => the base class of the plugin (LogStash::Inputs::Base, etc)
      #   :plugin => the class of the plugin (LogStash::Inputs::File, etc)
      #   :parameters => hash of key-value parameters from the config.
      type = plugin[:type].config_name  # "input" or "filter" etc...
      klass = plugin[:plugin]

      # Create a new instance of a plugin, called like:
      # -> LogStash::Inputs::File.new( params )
      instance = klass.new(plugin[:parameters])
      instance.logger = @logger

      case type
        when "input"
          inputs << instance
        when "filter"
          filters << instance
        when "output"
          outputs << instance
        else
          msg = "Unknown config type '#{type}'"
          @logger.error(msg)
          raise msg
      end # case type
    end # config.parse
    return inputs, filters, outputs
  end

  public
  def run(args, &block)
    @logger.info("Register signal handlers")
    register_signal_handlers

    @logger.info("Parse options ")
    remaining = parse_options(args)
    if remaining == false
      raise "Option parsing failed. See error log."
    end

    @logger.info("Configure")
    configure

    # Load the config file
    @logger.info("Read config")
    config = read_config

    @logger.info("Start thread")
    @thread = Thread.new do
      LogStash::Util::set_thread_name(self.class.name)
      run_with_config(config, &block)
    end

    return remaining
  end # def run

  public
  def wait
    @thread.join
    return 0
  end # def wait

  public
  def run_with_config(config)
    @inputs, @filters, @outputs = parse_config(config)

    # If we are given a config string (run usually with
    # 'agent -e "some config string"') # then set up some defaults.
    if @config_string
      require "logstash/inputs/stdin"
      require "logstash/outputs/stdout"

      # All filters default to 'stdin' type
      @filters.each do |filter|
        filter.type = "stdin" if filter.type.nil?
      end

      # If no inputs are specified, use stdin by default.
      @inputs = [LogStash::Inputs::Stdin.new("type" => [ "stdin" ])] if @inputs.length == 0

      # If no outputs are specified, use stdout in debug mode.
      @outputs = [LogStash::Outputs::Stdout.new("debug" => [ "true" ])] if @outputs.length == 0
    end # if @config_string

    if @inputs.length == 0 or @outputs.length == 0
      raise "Must have both inputs and outputs configured."
    end

    # Create our pipeline
    @pipeline = LogStash::Pipeline.new
    @pipeline.logger = @logger

    @inputs.each { |i| @pipeline.add_plugin(:input, i) }
    @filters.each { |f| @pipeline.add_plugin(:filter, f) }
    @outputs.each { |o| @pipeline.add_plugin(:output, o) }

    @pipeline.run
  end # def run

  public
  def register_signal_handlers
    Signal.trap("INT") do
      @logger.warn("SIGINT received, shutting down.")
      shutdown
    end

    Signal.trap("HUP") do
      @logger.warn("SIGHUP received, reloading.")
      reload
    end

    Signal.trap("TERM") do
      @logger.warn("SIGTERM received, shutting down.")
      shutdown
    end
  end # def register_signal_handlers
end # class LogStash::Agent
