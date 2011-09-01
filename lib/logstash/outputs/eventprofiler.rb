require "logstash/outputs/base"
require "logstash/namespace"

# TODO(petef): statsd output

class LogStash::Outputs::EventProfiler < LogStash::Outputs::Base
  config_name "eventprofiler"

  # Output event profile statistics every :interval messages.
  config :interval, :default => 10, :validate => :number

  public
  def register
    @counter = 0
  end

  public
  def receive(event)
    if event == LogStash::SHUTDOWN
      finished
      return
    end

    @counter += 1
    if @counter == @interval.to_i
      profile = Hash.new { |h, k| h[k] = {} }
      LogStash::EventProfiler.filter_time.each do |type, info|
        info.each do |name, time|
          calls = LogStash::EventProfiler.filter_calls[type][name]
          profile[type][name] = {:calls => calls,
                                 :time => time,
                                 :average => time/calls,
          }
          profile[:global][name] ||= {:calls => 0, :time => 0, :average => 0}
          profile[:global][name][:calls] += calls
          profile[:global][name][:time] += time
          profile[:global][name][:average] = profile[:global][name][:time] /
                                             profile[:global][name][:calls]
        end
      end
      filter_calls = LogStash::EventProfiler.filter_calls
      @counter = 0
      puts "Event profiling: #{profile.inspect}"
    end
  end # def receive
end # class LogStash::Outputs::EventProfiler
