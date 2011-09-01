require "logstash/namespace"

# Track performance on events by type.
class LogStash::EventProfiler
  @@filter_time = Hash.new { |h, k| h[k] = {} }
  @@filter_calls = Hash.new { |h, k| h[k] = {} }
  @@filter_lock = Hash.new { |h, k| h[k] = Mutex.new }

  public
  def self.record_profile(type, filter, time)
    @@filter_lock[type].synchronize do
      @@filter_time[type][filter] ||= 0
      @@filter_time[type][filter] += time
      @@filter_calls[type][filter] ||= 0
      @@filter_calls[type][filter] += 1
    end
  end # def self.record_profile

  public
  def self.filter_time; return @@filter_time; end
  def self.filter_calls; return @@filter_calls; end
end # class LogStash::Event
