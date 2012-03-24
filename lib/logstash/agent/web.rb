require "sinatra/base"
$:.unshift File.join(File.dirname(__FILE__), "lib")
require "ftw/websocket/rack"
require "cabin"
require "cabin/web/logs"

class LogStash::Agent::Web < Sinatra::Base
  class RootApp < Sinatra::Base
    get "/" do
      redirect "/logs" # Served by Cabin::Web::Logs
    end
  end

  enable :logging
  use RootApp
  use Cabin::Web::Logs
  use Rack::Logger
  use Rack::CommonLogger
  use Rack::Logger

  before do
    env["rack.logger"] = Cabin::Channel.get
  end

  def route_missing(*args)
    return [404, {}, "FAIL"]
  end

  not_found do
    return [404, {}, "Failure"]
  end

  error do
    return [500, {}, env["sinatra.error"].inspect]
  end

  helpers do
    def logger
      Cabin::Channel.get
    end
  end
end # class LogStash::Agent::Web

