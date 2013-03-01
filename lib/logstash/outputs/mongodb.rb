require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs::Mongodb < LogStash::Outputs::Base

  config_name "mongodb"
  plugin_status "beta"

  # a MongoDB URI to connect to
  # See http://docs.mongodb.org/manual/reference/connection-string/
  config :uri, :validate => :string, :required => true
  
  # The database to use
  config :database, :validate => :string, :required => true
   
  # The collection to use. This value can use %{foo} values to dynamically
  # select a collection based on data in the event.
  config :collection, :validate => :string, :required => true

  # If true, store the @timestamp field in mongodb as an ISODate type instead
  # of an ISO8601 string.  For more information about this, see
  # http://www.mongodb.org/display/DOCS/Dates
  config :isodate, :validate => :boolean, :default => false

  # Number of seconds to wait after failure before retrying
  config :waitTime, :validate => :number, :default => 3, :required => false

  public
  def register
    require "mongo"
    conn = Mongo::MongoClient.from_uri(@uri)
    @db = conn.db(@database)
  end # def register

  public
  def receive(event)
    return unless output?(event)

    begin
      if @isodate
        # the mongodb driver wants time values as a ruby Time object.
        # set the @timestamp value of the document to a ruby Time object, then.
        document = event.to_hash.merge("@timestamp" => event.ruby_timestamp)
        @db.collection(event.sprintf(@collection)).insert(document)
      else
        @db.collection(event.sprintf(@collection)).insert(event.to_hash)
      end
    rescue => e
      @logger.warn("Failed to send event to MongoDB", :event => event,
                   :exception => e, :backtrace => e.backtrace)
      sleep @waitTime
      retry
    end
  end # def receive
end # class LogStash::Outputs::Mongodb
