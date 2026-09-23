# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "stud/buffer"
require "redis"
require "zlib"

# This output will send events to Redis Streams using XADD.
# Redis Streams were introduced in Redis 5.0 and provide a powerful
# append-only data structure for message queuing and event sourcing.
# 
# For more information, see http://redis.io/[the Redis homepage]
#
class LogStash::Outputs::RedisStreams < LogStash::Outputs::Base

  include Stud::Buffer

  config_name "redis_streams"

  default :codec, "json"

  # The hostname(s) of your Redis server(s). Ports may be specified on any
  # hostname, which will override the global port config.
  # If the hosts list is an array, Logstash will pick one random host to connect to,
  # if that host is disconnected it will then pick another.
  #
  # For example:
  # [source,ruby]
  #     "127.0.0.1"
  #     ["127.0.0.1", "127.0.0.2"]
  #     ["127.0.0.1:6380", "127.0.0.1"]
  config :host, :validate => :array, :default => ["127.0.0.1"]

  # Shuffle the host list during Logstash startup.
  config :shuffle_hosts, :validate => :boolean, :default => true

  # The default port to connect on. Can be overridden on any hostname.
  config :port, :validate => :number, :default => 6379

  # SSL
  config :ssl_enabled, :validate => :boolean, :default => false

  # Validate the certificate chain against these authorities. You can define multiple files.
  # All the certificates will be read and added to the trust store.
  config :ssl_certificate_authorities, :validate => :path, :list => true

  # Options to verify the server's certificate.
  # "full": validates that the provided certificate has an issue date that's within the not_before and not_after dates;
  # chains to a trusted Certificate Authority (CA); has a hostname or IP address that matches the names within the certificate.
  # "none": performs no certificate validation. Disabling this severely compromises security (https://www.cs.utexas.edu/~shmat/shmat_ccs12.pdf)
  config :ssl_verification_mode, :validate => %w[full none], :default => 'full'

  # SSL certificate path
  config :ssl_certificate, :validate => :path

  # SSL key path
  config :ssl_key, :validate => :path

  # SSL key passphrase
  config :ssl_key_passphrase, :validate => :password, :default => nil

  # NOTE: the default setting [] uses SSL engine defaults
  config :ssl_supported_protocols, :validate => %w[TLSv1.1 TLSv1.2 TLSv1.3], :default => [], :list => true

  # The list of ciphers suite to use
  config :ssl_cipher_suites, :validate => :string, :list => true

  # The Redis database number.
  config :db, :validate => :number, :default => 0

  # Redis connection timeout in seconds.
  config :connect_timeout, :validate => :number, :default => 5

  # Redis read timeout in seconds.
  config :read_timeout, :validate => :number, :default => 5

  # Redis write timeout in seconds.
  config :write_timeout, :validate => :number, :default => 5

  # Password to authenticate with.  There is no authentication by default.
  config :password, :validate => :password

  # The name of a Redis stream. Dynamic names are valid here, for example `logstash-%{type}`.
  config :stream, :validate => :string, :required => true
  config :field, :validate => :string, :default => "body"

  # Enable stream partitioning to distribute events across multiple streams.
  # When enabled, multiple streams will be created based on the partitioning strategy.
  config :enable_partitioning, :validate => :boolean, :default => false

  # Number of stream partitions to create when partitioning is enabled.
  config :partition_count, :validate => :number, :default => 1

  # Partitioning strategy: "random", "hash", or "time_based"
  # - "random": Events are randomly distributed across partitions
  # - "hash": Events are distributed based on hash of a field value
  # - "time_based": Streams are named with time-based suffixes (e.g., stream-2024-01-15-10)
  config :partition_strategy, :validate => ["random", "hash", "time_based"], :default => "random"

  # Field to use for hash-based partitioning. Only used when partition_strategy is "hash".
  config :partition_field, :validate => :string, :default => "message"

  # Time format for time-based partitioning. Only used when partition_strategy is "time_based".
  # Supports strftime format. Examples:
  # - "%Y-%m-%d-%H" for hourly streams (stream-2024-01-15-10)
  # - "%Y-%m-%d" for daily streams (stream-2024-01-15)
  config :time_format, :validate => :string, :default => "%Y-%m-%d-%H"

  # Set to true if you want to batch events and use Redis pipelining to send multiple 
  # XADD commands in a single network round-trip for better performance.
  config :batch, :validate => :boolean, :default => false

  # If batch is set to true, the number of events we queue up before sending.
  config :batch_events, :validate => :number, :default => 50

  # If batch is set to true, the maximum amount of time between batch sends
  # when there are pending events to flush.
  config :batch_timeout, :validate => :number, :default => 5

  # Interval for reconnecting to failed Redis connections
  config :reconnect_interval, :validate => :number, :default => 1

  # Maximum length of each stream. When a stream reaches this length,
  # Redis will automatically trim it. Set to 0 to disable trimming.
  config :maxlen, :validate => :number, :default => 0

  # Use approximate trimming for better performance when maxlen is set.
  # This allows the stream to be slightly longer than maxlen for better performance.
  config :approximate_trimming, :validate => :boolean, :default => true

  # Maximum number of messages to keep in each stream. Alternative to maxlen.
  # When set, older messages beyond this count will be automatically trimmed.
  # Set to 0 to disable message count-based trimming.
  config :max_stream_size, :validate => :number, :default => 0

  # Stream retention time in seconds. Messages older than this will be automatically removed.
  # Uses MINID trimming based on Redis Stream ID timestamps (epoch milliseconds).
  # Set to 0 to disable time-based retention.
  config :stream_retention, :validate => :number, :default => 0

  def register
    validate_ssl_config!
    validate_partitioning_config!
    validate_stream_config!

    if @batch
      buffer_initialize(
        :max_items => @batch_events,
        :max_interval => @batch_timeout,
        :logger => @logger
      )
    end

    @redis = nil
    if @shuffle_hosts
        @host.shuffle!
    end
    @host_idx = 0

    @codec.on_event(&method(:send_to_redis_stream))
  end # def register

  def receive(event)
    begin
      @codec.encode(event)
    rescue StandardError => e
      @logger.warn("Error encoding event", :exception => e,
                   :event => event)
    end
  end # def receive

  # called from Stud::Buffer#buffer_flush when there are events to flush
  def flush(events, stream_name, close=false)
    @redis ||= connect
    
    # Use Redis pipelining to send all XADD commands in a single network round-trip
    @redis.pipelined do |pipeline|
      events.each do |event_payload|
        xadd_stream_pipelined(pipeline, stream_name, event_payload, nil)
      end
    end
  end

  # called from Stud::Buffer#buffer_flush when an error occurs
  def on_flush_error(e)
    @logger.warn("Failed to send backlog of events to Redis",
      :identity => identity,
      :exception => e,
      :backtrace => e.backtrace
    )
    @redis = connect
  end

  def close
    if @batch
      buffer_flush(:final => true)
    end
    if @redis
      @redis.quit
      @redis = nil
    end
  end

  private

  def connect
    @current_host, @current_port = @host[@host_idx].split(':')
    @host_idx = @host_idx + 1 >= @host.length ? 0 : @host_idx + 1

    if not @current_port
      @current_port = @port
    end

    params = {
      :host => @current_host,
      :port => @current_port,
      :connect_timeout => @connect_timeout,
      :read_timeout => @read_timeout,
      :write_timeout => @write_timeout,
      :db => @db,
      :ssl => @ssl_enabled,
    }

    params[:ssl_params] = setup_ssl_params if @ssl_enabled

    @logger.debug("connection params", params)

    if @password
      params[:password] = @password.value
    end

    Redis.new(params)
  end # def connect

  def setup_ssl_params
    require "openssl"

    params = {}
    params[:cert_store] = ssl_certificate_store

    if @ssl_verification_mode == 'none'
      params[:verify_mode] = OpenSSL::SSL::VERIFY_NONE
    else
      params[:verify_mode] = OpenSSL::SSL::VERIFY_PEER
    end

    if @ssl_certificate
      params[:cert] = OpenSSL::X509::Certificate.new(File.read(@ssl_certificate))
      if @ssl_key
        params[:key] = OpenSSL::PKey::RSA.new(File.read(@ssl_key), @ssl_key_passphrase.value || '')
      end
    end

    params[:min_version] = :TLS1_1
    if @ssl_supported_protocols.any?
      protocols = @ssl_supported_protocols.map { |v| v.delete('v').tr(".", "_").to_sym }.sort
      params[:min_version] = protocols.first
      params[:max_version] = protocols.last
    end

    params[:ciphers] = @ssl_cipher_suites if @ssl_cipher_suites&.any?
    params
  end

  def ssl_certificate_store
    cert_store = new_ssl_certificate_store
    cert_store.set_default_paths
    @ssl_certificate_authorities&.each do |cert|
      cert_store.add_file(cert)
    end

    cert_store
  end

  def new_ssl_certificate_store
    OpenSSL::X509::Store.new
  end

  def validate_ssl_config!
    unless @ssl_enabled
      ignored_ssl_settings = original_params.select { |k| k != 'ssl_enabled' && k.start_with?('ssl_') }
      @logger.warn("Configured SSL settings are not used when `ssl_enabled` is set to `false`: #{ignored_ssl_settings.keys}") if ignored_ssl_settings.any?
      return
    end

    if @ssl_certificate && !@ssl_key
      raise LogStash::ConfigurationError, "Using an `ssl_certificate` requires an `ssl_key`"
    elsif @ssl_key && !@ssl_certificate
      raise LogStash::ConfigurationError, 'An `ssl_certificate` is required when using an `ssl_key`'
    end
  end

  def validate_partitioning_config!
    if @enable_partitioning
      if @partition_count < 1
        raise LogStash::ConfigurationError, "partition_count must be greater than 0"
      end
      
      if @partition_strategy == "hash" && @partition_field.nil?
        raise LogStash::ConfigurationError, "partition_field is required when using hash partitioning strategy"
      end
    end
  end

  def validate_stream_config!
    if @field.nil? || @field.strip.empty?
      raise LogStash::ConfigurationError, "field must not be empty"
    end

    # Check for conflicting stream management configurations
    active_options = []
    active_options << "maxlen" if @maxlen > 0
    active_options << "max_stream_size" if @max_stream_size > 0
    active_options << "stream_retention" if @stream_retention > 0
    
    if active_options.length > 1
      raise LogStash::ConfigurationError, "Cannot specify multiple stream management options: #{active_options.join(', ')} - use only one"
    end

    # Validate retention settings
    if @stream_retention < 0
      raise LogStash::ConfigurationError, "stream_retention must be 0 or greater"
    end
  end


  def get_trim_options(event = nil)
    options = {}

    # Handle message count-based trimming (MAXLEN)
    maxlen_value = @maxlen > 0 ? @maxlen : @max_stream_size
    if maxlen_value > 0
      options[:maxlen] = maxlen_value
      options[:approximate] = @approximate_trimming
    end

    # Handle time-based retention (MINID)
    if @stream_retention > 0
      # Calculate the minimum ID threshold based on retention time
      # Redis Stream IDs are epoch milliseconds by default
      retention_ms = @stream_retention * 1000
      # Use event timestamp when available, fallback to current time
      event_time = event && event.timestamp ? event.timestamp.time : Time.now
      current_time_ms = (event_time.to_i * 1000)
      min_id_threshold = current_time_ms - retention_ms
      options[:minid] = "#{min_id_threshold}"
      # Note: When both MAXLEN and MINID are specified, Redis applies both
    end

    options
  end

  def get_stream_name(event)
    base_stream_name = event.sprintf(@stream)
    
    unless @enable_partitioning
      return base_stream_name
    end

    case @partition_strategy
    when "random"
      partition_num = rand(@partition_count)
      "#{base_stream_name}:#{partition_num}"
    when "hash"
      field_value = event.get(@partition_field)
      if field_value.nil?
        @logger.warn("Partition field '#{@partition_field}' not found in event, using partition 0")
        partition_num = 0
      else
        partition_num = Zlib.crc32(field_value.to_s).abs % @partition_count
      end
      "#{base_stream_name}:#{partition_num}"
    when "time_based"
      event_time = event.timestamp ? event.timestamp.time : Time.now
      time_suffix = event_time.strftime(@time_format)
      "#{base_stream_name}:#{time_suffix}"
    else
      base_stream_name
    end
  end

  def xadd_stream(stream_name, payload, event = nil)
    # Build the xadd arguments properly for the Redis gem
    trim_options = get_trim_options(event)
    if trim_options.empty?
      @redis.xadd(stream_name, @field => payload)
    else
      @redis.xadd(stream_name, @field => payload, **trim_options)
    end
  end

  def xadd_stream_pipelined(pipeline, stream_name, payload, event = nil)
    # Build the xadd arguments properly for the Redis gem in pipelined mode
    trim_options = get_trim_options(event)
    if trim_options.empty?
      pipeline.xadd(stream_name, @field => payload)
    else
      pipeline.xadd(stream_name, @field => payload, **trim_options)
    end
  end

  # A string used to identify a Redis instance in log messages
  def identity
    password_part = @password ? "****@" : ""
    "redis://#{password_part}#{@current_host}:#{@current_port}/#{@db} stream:#{@stream}"
  end

  def send_to_redis_stream(event, payload)
    stream_name = get_stream_name(event)

    if @batch
      # Use batched method
      buffer_receive(payload, stream_name)
      return
    end

    begin
      @redis ||= connect
      
      xadd_stream(stream_name, payload, event)
    rescue => e
      @logger.warn("Failed to send event to Redis Stream", :event => event,
                   :identity => identity, :exception => e,
                   :backtrace => e.backtrace)
      sleep @reconnect_interval
      @redis = nil
      retry
    end
  end
end