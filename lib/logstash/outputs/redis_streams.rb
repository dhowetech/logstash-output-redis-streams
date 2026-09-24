# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "stud/buffer"
require "redis"
require "zlib"
require "connection_pool"

# This output will send events to Redis Streams using XADD.
# Redis Streams were introduced in Redis 5.0 and provide a powerful
# append-only data structure for message queuing and event sourcing.
# 
# For more information, see http://redis.io/[the Redis homepage]
#
class LogStash::Outputs::RedisStreams < LogStash::Outputs::Base

  include Stud::Buffer

  config_name "redis_streams"

  # Allow Logstash to run multiple pipeline workers concurrently against this
  # output. Safe because Redis client access is guarded by a connection pool
  # below (each worker checks out its own connection instead of contending
  # for a single shared socket).
  concurrency :shared

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

  # Number of Redis connections to keep in the pool. Since this output runs
  # with `concurrency :shared`, multiple Logstash pipeline workers may call
  # into it at the same time; each concurrent call checks out its own
  # connection from this pool instead of contending for a single shared
  # socket. Should generally be >= `pipeline.workers`.
  config :pool_size, :validate => :number, :default => 5

  # How long (in seconds) a worker thread will wait for a connection to
  # become available from the pool before raising an error.
  config :pool_timeout, :validate => :number, :default => 5

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

  # Base interval (in seconds) to wait before retrying after a failed send.
  # Retries use exponential backoff (reconnect_interval * 2^(attempt-1)),
  # capped by `max_reconnect_interval`, so a struggling Redis backs off
  # instead of hammering it while still failing fast overall.
  config :reconnect_interval, :validate => :number, :default => 1

  # Upper bound (in seconds) on the exponential backoff delay between retry
  # attempts, so a large `max_retries` can't stall the calling worker thread
  # for an unbounded amount of time on a single event/batch.
  config :max_reconnect_interval, :validate => :number, :default => 10

  # Maximum number of consecutive attempts to send an event (or a batch of
  # events) to Redis before giving up on it. Once this limit is reached the
  # event(s) are dropped (and logged at error level) instead of being retried
  # further.
  #
  # This protects the Logstash pipeline from blocking indefinitely when Redis
  # is unreachable or refuses writes (e.g. `OOM command not allowed` when
  # `maxmemory` is reached). Set to 0 to retry forever (legacy behavior) -
  # NOT recommended, since it will stall the whole pipeline while Redis stays
  # unavailable.
  config :max_retries, :validate => :number, :default => 3

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

    if @shuffle_hosts
        @host.shuffle!
    end
    @host_idx = 0
    @host_idx_mutex = Mutex.new

    # Connection pool so multiple concurrent pipeline workers (this output
    # runs with `concurrency :shared`) each get their own Redis connection
    # instead of contending for a single shared socket.
    @pool = ConnectionPool.new(size: @pool_size, timeout: @pool_timeout) { connect }

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
  #
  # NOTE: Stud::Buffer retries a raised error forever (with only a 1 second
  # sleep between attempts), which would block the whole Logstash pipeline
  # indefinitely if Redis stays unavailable/full. To avoid that, retries are
  # handled here instead, bounded by @max_retries; once exceeded, the batch
  # is dropped (logged at error level) and this method returns normally so
  # Stud::Buffer does not retry it again.
  def flush(events, stream_name, close=false)
    attempt = 0
    last_host, last_port = nil, nil

    begin
      @pool.with do |conn|
        last_host, last_port = conn.host, conn.port
        # Use Redis pipelining to send all XADD commands in a single network round-trip
        conn.redis.pipelined do |pipeline|
          events.each do |event_payload|
            xadd_stream_pipelined(pipeline, stream_name, event_payload, nil)
          end
        end
      end
    rescue => e
      attempt += 1
      @logger.warn("Failed to send batch of events to Redis Stream",
        :identity => identity(last_host, last_port), :exception => e, :attempt => attempt,
        :backtrace => e.backtrace
      )

      if @max_retries > 0 && attempt >= @max_retries
        @logger.error("Dropping batch of #{events.size} events after #{attempt} failed attempts to write to Redis Stream",
          :identity => identity(last_host, last_port))
        return
      end

      sleep backoff_interval(attempt)
      retry
    end
  end

  # called from Stud::Buffer#buffer_flush when an error occurs
  # (kept as a safety net; #flush handles its own retries/drops so this
  # should not normally be invoked)
  def on_flush_error(e)
    @logger.warn("Failed to send backlog of events to Redis",
      :exception => e,
      :backtrace => e.backtrace
    )
  end

  def close
    if @batch
      buffer_flush(:final => true)
    end
    @pool.shutdown { |conn| conn.redis.quit } if @pool
  end

  private

  # Redis connection paired with the host/port it was created against, so
  # log messages can identify which backend a failure came from without
  # relying on shared/racy instance state across concurrently-connecting
  # worker threads.
  RedisConnection = Struct.new(:redis, :host, :port)

  # Delay (in seconds) before the next retry attempt, using exponential
  # backoff based on @reconnect_interval, capped at @max_reconnect_interval.
  def backoff_interval(attempt)
    [@reconnect_interval * (2**(attempt - 1)), @max_reconnect_interval].min
  end

  def connect
    current_host, current_port = next_host_and_port

    params = {
      :host => current_host,
      :port => current_port,
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

    RedisConnection.new(Redis.new(params), current_host, current_port)
  end # def connect

  # Thread-safe round-robin host selection, since multiple pooled
  # connections may be established concurrently by different workers.
  def next_host_and_port
    host_str = nil
    @host_idx_mutex.synchronize do
      host_str = @host[@host_idx]
      @host_idx = @host_idx + 1 >= @host.length ? 0 : @host_idx + 1
    end

    current_host, current_port = host_str.split(':')
    current_port ||= @port
    [current_host, current_port]
  end

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

  # Builds a raw XADD command array compatible with the redis-rb `call`
  # interface. Used as a fallback on redis-rb 3.x (bundled with
  # logstash-input-redis, pinned to `redis ~> 3`), which predates the
  # gem's native `xadd` convenience method (added in redis-rb 4.x).
  def build_xadd_command(stream_name, payload, trim_options)
    args = [:xadd, stream_name]
    if trim_options[:maxlen]
      args << "MAXLEN"
      args << "~" if trim_options[:approximate]
      args << trim_options[:maxlen]
    elsif trim_options[:minid]
      args << "MINID"
      args << "~" if trim_options[:approximate]
      args << trim_options[:minid]
    end
    args << "*"
    args << @field
    args << payload
    args
  end

  def xadd_stream(redis, stream_name, payload, event = nil)
    trim_options = get_trim_options(event)
    if redis.respond_to?(:xadd)
      if trim_options.empty?
        redis.xadd(stream_name, @field => payload)
      else
        redis.xadd(stream_name, @field => payload, **trim_options)
      end
    else
      # redis-rb 3.x has no native `xadd`; issue the raw command instead.
      redis.call(*build_xadd_command(stream_name, payload, trim_options))
    end
  end

  def xadd_stream_pipelined(pipeline, stream_name, payload, event = nil)
    trim_options = get_trim_options(event)
    if pipeline.respond_to?(:xadd)
      if trim_options.empty?
        pipeline.xadd(stream_name, @field => payload)
      else
        pipeline.xadd(stream_name, @field => payload, **trim_options)
      end
    else
      # redis-rb 3.x's Pipeline#call takes the command as a single array.
      pipeline.call(build_xadd_command(stream_name, payload, trim_options))
    end
  end

  # A string used to identify a Redis instance in log messages
  def identity(host = nil, port = nil)
    password_part = @password ? "****@" : ""
    "redis://#{password_part}#{host}:#{port}/#{@db} stream:#{@stream}"
  end

  def send_to_redis_stream(event, payload)
    stream_name = get_stream_name(event)

    if @batch
      # Use batched method
      buffer_receive(payload, stream_name)
      return
    end

    attempt = 0
    last_host, last_port = nil, nil

    begin
      @pool.with do |conn|
        last_host, last_port = conn.host, conn.port
        xadd_stream(conn.redis, stream_name, payload, event)
      end
    rescue => e
      attempt += 1
      @logger.warn("Failed to send event to Redis Stream", :event => event,
                   :identity => identity(last_host, last_port), :exception => e, :attempt => attempt,
                   :backtrace => e.backtrace)

      if @max_retries > 0 && attempt >= @max_retries
        @logger.error("Dropping event after #{attempt} failed attempts to write to Redis Stream",
          :event => event, :identity => identity(last_host, last_port))
        return
      end

      sleep backoff_interval(attempt)
      retry
    end
  end
end