require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/redis_streams"
require "redis"

describe LogStash::Outputs::RedisStreams, :integration => true do
  
  # These tests require a running Redis instance
  # Skip if Redis is not available
  before(:all) do
    begin
      @redis = Redis.new(:host => "127.0.0.1", :port => 6379, :timeout => 1)
      @redis.ping
      @redis_available = true
    rescue
      @redis_available = false
    end
  end

  before(:each) do
    skip "Redis not available" unless @redis_available
    # Clean up any existing test streams
    test_streams = @redis.keys("test_stream*")
    @redis.del(*test_streams) unless test_streams.empty?
  end

  context "Basic stream operations" do
    let(:config) {
      {
        "host" => ["127.0.0.1"],
        "port" => 6379,
        "stream" => "test_stream"
      }
    }
    let(:redis_streams) { described_class.new(config) }

    it "should successfully send events to Redis Stream" do
      redis_streams.register

      event = LogStash::Event.new({
        "message" => "test message",
        "timestamp" => "2024-01-15T10:30:00Z",
        "level" => "INFO"
      })

      redis_streams.receive(event)
      sleep 0.1 # Give it a moment to process

      # Verify the stream was created and has entries
      stream_length = @redis.xlen("test_stream")
      expect(stream_length).to be > 0

      # Read the latest entry
      entries = @redis.xread("test_stream", "0-0", :count => 1)
      expect(entries).not_to be_empty
      
      stream_data = entries["test_stream"]
      expect(stream_data).not_to be_empty
      
      entry = stream_data.first[1]
      expect(entry["message"]).to eq("test message")
      expect(entry["level"]).to eq("INFO")
    end

    it "should handle dynamic stream names" do
      dynamic_config = config.merge("stream" => "test_stream_%{level}")
      redis_streams_dynamic = described_class.new(dynamic_config)
      redis_streams_dynamic.register

      event = LogStash::Event.new({
        "message" => "test message",
        "level" => "ERROR"
      })

      redis_streams_dynamic.receive(event)
      sleep 0.1

      # Verify the dynamic stream was created
      stream_length = @redis.xlen("test_stream_ERROR")
      expect(stream_length).to be > 0
    end
  end

  context "Stream partitioning" do
    context "Random partitioning" do
      let(:config) {
        {
          "host" => ["127.0.0.1"],
          "port" => 6379,
          "stream" => "test_stream",
          "enable_partitioning" => true,
          "partition_strategy" => "random",
          "partition_count" => 3
        }
      }
      let(:redis_streams) { described_class.new(config) }

      it "should distribute events across multiple streams" do
        redis_streams.register

        # Send multiple events
        10.times do |i|
          event = LogStash::Event.new({"message" => "test message #{i}"})
          redis_streams.receive(event)
        end
        sleep 0.1

        # Check that at least one partition was used
        partitioned_streams = @redis.keys("test_stream-*")
        expect(partitioned_streams.length).to be > 0
        expect(partitioned_streams.length).to be <= 3

        # Verify total events
        total_events = partitioned_streams.sum { |stream| @redis.xlen(stream) }
        expect(total_events).to eq(10)
      end
    end

    context "Hash partitioning" do
      let(:config) {
        {
          "host" => ["127.0.0.1"],
          "port" => 6379,
          "stream" => "test_stream",
          "enable_partitioning" => true,
          "partition_strategy" => "hash",
          "partition_field" => "user_id",
          "partition_count" => 4
        }
      }
      let(:redis_streams) { described_class.new(config) }

      it "should consistently route events with same field value to same partition" do
        redis_streams.register

        # Send events with same user_id
        3.times do |i|
          event = LogStash::Event.new({
            "message" => "test message #{i}",
            "user_id" => "user123"
          })
          redis_streams.receive(event)
        end
        sleep 0.1

        # All events should be in the same partition
        partitioned_streams = @redis.keys("test_stream-*")
        expect(partitioned_streams.length).to eq(1)
        
        stream_name = partitioned_streams.first
        expect(@redis.xlen(stream_name)).to eq(3)
      end
    end

    context "Time-based partitioning" do
      let(:config) {
        {
          "host" => ["127.0.0.1"],
          "port" => 6379,
          "stream" => "test_stream",
          "enable_partitioning" => true,
          "partition_strategy" => "time_based",
          "time_format" => "%Y-%m-%d-%H"
        }
      }
      let(:redis_streams) { described_class.new(config) }

      it "should create time-based stream names" do
        redis_streams.register

        event = LogStash::Event.new({"message" => "test message"})
        redis_streams.receive(event)
        sleep 0.1

        expected_suffix = Time.now.strftime("%Y-%m-%d-%H")
        expected_stream = "test_stream-#{expected_suffix}"
        
        expect(@redis.xlen(expected_stream)).to be > 0
      end
    end
  end

  context "Batch processing" do
    let(:config) {
      {
        "host" => ["127.0.0.1"],
        "port" => 6379,
        "stream" => "test_stream",
        "batch" => true,
        "batch_events" => 5,
        "batch_timeout" => 1
      }
    }
    let(:redis_streams) { described_class.new(config) }

    it "should batch events and flush them" do
      redis_streams.register

      # Send exactly batch_events number of events
      5.times do |i|
        event = LogStash::Event.new({"message" => "test message #{i}"})
        redis_streams.receive(event)
      end

      sleep 0.1 # Give it time to process the batch

      # Verify all events were written
      stream_length = @redis.xlen("test_stream")
      expect(stream_length).to eq(5)
    end
  end

  context "Stream length management" do
    let(:config) {
      {
        "host" => ["127.0.0.1"],
        "port" => 6379,
        "stream" => "test_stream",
        "maxlen" => 3,
        "approximate_trimming" => false
      }
    }
    let(:redis_streams) { described_class.new(config) }

    it "should trim stream to maxlen" do
      redis_streams.register

      # Send more events than maxlen
      5.times do |i|
        event = LogStash::Event.new({"message" => "test message #{i}"})
        redis_streams.receive(event)
      end
      sleep 0.1

      # Stream should be trimmed to maxlen
      stream_length = @redis.xlen("test_stream")
      expect(stream_length).to be <= 3
    end
  end
end