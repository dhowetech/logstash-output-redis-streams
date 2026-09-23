require "logstash/devutils/rspec/spec_helper"
require "logstash/outputs/redis_streams"
require "logstash/json"
require "redis"
require "flores/random"
require "flores/pki"

describe LogStash::Outputs::RedisStreams do

  context "Basic functionality" do
    let(:stream_name) { "test_stream" }
    let(:config) {
      {
        "stream" => stream_name
      }
    }
    let(:redis_streams) { described_class.new(config) }

    it "should register without error" do
      expect { redis_streams.register }.to_not raise_error
    end

    it "should have the correct config name" do
      expect(described_class.config_name).to eq("redis_streams")
    end

    it "should use json codec by default" do
      redis_streams.register
      expect(redis_streams.codec).to be_a(LogStash::Codecs::JSON)
    end
  end

  context "Stream partitioning" do
    context "when partitioning is disabled" do
      let(:config) {
        {
          "stream" => "test_stream",
          "enable_partitioning" => false
        }
      }
      let(:redis_streams) { described_class.new(config) }

      it "should return the base stream name" do
        redis_streams.register
        event = LogStash::Event.new({"message" => "test message"})
        stream_name = redis_streams.send(:get_stream_name, event)
        expect(stream_name).to eq("test_stream")
      end
    end

    context "when using random partitioning" do
      let(:config) {
        {
          "stream" => "test_stream",
          "enable_partitioning" => true,
          "partition_strategy" => "random",
          "partition_count" => 3
        }
      }
      let(:redis_streams) { described_class.new(config) }

      it "should return a partitioned stream name" do
        redis_streams.register
        event = LogStash::Event.new({"message" => "test message"})
        stream_name = redis_streams.send(:get_stream_name, event)
        expect(stream_name).to match(/^test_stream-[0-2]$/)
      end
    end

    context "when using hash partitioning" do
      let(:config) {
        {
          "stream" => "test_stream",
          "enable_partitioning" => true,
          "partition_strategy" => "hash",
          "partition_field" => "user_id",
          "partition_count" => 4
        }
      }
      let(:redis_streams) { described_class.new(config) }

      it "should return a consistent partitioned stream name for the same field value" do
        redis_streams.register
        event = LogStash::Event.new({"user_id" => "user123", "message" => "test message"})
        
        stream_name1 = redis_streams.send(:get_stream_name, event)
        stream_name2 = redis_streams.send(:get_stream_name, event)
        
        expect(stream_name1).to eq(stream_name2)
        expect(stream_name1).to match(/^test_stream-[0-3]$/)
      end

      it "should handle missing partition field gracefully" do
        redis_streams.register
        event = LogStash::Event.new({"message" => "test message"})
        
        stream_name = redis_streams.send(:get_stream_name, event)
        expect(stream_name).to eq("test_stream-0")
      end
    end

    context "when using time-based partitioning" do
      let(:config) {
        {
          "stream" => "test_stream",
          "enable_partitioning" => true,
          "partition_strategy" => "time_based",
          "time_format" => "%Y-%m-%d-%H"
        }
      }
      let(:redis_streams) { described_class.new(config) }

      it "should return a time-based partitioned stream name" do
        redis_streams.register
        event = LogStash::Event.new({"message" => "test message"})
        
        expected_suffix = Time.now.strftime("%Y-%m-%d-%H")
        stream_name = redis_streams.send(:get_stream_name, event)
        
        expect(stream_name).to eq("test_stream-#{expected_suffix}")
      end
    end
  end

  context "Configuration validation" do
    it "should require stream parameter" do
      config = {}
      expect { described_class.new(config) }.to raise_error(LogStash::ConfigurationError)
    end

    it "should reject an empty payload field" do
      redis_streams = described_class.new("stream" => "test_stream", "field" => " ")
      expect { redis_streams.register }.to raise_error(LogStash::ConfigurationError, /field must not be empty/)
    end

    it "should validate partition_count when partitioning is enabled" do
      config = {
        "stream" => "test_stream",
        "enable_partitioning" => true,
        "partition_count" => 0
      }
      redis_streams = described_class.new(config)
      expect { redis_streams.register }.to raise_error(LogStash::ConfigurationError, /partition_count must be greater than 0/)
    end

    it "should require partition_field for hash partitioning" do
      config = {
        "stream" => "test_stream",
        "enable_partitioning" => true,
        "partition_strategy" => "hash"
      }
      redis_streams = described_class.new(config)
      expect { redis_streams.register }.to raise_error(LogStash::ConfigurationError, /partition_field is required/)
    end
  end

  context "Batch mode" do
    let(:config) {
      {
        "stream" => "test_stream",
        "batch" => true,
        "batch_events" => 50,
        "batch_timeout" => 3600 * 24 # Large timeout to prevent auto-flush
      }
    }
    let(:redis_streams) { described_class.new(config) }

    it "should call buffer_receive in batch mode" do
      redis_streams.register
      expect(redis_streams).to receive(:buffer_receive).exactly(100).times.and_call_original
      expect(redis_streams).to receive(:flush).exactly(2).times
      expect(redis_streams).not_to receive(:on_flush_error)

      100.times do |i|
        expect{redis_streams.receive(LogStash::Event.new({"message" => "test-#{i}"}))}.to_not raise_error
      end
    end

    it "should use Redis pipelining when flushing batched events" do
      redis_streams.register
      mock_redis = double("redis")
      allow(redis_streams).to receive(:connect).and_return(mock_redis)
      
      # Mock pipelining - the key test is that pipelined is called
      expect(mock_redis).to receive(:pipelined).and_yield
      expect(mock_redis).to receive(:xadd).exactly(3).times
      
      # Simulate a flush with 3 events
      events = [
        '{"message": "test1"}',
        '{"message": "test2"}',
        '{"message": "test3"}'
      ]
      
      redis_streams.send(:flush, events, "test_stream")
    end
  end

  context "with SSL enabled" do
    let(:config) {{ "ssl_enabled" => true, "stream" => "test_stream" }}
    subject(:plugin) { described_class.new(config) }

    context "and not providing a certificate/key pair" do
      it "registers without error" do
        expect { plugin.register }.to_not raise_error
      end
    end

    context "and providing a certificate/key pair" do
      let(:cert_key_pair) { Flores::PKI.generate }
      let(:certificate) do
        path = Tempfile.new('certificate').path
        IO.write(path, cert_key_pair.first.to_s)
        path
      end
      let(:key) do
        path = Tempfile.new('key').path
        IO.write(path, cert_key_pair[1].to_s)
        path
      end
      let(:config) { super().merge("ssl_certificate" => certificate, "ssl_key" => key) }

      it "registers without error" do
        expect { plugin.register }.to_not raise_error
      end
    end

    context "with only ssl_certificate set" do
      let(:certificate) { Tempfile.new('certificate').path }
      let(:config) { super().merge("ssl_certificate" => certificate) }

      it "should raise a configuration error to request also `ssl_key`" do
        expect { plugin.register }.to raise_error(LogStash::ConfigurationError, /Using an `ssl_certificate` requires an `ssl_key`/)
      end
    end

    context "with only ssl_key set" do
      let(:key) { Tempfile.new('key').path }
      let(:config) { super().merge("ssl_key" => key) }

      it "should raise a configuration error to request also `ssl_certificate`" do
        expect { plugin.register }.to raise_error(LogStash::ConfigurationError, /An `ssl_certificate` is required when using an `ssl_key`/)
      end
    end
  end

  context "XADD functionality" do
    let(:config) {
      {
        "stream" => "test_stream"
      }
    }
    let(:redis_streams) { described_class.new(config) }
    let(:mock_redis) { double("redis") }

    before do
      allow(redis_streams).to receive(:connect).and_return(mock_redis)
    end

    it "should write the complete payload to the body field for XADD" do
      redis_streams.register
      payload = '{"type":"log4_metric","metric":{"name":"api.time","value":123.45}}'

      expect(mock_redis).to receive(:xadd).with(
        "test_stream",
        {"body" => payload}
      )

      redis_streams.send(:xadd_stream, "test_stream", payload)
    end

    it "should add MAXLEN trimming when configured" do
      config_with_maxlen = config.merge("maxlen" => 1000, "approximate_trimming" => true)
      redis_streams_with_maxlen = described_class.new(config_with_maxlen)
      allow(redis_streams_with_maxlen).to receive(:connect).and_return(mock_redis)
      redis_streams_with_maxlen.register
      
      payload = '{"message":"test"}'

      expect(mock_redis).to receive(:xadd).with(
        "test_stream",
        {"body" => payload},
        :maxlen => 1000,
        :approximate => true
      )

      redis_streams_with_maxlen.send(:xadd_stream, "test_stream", payload)
    end
  end
end