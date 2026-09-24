# Logstash Plugin

[![Travis Build Status](https://travis-ci.com/logstash-plugins/logstash-output-redis-streams.svg)](https://travis-ci.com/logstash-plugins/logstash-output-redis-streams)

This is a plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are pretty much free to use it however you want in whatever way.

## Description

This output plugin sends events to Redis Streams using the `XADD` command. Redis Streams were introduced in Redis 5.0 and provide a powerful append-only data structure for message queuing and event sourcing.

Unlike the standard logstash-output-redis plugin which supports lists (RPUSH) and pub/sub (PUBLISH), this plugin is specifically designed for Redis Streams and includes advanced partitioning capabilities.

## Features

- **Redis Streams Support**: Uses XADD to append events to Redis Streams
- **Stream Partitioning**: Distribute events across multiple streams using different strategies:
  - Random partitioning
  - Hash-based partitioning (based on field values)
  - Time-based partitioning (with configurable time formats)
- **Redis Pipelining**: Batch multiple events and use Redis pipelining for optimal performance
- **Stream Trimming**: Automatic stream length management with MAXLEN
- **SSL Support**: Full SSL/TLS configuration support
- **Connection Management**: Automatic reconnection and multiple host support

## Installation

```sh
bin/logstash-plugin install logstash-output-redis-streams
```

## Configuration

### Basic Configuration

```ruby
output {
  redis_streams {
    host => ["127.0.0.1"]
    port => 6379
    stream => "logstash-events"
  }
}
```

### Redis-to-OTel Bridge Configuration

The bridge expects the complete JSON event in one stream field named `body`. Use
one output per bridge stream. The example below includes the recommended
production settings (batching/pipelining, connection pooling, bounded
timeouts/retries, and stream trimming — see
[Recommended Production Configuration](#recommended-production-configuration)
for details):

```ruby
output {
  if [sampled_for_bridge] and ([type] == "log4net_log" or [type] == "log4_log") {
    redis_streams {
      host => ["localhost"]
      port => 6379
      db => 0
      stream => "log4_logs"
      field => "body"
      codec => json

      # Batch + pipeline events to cut network round-trips
      batch => true
      batch_events => 200
      batch_timeout => 2

      # Pool connections so concurrent pipeline workers don't serialize on
      # one shared socket. Size the pool to match (or exceed) pipeline.workers.
      pool_size => 8
      pool_timeout => 5

      # Fail fast on a truly stuck Redis instead of blocking the pipeline
      connect_timeout => 2
      read_timeout => 2
      write_timeout => 2

      # Retry with exponential backoff, bounded so a single batch can't
      # stall a worker thread indefinitely; drop after repeated failures
      # rather than backing up the whole pipeline.
      reconnect_interval => 1
      max_reconnect_interval => 10
      max_retries => 5

      # Bound stream growth so Redis memory doesn't grow unbounded
      max_stream_size => 1000000
      approximate_trimming => true
    }
  }

  if [sampled_for_bridge] and ([type] == "log4net_metric" or [type] == "log4_metric") {
    redis_streams {
      host => ["localhost"]
      port => 6379
      db => 0
      stream => "log4_metrics"
      field => "body"
      codec => json

      batch => true
      batch_events => 200
      batch_timeout => 2

      pool_size => 8
      pool_timeout => 5

      connect_timeout => 2
      read_timeout => 2
      write_timeout => 2

      reconnect_interval => 1
      max_reconnect_interval => 10
      max_retries => 5

      max_stream_size => 1000000
      approximate_trimming => true
    }
  }
}
```

Each event is written as `XADD <stream> * body '<JSON event>'`. Do not enable
partitioning for these outputs unless the bridge is configured with matching
partitioned stream names.

### Partitioned Streams

#### Random Partitioning
```ruby
output {
  redis_streams {
    host => ["127.0.0.1"]
    stream => "logstash-events"
    enable_partitioning => true
    partition_strategy => "random"
    partition_count => 4
  }
}
```

#### Hash-based Partitioning
```ruby
output {
  redis_streams {
    host => ["127.0.0.1"]
    stream => "logstash-events"
    enable_partitioning => true
    partition_strategy => "hash"
    partition_field => "user_id"
    partition_count => 8
  }
}
```

#### Time-based Partitioning
```ruby
output {
  redis_streams {
    host => ["127.0.0.1"]
    stream => "logstash-events"
    enable_partitioning => true
    partition_strategy => "time_based"
    time_format => "%Y-%m-%d-%H"  # Creates hourly streams
  }
}
```

### Batching with Redis Pipelining
```ruby
output {
  redis_streams {
    host => ["127.0.0.1"]
    stream => "logstash-events"
    batch => true
    batch_events => 100
    batch_timeout => 5
  }
}
```

### Recommended Production Configuration

Under sustained/high-volume load, the defaults favor safety over throughput
(single connection, no batching, fixed 1s retry sleep). For production
deployments, combine batching/pipelining with connection pooling and
exponential backoff so this output can scale with `pipeline.workers` and
degrade gracefully if Redis is slow or briefly unavailable:

```ruby
output {
  redis_streams {
    host              => ["redis-1:6379", "redis-2:6379"]  # multiple hosts for failover
    stream            => "logstash-events"

    # Batch + pipeline events to cut network round-trips
    batch             => true
    batch_events      => 200
    batch_timeout     => 2

    # Pool connections so concurrent pipeline workers don't serialize on
    # one shared socket. Size the pool to match (or exceed) pipeline.workers.
    pool_size         => 8
    pool_timeout      => 5

    # Fail fast on a truly stuck Redis instead of blocking the pipeline
    connect_timeout   => 2
    read_timeout      => 2
    write_timeout     => 2

    # Retry with exponential backoff, bounded so a single batch can't
    # stall a worker thread indefinitely; drop after repeated failures
    # rather than backing up the whole pipeline.
    reconnect_interval     => 1
    max_reconnect_interval => 10
    max_retries            => 5

    # Bound stream growth so Redis memory doesn't grow unbounded
    max_stream_size   => 1000000
    approximate_trimming => true
  }
}
```

Tuning notes:
- Set `pool_size` to at least `pipeline.workers` (or your expected
  concurrent output invocations) so workers aren't waiting on the pool.
- Keep `batch_events` and `batch_timeout` balanced against acceptable
  end-to-end latency — larger batches reduce round-trips but delay delivery.
- `max_retries` combined with `max_reconnect_interval` bounds the worst-case
  time a single failed batch/event can hold up the pipeline:
  roughly `connect/read/write_timeout + sum of backoff delays`. Tune lower
  for latency-sensitive pipelines, higher for durability during brief Redis
  blips.

### Stream Length Management
```ruby
output {
  redis_streams {
    host => ["127.0.0.1"]
    stream => "logstash-events"
    maxlen => 10000
    approximate_trimming => true
  }
}
```

### Stream Size and Retention Management
```ruby
output {
  redis_streams {
    host => ["127.0.0.1"]
    stream => "logstash-events"
    max_stream_size => 50000        # Keep only the latest 50k messages
    stream_retention => 86400       # Keep messages for 24 hours (86400 seconds)
    approximate_trimming => true
  }
}
```

## Configuration Options

| Setting | Input type | Required | Default | Description |
|---------|------------|----------|---------|-------------|
| `stream` | string | Yes | | The name of the Redis stream. Supports dynamic names like `logstash-%{type}` |
| `field` | string | No | `body` | The field containing the complete codec-serialized event payload |
| `host` | array | No | `["127.0.0.1"]` | Redis server hostnames |
| `port` | number | No | `6379` | Redis server port |
| `password` | password | No | | Redis authentication password |
| `db` | number | No | `0` | Redis database number |
| `enable_partitioning` | boolean | No | `false` | Enable stream partitioning |
| `partition_strategy` | string | No | `"random"` | Partitioning strategy: `random`, `hash`, or `time_based` |
| `partition_count` | number | No | `1` | Number of partitions for `random` and `hash` strategies |
| `partition_field` | string | No | `"message"` | Field to use for hash-based partitioning |
| `time_format` | string | No | `"%Y-%m-%d-%H"` | Time format for time-based partitioning |
| `batch` | boolean | No | `false` | Enable event batching with Redis pipelining |
| `batch_events` | number | No | `50` | Number of events per batch |
| `batch_timeout` | number | No | `5` | Maximum time between batches (seconds) |
| `maxlen` | number | No | `0` | Maximum stream length (0 = unlimited) |
| `max_stream_size` | number | No | `0` | Alternative to maxlen - maximum number of messages in stream (0 = unlimited) |
| `stream_retention` | number | No | `0` | Time-based retention in seconds - removes messages older than this (0 = unlimited) |
| `approximate_trimming` | boolean | No | `true` | Use approximate trimming for better performance |
| `ssl_enabled` | boolean | No | `false` | Enable SSL/TLS |
| `pool_size` | number | No | `5` | Number of pooled Redis connections; each concurrent pipeline worker checks out its own connection instead of contending for one shared socket |
| `pool_timeout` | number | No | `5` | Seconds a worker waits for a pooled connection before raising an error |
| `reconnect_interval` | number | No | `1` | Base delay (seconds) before retrying a failed send; retries use exponential backoff |
| `max_reconnect_interval` | number | No | `10` | Upper bound (seconds) on the exponential backoff delay between retries |
| `max_retries` | number | No | `3` | Maximum consecutive send attempts before dropping the event/batch (0 = retry forever) |

## Requirements

- Redis 5.0 or higher (for Redis Streams support)
- Logstash 6.0 or higher

## Development

### Testing

```sh
bundle install
bundle exec rspec
```

### Installation from source

```sh
gem build logstash-output-redis-streams.gemspec
bin/logstash-plugin install --no-verify logstash-output-redis-streams-1.0.0.gem
```

## Contributing

All contributions are welcome: ideas, patches, documentation, bug reports, complaints, and even something you drew up on a napkin.

Programming is not a required skill. Whatever you've seen about open source and maintainers or community members saying "send patches or die" - you will not see that here.

It is more important to the community that you are able to contribute.

For more information about contributing, see the [CONTRIBUTING](https://github.com/elastic/logstash/blob/master/CONTRIBUTING.md) file.
