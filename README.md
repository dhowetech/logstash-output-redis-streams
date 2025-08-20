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

## Configuration Options

| Setting | Input type | Required | Default | Description |
|---------|------------|----------|---------|-------------|
| `stream` | string | Yes | | The name of the Redis stream. Supports dynamic names like `logstash-%{type}` |
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
| `approximate_trimming` | boolean | No | `true` | Use approximate trimming for better performance |
| `ssl_enabled` | boolean | No | `false` | Enable SSL/TLS |

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