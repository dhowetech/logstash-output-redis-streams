## Unreleased
  - Performance: run with `concurrency :shared` and pool Redis connections
    (`pool_size`/`pool_timeout`) so concurrent pipeline workers no longer
    serialize on a single shared socket. Pooling is implemented with a
    small stdlib-only pool (no new runtime gem dependency), so it installs
    cleanly in restricted/air-gapped Logstash environments.
  - Reliability: retries now use exponential backoff (`reconnect_interval`,
    capped by new `max_reconnect_interval`) instead of a fixed sleep, so a
    struggling Redis backs off without blocking the pipeline indefinitely

## 1.0.0
  - Initial release of logstash-output-redis-streams plugin
  - Support for Redis Streams using XADD command
  - Stream partitioning with multiple strategies:
    - Random partitioning
    - Hash-based partitioning
    - Time-based partitioning
  - Batch processing with Redis pipelining for optimal performance
  - Stream length management with MAXLEN
  - SSL/TLS support
  - Connection management with automatic reconnection
  - Multiple host support with failover