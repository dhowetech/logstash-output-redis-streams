Gem::Specification.new do |s|

  s.name            = 'logstash-output-redis-streams'
  s.version         = '1.0.0'
  s.licenses        = ['Apache-2.0']
  s.summary         = "Sends events to Redis Streams using the `XADD` command"
  s.description     = "This gem is a Logstash plugin required to be installed on top of the Logstash core pipeline using $LS_HOME/bin/logstash-plugin install gemname. This gem is not a stand-alone program"
  s.authors         = ["Jeremy Plichta"]
  s.email           = 'jeremy.plichta@redis.com'
  s.homepage        = "http://www.elastic.co/guide/en/logstash/current/index.html"
  s.require_paths = ["lib"]

  # Files
  s.files = Dir["lib/**/*","spec/**/*","*.gemspec","*.md","CONTRIBUTORS","Gemfile","LICENSE","NOTICE.TXT", "vendor/jar-dependencies/**/*.jar", "vendor/jar-dependencies/**/*.rb", "VERSION", "docs/**/*"]

  # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "output" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency 'logstash-core', '>= 6.0', '< 9.0'

  s.add_runtime_dependency 'redis', '>= 3.3', '< 6.0'
  s.add_runtime_dependency 'stud', '~> 0.0'
  s.add_runtime_dependency 'connection_pool', '~> 2.2'

  s.add_development_dependency 'logstash-devutils', '~> 1.0'
  s.add_development_dependency 'logstash-input-generator', '~> 1.0'
  s.add_development_dependency 'logstash-codec-json', '~> 3.0'
  s.add_development_dependency 'flores', '~> 0.0'
end