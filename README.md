# CassandraC

[![Build Status](https://github.com/kreynolds/cassandra_c/workflows/Build%20Check/badge.svg)](https://github.com/kreynolds/cassandra_c/actions/workflows/build.yml)
[![Test Suite](https://github.com/kreynolds/cassandra_c/workflows/Test%20Suite/badge.svg)](https://github.com/kreynolds/cassandra_c/actions/workflows/test.yml)

CassandraC is a Ruby gem that provides bindings for the Datastax C/C++ Cassandra Driver. It's designed to offer high-performance access to Cassandra databases from Ruby applications through a native C extension.

> **🤖 AI-Assisted Development Experiment**: This project is an experiment in using AI (Claude) to write production-quality code. See [COSTS.md](COSTS.md) for detailed cost tracking of AI-generated features.

## Installation

Before installing the gem, you must have the Cassandra C/C++ driver installed on your system.

### Installing the Cassandra C/C++ Driver

```bash
# On MacOS
brew install cassandra-cpp-driver

# On Debian/Ubuntu
apt-get install libuv1 libuv1-dev libcassandra libcassandra-dev
```

Then add the gem to your application's Gemfile:

```ruby
gem 'cassandra_c'
```

And then execute:

    $ bundle install

Or install it yourself as:

    $ gem install cassandra_c

## Usage

### Basic Connection

```ruby
require 'cassandra_c'

# Create cluster configuration
cluster = CassandraC::Cluster.new
cluster.contact_points = "127.0.0.1"
cluster.port = 9042

# Create session and connect
session = CassandraC::Session.new
session.connect(cluster)

# Execute a simple query
result = session.query("SELECT * FROM system_schema.tables")

# Close the session when done
session.close
```

## Supported Data Types

CassandraC supports all major Cassandra data types with seamless Ruby integration:

### Scalar Types
- **Text/VARCHAR/ASCII**: Regular Ruby strings with UTF-8 and ASCII validation
- **Integers**: TinyInt, SmallInt, Int, BigInt, VarInt with overflow handling
- **Floating Point**: Float, Double, Decimal with precision control
- **Boolean**: Ruby true/false values
- **UUID/TimeUUID**: Typed UUID objects with generation and time extraction
- **Blob**: Binary data with proper encoding
- **Inet**: IP address storage (IPv4/IPv6)

### Collection Types
- **Lists**: Ruby Arrays work seamlessly with Cassandra `list<type>` columns
- **Sets**: Ruby Set objects work seamlessly with Cassandra `set<type>` columns  
- **Maps**: Ruby Hash objects work seamlessly with Cassandra `map<key_type, value_type>` columns

```ruby
require 'set'

# Arrays bind directly to list columns
session.execute("INSERT INTO table (id, numbers, tags, scores) VALUES (?, ?, ?, ?)", 
                [1, [1, 2, 3], Set.new(["ruby", "cassandra"]), {"total" => 95}])

# Results come back as Ruby Arrays, Sets, and Hashes
result = session.query("SELECT numbers, tags, scores FROM table WHERE id = 1")
numbers, tags, scores = result.to_a.first  
# numbers: [1, 2, 3] (Array)
# tags: #<Set: {"ruby", "cassandra"}> (Set)  
# scores: {"total" => 95} (Hash)
```

See [EXAMPLES.md](EXAMPLES.md) for comprehensive usage examples of all data types.

### Load Balancing Policies

CassandraC supports different load balancing policies to control how queries are distributed to nodes in a Cassandra cluster.

#### Round Robin Load Balancing

The Round Robin policy distributes queries across all nodes in the cluster in a round-robin fashion:

```ruby
cluster = CassandraC::Cluster.new
cluster.use_round_robin_load_balancing
```

#### DC-Aware Load Balancing

The DC-Aware policy distributes queries to nodes in a specified local datacenter:

```ruby
cluster = CassandraC::Cluster.new

# Only parameter is the local datacenter name
cluster.use_dc_aware_load_balancing("dc1")
```

With this configuration:
- All nodes in the datacenter named "dc1" will be used
- Nodes in remote datacenters will not be used

This approach follows the driver's recommended practice for datacenter-aware routing, avoiding the deprecated remote datacenter settings.

### Retry Policies

CassandraC provides several retry policies that determine how the driver responds to failed queries, timeouts, and unavailable errors.

#### Default Retry Policy

The default policy provides reasonable behavior for most applications:

```ruby
cluster = CassandraC::Cluster.new
cluster.use_default_retry_policy
```

This policy will:
- Retry on a read timeout if there were enough replicas but no data present
- Retry on a write timeout if a logged batch request failed to write the batch log
- Retry on an unavailable error using a new host
- In all other cases, return an error

#### Fallthrough Retry Policy

The fallthrough policy never retries or ignores any server-side failure:

```ruby
cluster = CassandraC::Cluster.new
cluster.use_fallthrough_retry_policy
```

#### Logging Retry Policy

The logging policy wraps another retry policy and logs the retry decisions:

```ruby
cluster = CassandraC::Cluster.new
# Using default policy as the child policy
cluster.use_logging_retry_policy(:default)

# Or using fallthrough policy as the child policy
cluster.use_logging_retry_policy(:fallthrough)
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake test` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and the created tag, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/cassandra_c. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [code of conduct](https://github.com/[USERNAME]/cassandra_c/blob/main/CODE_OF_CONDUCT.md).

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the CassandraC project's codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/[USERNAME]/cassandra_c/blob/main/CODE_OF_CONDUCT.md).
