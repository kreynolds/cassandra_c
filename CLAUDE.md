# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CassandraC is a Ruby gem that provides Ruby bindings for the Datastax C/C++ Cassandra Driver. It's designed to offer high-performance access to Cassandra databases from Ruby applications through a native C extension.

## Development Environment Setup

### Prerequisites

- Ruby >= 3.0.0
- Cassandra C/C++ driver (`libcassandra` and development headers)
- Docker and Docker Compose (for testing with a Cassandra instance)

### Installation

The Cassandra C/C++ driver must be installed before you can build the gem:

```bash
# On MacOS
brew install cassandra-cpp-driver

# On Debian/Ubuntu
apt-get install libuv1 libuv1-dev libcassandra libcassandra-dev
```

## Commands

### Build and Install

```bash
# Install dependencies
bundle install

# Build the C extension
bundle exec rake compile

# Build the gem
bundle exec rake build

# Install the gem locally
bundle exec rake install
```

### Testing

```bash
# Start Cassandra container for testing
docker-compose up -d

# Run all tests
bundle exec rake test

# Run a specific test file
bundle exec ruby -Itest test/test_session.rb

# Run a specific test
bundle exec ruby -Itest test/test_session.rb -n test_connects_and_disconnects
```

### Code Quality

```bash
# Run standardrb linter
bundle exec rake standard

# Fix linting issues
bundle exec rake standard:fix
```

### Complete Build Process

```bash
# Complete build, including compilation, tests and linting
bundle exec rake
```

## Architecture

The gem has the following main components:

1. **Core Module** (`lib/cassandra_c.rb`): Main entry point for the gem
2. **C Extension** (`ext/cassandra_c/*.c`): Native bindings to the Cassandra C/C++ driver
3. **Ruby Interface Classes**:
   - `CassandraC::Cluster`: Manages cluster connections
   - `CassandraC::Session`: Handles query execution and prepared statements
   - `CassandraC::Future`: Async operation handler
   - `CassandraC::Statement`: Represents a CQL statement
   - `CassandraC::PreparedStatement`: Pre-parsed CQL statement
   - `CassandraC::Result`: Query results

### Extension Structure

- `cassandra_c.c/h`: Core module definitions and initialization
- `cluster.c`: Cluster connection and configuration
- `session.c`: Session management and query execution
- `future.c`: Async operation handling
- `prepared.c`: Prepared statement operations
- `result.c`: Result processing
- `statement.c`: Statement creation and binding
- `value.c`: Data type conversions between Ruby and Cassandra

### Memory Management

The C extension uses Ruby's typed data to properly manage memory and ensure resources are freed when no longer needed, using the RUBY_TYPED_FREE_IMMEDIATELY flag to immediately free Cassandra resources when Ruby objects are garbage collected.

## Usage Example

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

# Prepare a statement
prepared = session.prepare("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?")

# Create a statement from the prepared statement
statement = prepared.bind("system")

# Execute the prepared statement
result = session.execute(statement)

# Close the session when done
session.close
```