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

# Run native layer tests only
bundle exec ruby -Itest test/native/test_session.rb

# Run a specific test
bundle exec ruby -Itest test/native/test_session.rb -n test_connects_and_disconnects

# Run specific test suites
bundle exec rake test TEST="test/native/**/*.rb"
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

The gem follows a two-layer architecture:

### Native Layer (`CassandraC::Native`)
Low-level faithful bindings to the DataStax C/C++ driver:
- **`CassandraC::Native::Cluster`**: Cluster configuration and connection management
- **`CassandraC::Native::Session`**: Query execution and session management  
- **`CassandraC::Native::Future`**: Async operation handling
- **`CassandraC::Native::Statement`**: CQL statement representation
- **`CassandraC::Native::Prepared`**: Prepared statement management
- **`CassandraC::Native::Result`**: Query result processing

### Idiomatic Layer (`CassandraC`) - Future
Ruby-friendly interface built on top of the Native layer:
- Enumerable result sets
- Block-based iteration
- Method chaining
- Ruby naming conventions

**Current Focus**: Completing and stabilizing the Native layer before building idiomatic wrappers.

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

### Native Layer Usage

```ruby
require 'cassandra_c'

# Create cluster configuration (Native layer)
cluster = CassandraC::Native::Cluster.new
cluster.contact_points = "127.0.0.1"
cluster.port = 9042

# Create session and connect
session = CassandraC::Native::Session.new
session.connect(cluster)

# Execute a simple query
result = session.query("SELECT * FROM system_schema.tables")

# Prepare a statement with parameter binding
prepared = session.prepare("SELECT * FROM system_schema.keyspaces WHERE keyspace_name = ?")

# Bind parameters and execute
statement = prepared.bind(["system"])
result = session.execute(statement)

# Alternative parameter binding approaches
statement = prepared.bind
statement.bind_by_index(0, "system")
# or
statement.bind_by_name("keyspace_name", "system")

# Close the session when done
session.close
```

### Integer Types Usage

CassandraC provides typed integer wrappers that map to specific Cassandra types:

```ruby
require 'cassandra_c'

# Create typed integers with conversion methods
tiny_val = 42.to_cassandra_tinyint        # TinyInt: 8-bit (-128 to 127)
small_val = 1000.to_cassandra_smallint    # SmallInt: 16-bit (-32,768 to 32,767)
int_val = 50000.to_cassandra_int          # Int: 32-bit (-2,147,483,648 to 2,147,483,647)  
big_val = 9223372036854775807.to_cassandra_bigint  # BigInt: 64-bit
var_val = 123456789012345678901234567890.to_cassandra_varint  # VarInt: unlimited precision

# Use in prepared statements
session.query("CREATE TABLE users (id int, score tinyint, points smallint, balance bigint, total varint)")

prepared = session.prepare("INSERT INTO users (id, score, points, balance, total) VALUES (?, ?, ?, ?, ?)")
statement = prepared.bind([
  1.to_cassandra_int,
  85.to_cassandra_tinyint, 
  1500.to_cassandra_smallint,
  999999999999.to_cassandra_bigint,
  999999999999999999999999999999.to_cassandra_varint
])
session.execute(statement)

# Results return typed integers  
result = session.query("SELECT * FROM users WHERE id = 1")
row = result.to_a.first

row[0].class  # => CassandraC::Types::Int
row[1].class  # => CassandraC::Types::TinyInt
row[2].class  # => CassandraC::Types::SmallInt
row[3].class  # => CassandraC::Types::BigInt  
row[4].class  # => CassandraC::Types::VarInt

# Convert back to regular Ruby integers
row[0].to_i   # => 1
row[1].to_i   # => 85
row[4].to_i   # => 999999999999999999999999999999

# Arithmetic operations preserve types with overflow wrapping
tiny1 = 100.to_cassandra_tinyint
tiny2 = 50.to_cassandra_tinyint
result = tiny1 + tiny2  # => TinyInt(150)

# Overflow wraps around
overflow = 128.to_cassandra_tinyint  # => TinyInt(-128)
```

### Blob Types Usage

CassandraC supports blob (binary) data using Ruby strings with binary encoding:

```ruby
require 'cassandra_c'

# Create session and table
session.query("CREATE TABLE files (id text PRIMARY KEY, data blob)")

# Store binary data as blob
binary_data = "\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR".b  # PNG header
prepared = session.prepare("INSERT INTO files (id, data) VALUES (?, ?)")
statement = prepared.bind
statement.bind_text_by_index(0, "image1")
statement.bind_blob_by_index(1, binary_data)
session.execute(statement)

# Can also bind by name
statement = prepared.bind
statement.bind_text_by_name("id", "image2")
statement.bind_blob_by_name("data", binary_data)
session.execute(statement)

# Retrieved blob data maintains binary encoding
result = session.query("SELECT data FROM files WHERE id = 'image1'")
# Result data will have ASCII-8BIT encoding
```

### Inet Types Usage

CassandraC supports inet (IP address) data using either Ruby strings or IPAddr objects:

```ruby
require 'cassandra_c'
require 'ipaddr'

# Create session and table
session.query("CREATE TABLE servers (id text PRIMARY KEY, ip_address inet, backup_ip inet)")

# Store IP addresses using strings
prepared = session.prepare("INSERT INTO servers (id, ip_address, backup_ip) VALUES (?, ?, ?)")
statement = prepared.bind
statement.bind_text_by_index(0, "server1")
statement.bind_inet_by_index(1, "192.168.1.100")      # IPv4 string
statement.bind_inet_by_index(2, "2001:db8::1")        # IPv6 string
session.execute(statement)

# Store IP addresses using IPAddr objects
statement = prepared.bind
statement.bind_text_by_index(0, "server2")
statement.bind_inet_by_index(1, IPAddr.new("10.0.0.1"))           # IPv4 IPAddr
statement.bind_inet_by_index(2, IPAddr.new("fe80::1"))             # IPv6 IPAddr
session.execute(statement)

# Can also bind by name
statement = prepared.bind
statement.bind_text_by_name("id", "server3")
statement.bind_inet_by_name("ip_address", "172.16.0.1")
statement.bind_inet_by_name("backup_ip", IPAddr.new("::1"))
session.execute(statement)

# Retrieved inet data returns as strings
result = session.query("SELECT ip_address, backup_ip FROM servers WHERE id = 'server1'")
row = result.to_a.first
row[0]  # => "192.168.1.100" (String)
row[1]  # => "2001:db8::1" (String)

# Convert back to IPAddr if needed
primary_ip = IPAddr.new(row[0])
backup_ip = IPAddr.new(row[1])
```

### Decimal/Floating Point Types Usage

CassandraC supports decimal and floating point data using typed wrappers that map to specific Cassandra types:

```ruby
require 'cassandra_c'

# Create typed floats and doubles with conversion methods
float_val = 3.14.to_cassandra_float         # Float: 32-bit IEEE 754
double_val = 2.718281828.to_cassandra_double # Double: 64-bit IEEE 754

# Create from integers too
int_float = 42.to_cassandra_float            # Float: 42.0
int_double = 123.to_cassandra_double         # Double: 123.0

# Use in prepared statements
session.query("CREATE TABLE metrics (id int, score float, precision_value double)")

prepared = session.prepare("INSERT INTO metrics (id, score, precision_value) VALUES (?, ?, ?)")
statement = prepared.bind([
  1.to_cassandra_int,
  3.14159.to_cassandra_float,
  2.718281828459045.to_cassandra_double
])
session.execute(statement)

# Type-specific binding methods
statement = prepared.bind
statement.bind_by_index(0, 2.to_cassandra_int)
statement.bind_float_by_index(1, 1.23.to_cassandra_float)
statement.bind_double_by_index(2, 4.56789.to_cassandra_double)
session.execute(statement)

# Can also bind by name
statement = prepared.bind
statement.bind_text_by_name("id", 3)
statement.bind_float_by_name("score", 9.87)
statement.bind_double_by_name("precision_value", 1.23456789012345)
session.execute(statement)

# Can bind raw numeric values too (automatically converted)
statement = prepared.bind
statement.bind_by_index(0, 4)               # Raw integer
statement.bind_float_by_index(1, 7.89)      # Raw float -> 32-bit
statement.bind_double_by_index(2, 12.345)   # Raw float -> 64-bit
session.execute(statement)

# Results return typed float/double objects
result = session.query("SELECT * FROM metrics WHERE id = 1")
row = result.to_a.first

row[0].class  # => CassandraC::Types::Int
row[1].class  # => CassandraC::Types::Float
row[2].class  # => CassandraC::Types::Double

# Convert back to regular Ruby numbers
row[0].to_i   # => 1
row[1].to_f   # => 3.14159...
row[2].to_f   # => 2.718281828459045

# Arithmetic operations preserve types
float1 = 1.5.to_cassandra_float
float2 = 2.5.to_cassandra_float
result = float1 + float2  # => Float(4.0)

# Float vs Double precision
# Float: 32-bit IEEE 754 (approximately 7 decimal digits)
# Double: 64-bit IEEE 754 (approximately 15-17 decimal digits)
precise_value = 1.23456789012345
float_val = precise_value.to_cassandra_float   # May lose precision
double_val = precise_value.to_cassandra_double # Retains more precision
```

### Decimal Types Usage

CassandraC supports arbitrary precision decimal data using typed wrappers:

```ruby
require 'cassandra_c'
require 'bigdecimal'

# Create typed decimals with conversion methods
decimal_val = "123.456789".to_cassandra_decimal      # Decimal: arbitrary precision
decimal_from_float = 3.14159.to_cassandra_decimal    # Decimal: from float
decimal_from_bigdecimal = BigDecimal("999.123456789012345").to_cassandra_decimal

# Specify explicit scale (decimal places)
decimal_with_scale = "12.34".to_cassandra_decimal(4) # Scale of 4 decimal places

# Use in Ruby applications (C binding not yet fully implemented)
decimal1 = "123.456".to_cassandra_decimal
decimal2 = "789.012".to_cassandra_decimal

# Arithmetic operations preserve types
result = decimal1 + decimal2  # => Decimal(912.468, scale: 3)

# Conversion methods
decimal1.to_f     # => 123.456 (Float)
decimal1.to_d     # => BigDecimal('123.456')
decimal1.to_s     # => "123.456"
decimal1.scale    # => 3 (number of decimal places)
decimal1.unscaled_value  # => 123456 (value * 10^scale)

# Comparison operations
decimal1 == BigDecimal("123.456")  # => true
decimal1 < decimal2                # => true

# Marker method for type identification
decimal1.cassandra_typed_decimal?  # => true

# Note: Full Cassandra DECIMAL binding requires varint encoding implementation
# Currently supports Ruby-side operations and type safety
# Cassandra binding will be completed in future versions
```

### Test Structure

Tests are organized by namespace:
- `test/` - Main module tests
- `test/native/` - Native layer tests (CassandraC::Native::*)
  - `test_session.rb` - Session functionality tests
  - `test_cluster.rb` - Cluster configuration tests
  - `test_statement.rb` - Statement and binding tests
  - `test_retry_policy.rb` - Retry policy tests
  - `test_integer_types.rb` - Integer type functionality tests
  - `test_string_types.rb` - String type functionality tests
  - `test_blob_types.rb` - Blob type functionality tests
  - `test_boolean_types.rb` - Boolean type functionality tests
  - `test_inet_types.rb` - Inet type functionality tests
  - `test_decimal_types.rb` - Decimal/floating point type functionality tests

## Development Guidelines

### Testing Environment
- **Cassandra is assumed to be running** - Do not start/stop Cassandra containers in development
- Always run the full test suite (`bundle exec rake test`) when completing features
- Address all test failures before considering work complete
- Run linter (`bundle exec rake standard`) and fix all issues

### Code Patterns
- **Study existing patterns first** - Always examine similar existing code before implementing
- **Reuse setup/teardown patterns** - Use proper `setup` and `teardown` methods in tests, avoid duplication
- **DDL in test_helper ONLY** - ALL table creation DDL must be in `test/test_helper.rb` global setup, NEVER in individual test files or class setup methods. Individual tests should only connect to existing tables.
- **Follow file conventions** - End all files with newlines, avoid whitespace-only lines
- **Type-specific binding** - Use dedicated binding methods like `bind_text_by_index`, `bind_blob_by_index`, `bind_inet_by_index`
- **Memory management** - Use Ruby's typed data with `RUBY_TYPED_FREE_IMMEDIATELY` for C resources

### Feature Completion Checklist
When implementing a new feature:
1. Research existing patterns in the codebase
2. Implement following established conventions
3. Create comprehensive tests following existing test structure
4. Run full test suite and fix any failures
5. Run linter and fix all style issues
6. Update documentation in CLAUDE.md, EXAMPLES.md, and TODO.md
7. Verify examples work correctly
8. Update COSTS.md with development cost and feature details
9. Create git commit with proper message format
10. If bugs are discovered post-implementation, track them in ESCAPES.md

**IMPORTANT**: A feature is not considered complete until all documentation is updated AND a git commit has been made. The commit should include all code changes and documentation updates together.