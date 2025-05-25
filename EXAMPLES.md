# CassandraC Examples

This document provides comprehensive examples of using CassandraC to interact with Cassandra databases.

## Basic Connection and Queries

### Connecting to Cassandra

```ruby
require 'cassandra_c'

# Create cluster configuration
cluster = CassandraC::Native::Cluster.new
cluster.contact_points = "127.0.0.1"
cluster.port = 9042

# Create session and connect
session = CassandraC::Native::Session.new
session.connect(cluster)

# Execute a simple query
result = session.query("SELECT * FROM system_schema.keyspaces")
result.each do |row|
  puts "Keyspace: #{row[0]}"
end

# Close the session when done
session.close
```

### Prepared Statements

```ruby
# Prepare a statement with parameters
prepared = session.prepare("SELECT * FROM users WHERE id = ? AND status = ?")

# Bind parameters using array syntax
statement = prepared.bind([123, "active"])
result = session.execute(statement)

# Alternative: bind parameters individually
statement = prepared.bind
statement.bind_by_index(0, 123)
statement.bind_by_index(1, "active")
result = session.execute(statement)

# Named parameter binding
prepared_named = session.prepare("SELECT * FROM users WHERE id = :user_id AND status = :user_status")
statement = prepared_named.bind
statement.bind_by_name("user_id", 123)
statement.bind_by_name("user_status", "active")
result = session.execute(statement)
```

## Data Types

### String Types

```ruby
# Text/VARCHAR - supports full UTF-8
session.query("CREATE TABLE messages (id int PRIMARY KEY, content text)")

# ASCII - only 7-bit ASCII characters
session.query("CREATE TABLE codes (id int PRIMARY KEY, code ascii)")

# Insert with UTF-8 content
prepared = session.prepare("INSERT INTO messages (id, content) VALUES (?, ?)")
statement = prepared.bind([1, "Hello, L! =€"])
session.execute(statement)

# ASCII validation (will raise error for non-ASCII)
ascii_prepared = session.prepare("INSERT INTO codes (id, code) VALUES (?, ?)")
ascii_statement = ascii_prepared.bind([1, "ABCD123"])  # Valid ASCII
session.execute(ascii_statement)
```

### Integer Types

```ruby
# Create table with all integer types
session.query(<<~SQL)
  CREATE TABLE numeric_data (
    id int PRIMARY KEY,
    tiny_val tinyint,     -- 8-bit: -128 to 127
    small_val smallint,   -- 16-bit: -32,768 to 32,767
    int_val int,          -- 32-bit: -2,147,483,648 to 2,147,483,647
    big_val bigint,       -- 64-bit: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
    var_val varint        -- Unlimited precision
  )
SQL

# Create typed integers
tiny_val = 42.to_cassandra_tinyint
small_val = 1000.to_cassandra_smallint
int_val = 50000.to_cassandra_int
big_val = 9223372036854775807.to_cassandra_bigint
var_val = 123456789012345678901234567890.to_cassandra_varint

# Insert using typed integers
prepared = session.prepare("INSERT INTO numeric_data (id, tiny_val, small_val, int_val, big_val, var_val) VALUES (?, ?, ?, ?, ?, ?)")
statement = prepared.bind([
  1.to_cassandra_int,
  tiny_val,
  small_val, 
  int_val,
  big_val,
  var_val
])
session.execute(statement)

# Query and work with results
result = session.query("SELECT * FROM numeric_data WHERE id = 1")
row = result.to_a.first

# Results return typed integers
puts row[1].class  # => CassandraC::Types::TinyInt
puts row[2].class  # => CassandraC::Types::SmallInt
puts row[5].class  # => CassandraC::Types::VarInt

# Convert to regular Ruby integers
puts row[1].to_i   # => 42
puts row[5].to_i   # => 123456789012345678901234567890

# Arithmetic operations preserve types
tiny1 = 10.to_cassandra_tinyint
tiny2 = 20.to_cassandra_tinyint
result = tiny1 + tiny2  # => TinyInt(30)
puts result.class       # => CassandraC::Types::TinyInt

# Overflow handling (wraps around)
overflow = 128.to_cassandra_tinyint  # => TinyInt(-128)
puts overflow.to_i      # => -128
```

### Boolean and Null Values

```ruby
# Create table with boolean column
session.query("CREATE TABLE settings (id int PRIMARY KEY, enabled boolean, description text)")

# Insert with boolean and null values
prepared = session.prepare("INSERT INTO settings (id, enabled, description) VALUES (?, ?, ?)")

# Insert with true value
statement = prepared.bind([1, true, "Feature enabled"])
session.execute(statement)

# Insert with false value  
statement = prepared.bind([2, false, "Feature disabled"])
session.execute(statement)

# Insert with null value
statement = prepared.bind([3, nil, nil])
session.execute(statement)
```

## Advanced Usage

### Async Operations

```ruby
# Async connection
session = CassandraC::Native::Session.new
future = session.connect(cluster, async: true)

# Do other work while connecting...
puts "Connecting..."

# Wait for connection to complete
future.wait
if future.ready?
  puts "Connected!"
else
  puts "Connection failed"
end
```

### Error Handling

```ruby
begin
  # This will fail if table doesn't exist
  result = session.query("SELECT * FROM nonexistent_table")
rescue CassandraC::Error => e
  puts "Cassandra error: #{e.message}"
end

begin
  # Invalid parameter binding
  prepared = session.prepare("SELECT * FROM users WHERE id = ?")
  statement = prepared.bind(["not_a_number"])  # Invalid for int column
  session.execute(statement)
rescue CassandraC::Error => e
  puts "Binding error: #{e.message}"
end
```

### Working with Results

```ruby
# Execute query
result = session.query("SELECT keyspace_name, table_name FROM system_schema.tables LIMIT 10")

# Iterate over rows
result.each do |row|
  keyspace = row[0]
  table = row[1]
  puts "#{keyspace}.#{table}"
end

# Convert to array for further processing
rows = result.to_a
puts "Found #{rows.length} tables"

# Access result metadata
puts "Columns: #{result.column_count}"
puts "Rows: #{result.row_count}"
```

### Load Balancing and Retry Policies

```ruby
# Configure round-robin load balancing
cluster = CassandraC::Native::Cluster.new
cluster.contact_points = "127.0.0.1,127.0.0.2,127.0.0.3"
cluster.load_balance_round_robin

# Configure DC-aware load balancing
cluster.load_balance_dc_aware("datacenter1")

# Configure retry policy
cluster.retry_policy_default        # Default retry policy
cluster.retry_policy_fallthrough    # Never retry
```

## Complete Example Application

```ruby
#!/usr/bin/env ruby

require 'cassandra_c'

class UserService
  def initialize(contact_points = "127.0.0.1")
    @cluster = CassandraC::Native::Cluster.new
    @cluster.contact_points = contact_points
    @cluster.port = 9042
    
    @session = CassandraC::Native::Session.new
    @session.connect(@cluster)
    
    setup_schema
  end

  def setup_schema
    @session.query(<<~SQL)
      CREATE KEYSPACE IF NOT EXISTS user_app 
      WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    SQL
    
    @session.query("USE user_app")
    
    @session.query(<<~SQL)
      CREATE TABLE IF NOT EXISTS users (
        id int PRIMARY KEY,
        username text,
        email text,
        age tinyint,
        score smallint,
        balance bigint,
        metadata varint,
        is_active boolean,
        created_at timestamp
      )
    SQL
  end

  def create_user(id, username, email, age, score, balance, metadata)
    prepared = @session.prepare(<<~SQL)
      INSERT INTO users (id, username, email, age, score, balance, metadata, is_active, created_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, toTimestamp(now()))
    SQL
    
    statement = prepared.bind([
      id.to_cassandra_int,
      username,
      email,
      age.to_cassandra_tinyint,
      score.to_cassandra_smallint,
      balance.to_cassandra_bigint,
      metadata.to_cassandra_varint,
      true
    ])
    
    @session.execute(statement)
    puts "Created user: #{username}"
  end

  def find_user(id)
    prepared = @session.prepare("SELECT * FROM users WHERE id = ?")
    statement = prepared.bind([id.to_cassandra_int])
    result = @session.execute(statement)
    
    rows = result.to_a
    return nil if rows.empty?
    
    row = rows.first
    {
      id: row[0].to_i,
      username: row[1],
      email: row[2],
      age: row[3].to_i,
      score: row[4].to_i,
      balance: row[5].to_i,
      metadata: row[6].to_i,
      is_active: row[7]
    }
  end

  def close
    @session.close
  end
end

# Usage
service = UserService.new

# Create some users
service.create_user(1, "alice", "alice@example.com", 25, 1500, 999999999999, 123456789012345678901234567890)
service.create_user(2, "bob", "bob@example.com", 30, 2000, 888888888888, 987654321098765432109876543210)

# Find users
user1 = service.find_user(1)
puts "Found user: #{user1[:username]} (#{user1[:email]})"

user2 = service.find_user(2)
puts "Found user: #{user2[:username]} (#{user2[:email]})"

service.close
```

This example demonstrates a complete application using CassandraC with proper error handling, typed integers, and resource management.