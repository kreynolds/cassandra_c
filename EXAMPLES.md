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
statement = prepared.bind([1, "Hello, L! =�"])
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

### Blob Types (Binary Data)

```ruby
# Create table with blob column for storing binary data
session.query("CREATE TABLE files (id text PRIMARY KEY, filename text, data blob, file_type text)")

# Store binary file data
image_data = File.binread("example.png")  # Read binary file
pdf_data = "\x25\x50\x44\x46\x2D\x31\x2E\x34".b  # PDF header

# Insert blob data using type-specific binding
prepared = session.prepare("INSERT INTO files (id, filename, data, file_type) VALUES (?, ?, ?, ?)")

# Bind blob data by index
statement = prepared.bind
statement.bind_text_by_index(0, "file1")
statement.bind_text_by_index(1, "example.png")
statement.bind_blob_by_index(2, image_data)
statement.bind_text_by_index(3, "image/png")
session.execute(statement)

# Bind blob data by name
prepared_named = session.prepare("INSERT INTO files (id, filename, data, file_type) VALUES (:id, :filename, :data, :file_type)")
statement = prepared_named.bind
statement.bind_text_by_name("id", "file2")
statement.bind_text_by_name("filename", "document.pdf")
statement.bind_blob_by_name("data", pdf_data)
statement.bind_text_by_name("file_type", "application/pdf")
session.execute(statement)

# Retrieve blob data (maintains binary encoding)
result = session.query("SELECT filename, data FROM files WHERE id = 'file1'")
row = result.to_a.first
filename = row[0]
file_data = row[1]

puts "File: #{filename}"
puts "Size: #{file_data.bytesize} bytes"
puts "Encoding: #{file_data.encoding}"  # => ASCII-8BIT

# Write retrieved data to file
File.binwrite("retrieved_#{filename}", file_data)

# Handle various binary data types
binary_examples = [
  "\x00\x01\x02\x03\xFF".b,              # Raw binary sequence
  "Hello".force_encoding("ASCII-8BIT"),   # Text as binary
  Random.bytes(1024),                      # Random binary data
  "".b                                     # Empty binary data
]

binary_examples.each_with_index do |data, index|
  statement = prepared.bind
  statement.bind_text_by_index(0, "binary_#{index}")
  statement.bind_text_by_index(1, "data_#{index}.bin")
  statement.bind_blob_by_index(2, data)
  statement.bind_text_by_index(3, "application/octet-stream")
  session.execute(statement)
end
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

### Counter Types

```ruby
# Counter tables require special structure - all non-counter columns must be primary key
session.query(<<~SQL)
  CREATE TABLE page_analytics (
    page_id text,
    category text,
    page_views counter,
    unique_visitors counter,
    PRIMARY KEY (page_id, category)
  )
SQL

# Single counter table
session.query("CREATE TABLE stats (id text PRIMARY KEY, count counter)")

# Increment counters using UPDATE statements
session.query("UPDATE page_analytics SET page_views = page_views + 100, unique_visitors = unique_visitors + 10 WHERE page_id = 'home' AND category = 'tech'")

# Increment single counter
session.query("UPDATE stats SET count = count + 1 WHERE id = 'total_visits'")

# Decrement counters (negative increment)
session.query("UPDATE stats SET count = count - 5 WHERE id = 'total_visits'")

# Use prepared statements with counters (counters map to BigInt type)
prepared = session.prepare("UPDATE stats SET count = count + ? WHERE id = ?")

# Increment by a large value
increment_val = 42.to_cassandra_bigint
statement = prepared.bind([increment_val, "user_actions"])
session.execute(statement)

# Decrement with negative value
decrement_val = (-10).to_cassandra_bigint
statement = prepared.bind([decrement_val, "user_actions"])
session.execute(statement)

# Query counter values (returns BigInt type)
result = session.query("SELECT count FROM stats WHERE id = 'total_visits'")
row = result.to_a.first
counter_value = row[0]

puts counter_value.class  # => CassandraC::Types::BigInt
puts counter_value.to_i   # => Current counter value

# Batch counter operations (requires COUNTER BATCH)
session.query(<<~SQL)
  BEGIN COUNTER BATCH
    UPDATE stats SET count = count + 1 WHERE id = 'batch_test1';
    UPDATE stats SET count = count + 2 WHERE id = 'batch_test2';
  APPLY BATCH
SQL

# Large counter values (near int64 limits)
large_val = 9223372036854775000.to_cassandra_bigint
session.query("UPDATE stats SET count = count + #{large_val.to_i} WHERE id = 'large_counter'")

# Counter arithmetic preserves BigInt type
counter1 = 100.to_cassandra_bigint
counter2 = 50.to_cassandra_bigint
result = counter1 + counter2  # => BigInt(150)
puts result.class             # => CassandraC::Types::BigInt
```

### Inet Types (IP Addresses)

```ruby
require 'ipaddr'

# Create table with inet columns for storing IP addresses
session.query(<<~SQL)
  CREATE TABLE servers (
    id text PRIMARY KEY,
    primary_ip inet,
    backup_ip inet,
    gateway inet,
    created_at timestamp
  )
SQL

# Store IP addresses using string literals
prepared = session.prepare("INSERT INTO servers (id, primary_ip, backup_ip, gateway) VALUES (?, ?, ?, ?)")

# IPv4 addresses as strings
statement = prepared.bind
statement.bind_text_by_index(0, "web-server-1")
statement.bind_inet_by_index(1, "192.168.1.100")      # Primary IP
statement.bind_inet_by_index(2, "192.168.1.101")      # Backup IP  
statement.bind_inet_by_index(3, "192.168.1.1")        # Gateway
session.execute(statement)

# IPv6 addresses as strings
statement = prepared.bind
statement.bind_text_by_index(0, "web-server-2")
statement.bind_inet_by_index(1, "2001:db8::100")      # Primary IPv6
statement.bind_inet_by_index(2, "2001:db8::101")      # Backup IPv6
statement.bind_inet_by_index(3, "2001:db8::1")        # Gateway IPv6
session.execute(statement)

# Store IP addresses using IPAddr objects
statement = prepared.bind
statement.bind_text_by_index(0, "db-server-1")
statement.bind_inet_by_index(1, IPAddr.new("10.0.0.50"))       # Primary IP object
statement.bind_inet_by_index(2, IPAddr.new("10.0.0.51"))       # Backup IP object
statement.bind_inet_by_index(3, IPAddr.new("10.0.0.1"))        # Gateway IP object
session.execute(statement)

# Mixed IPv4 and IPv6
statement = prepared.bind
statement.bind_text_by_index(0, "mixed-server")
statement.bind_inet_by_index(1, "172.16.0.100")               # IPv4 string
statement.bind_inet_by_index(2, IPAddr.new("fe80::1"))        # IPv6 object
statement.bind_inet_by_index(3, "127.0.0.1")                  # Localhost IPv4
session.execute(statement)

# Bind by name instead of index
prepared_named = session.prepare("INSERT INTO servers (id, primary_ip, backup_ip, gateway) VALUES (:server_id, :primary, :backup, :gw)")
statement = prepared_named.bind
statement.bind_text_by_name("server_id", "app-server-1")
statement.bind_inet_by_name("primary", "192.168.100.10")
statement.bind_inet_by_name("backup", IPAddr.new("192.168.100.11"))
statement.bind_inet_by_name("gw", "192.168.100.1")
session.execute(statement)

# Handle null values
statement = prepared.bind
statement.bind_text_by_index(0, "no-backup-server")
statement.bind_inet_by_index(1, "192.168.200.10")      # Primary IP
statement.bind_inet_by_index(2, nil)                    # No backup IP
statement.bind_inet_by_index(3, "192.168.200.1")       # Gateway
session.execute(statement)

# Query inet data (returns as strings)
result = session.query("SELECT id, primary_ip, backup_ip, gateway FROM servers WHERE id = 'web-server-1'")
row = result.to_a.first

server_id = row[0]      # => "web-server-1"
primary_ip = row[1]     # => "192.168.1.100" (String)
backup_ip = row[2]      # => "192.168.1.101" (String)  
gateway = row[3]        # => "192.168.1.1" (String)

puts "Server: #{server_id}"
puts "Primary IP: #{primary_ip} (#{primary_ip.class})"
puts "Backup IP: #{backup_ip}"
puts "Gateway: #{gateway}"

# Convert results to IPAddr objects for network operations
primary_addr = IPAddr.new(primary_ip)
backup_addr = IPAddr.new(backup_ip)
gateway_addr = IPAddr.new(gateway)

# Perform network calculations
puts "Primary is IPv4: #{primary_addr.ipv4?}"
puts "Primary is IPv6: #{primary_addr.ipv6?}"
puts "Primary in same subnet as backup: #{primary_addr.mask(24) == backup_addr.mask(24)}"

# Query all servers and group by IP version
result = session.query("SELECT id, primary_ip FROM servers")
ipv4_servers = []
ipv6_servers = []

result.each do |row|
  server_id = row[0]
  ip_string = row[1]
  
  next if ip_string.nil?  # Skip servers with null IPs
  
  ip_addr = IPAddr.new(ip_string)
  if ip_addr.ipv4?
    ipv4_servers << {id: server_id, ip: ip_string}
  else
    ipv6_servers << {id: server_id, ip: ip_string}
  end
end

puts "\nIPv4 Servers:"
ipv4_servers.each { |server| puts "  #{server[:id]}: #{server[:ip]}" }

puts "\nIPv6 Servers:"  
ipv6_servers.each { |server| puts "  #{server[:id]}: #{server[:ip]}" }

# Special IPv6 formats and edge cases
special_cases = [
  ["localhost-ipv6", "::1"],                                    # IPv6 localhost
  ["any-address", "::"],                                        # IPv6 any address
  ["mapped-ipv4", "::ffff:192.0.2.1"],                        # IPv4-mapped IPv6
  ["full-ipv6", "2001:0db8:85a3:0000:0000:8a2e:0370:7334"],   # Full IPv6 notation
  ["compressed", "2001:db8:85a3::8a2e:370:7334"]              # Compressed IPv6
]

special_cases.each do |server_id, ip_address|
  statement = prepared.bind
  statement.bind_text_by_index(0, server_id)
  statement.bind_inet_by_index(1, ip_address)
  statement.bind_inet_by_index(2, nil)  # No backup
  statement.bind_inet_by_index(3, nil)  # No gateway
  session.execute(statement)
  
  puts "Stored #{server_id}: #{ip_address}"
end

# Query and validate special cases
special_cases.each do |server_id, original_ip|
  result = session.query("SELECT primary_ip FROM servers WHERE id = '#{server_id}'")
  row = result.to_a.first
  stored_ip = row[0]
  
  # Note: Cassandra may normalize IPv6 addresses
  puts "#{server_id}: #{original_ip} -> #{stored_ip}"
  
  # Verify they represent the same address
  original_addr = IPAddr.new(original_ip)
  stored_addr = IPAddr.new(stored_ip)
  puts "  Addresses equal: #{original_addr == stored_addr}"
end
```

### UUID and TimeUUID Types

```ruby
# Create table with UUID and TimeUUID columns
session.query(<<~SQL)
  CREATE TABLE events (
    id uuid PRIMARY KEY,
    event_id timeuuid,
    user_id uuid,
    event_name text,
    created_at timeuuid,
    updated_at timeuuid
  )
SQL

# Generate UUIDs and TimeUUIDs
event_uuid = CassandraC::Types::Uuid.generate
user_uuid = CassandraC::Types::Uuid.generate  # Generate random UUID instead of hard-coded
event_timeuuid = CassandraC::Types::TimeUuid.generate
created_timeuuid = Time.now.to_cassandra_timeuuid

# Insert using UUID and TimeUUID types
prepared = session.prepare("INSERT INTO events (id, event_id, user_id, event_name, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)")

# Type-specific binding methods
statement = prepared.bind
statement.bind_uuid_by_index(0, event_uuid)                    # Random UUID
statement.bind_timeuuid_by_index(1, event_timeuuid)           # Generated TimeUUID
statement.bind_uuid_by_index(2, user_uuid)                    # UUID from string
statement.bind_text_by_index(3, "user_login")
statement.bind_timeuuid_by_index(4, created_timeuuid)         # TimeUUID from Time
statement.bind_timeuuid_by_index(5, CassandraC::Types::TimeUuid.generate)
session.execute(statement)

# Bind by name instead of index
prepared_named = session.prepare("INSERT INTO events (id, event_id, user_id, event_name, created_at) VALUES (:id, :event_id, :user_id, :event_name, :created_at)")
statement = prepared_named.bind
statement.bind_uuid_by_name("id", CassandraC::Types::Uuid.generate)
statement.bind_timeuuid_by_name("event_id", CassandraC::Types::TimeUuid.generate)
statement.bind_uuid_by_name("user_id", CassandraC::Types::Uuid.generate)
statement.bind_text_by_name("event_name", "page_view")
statement.bind_timeuuid_by_name("created_at", Time.new(2023, 6, 15, 12, 30, 45).to_cassandra_timeuuid)
session.execute(statement)

# Bind raw string values (automatically converted)
statement = prepared.bind
statement.bind_uuid_by_index(0, CassandraC::Types::Uuid.generate.to_s)  # Generated UUID as string
statement.bind_timeuuid_by_index(1, CassandraC::Types::TimeUuid.generate.to_s)  # TimeUUID as string
statement.bind_uuid_by_index(2, user_uuid)
statement.bind_text_by_index(3, "button_click")
statement.bind_timeuuid_by_index(4, created_timeuuid)
session.execute(statement)

# Query UUID and TimeUUID data (returns typed objects)
result = session.query("SELECT id, event_id, user_id, event_name, created_at FROM events")
result.each do |row|
  id, event_id, user_id, event_name, created_at = row
  
  puts "Event: #{event_name}"
  puts "  ID: #{id} (#{id.class})"                    # CassandraC::Types::Uuid
  puts "  Event ID: #{event_id} (#{event_id.class})"  # CassandraC::Types::TimeUuid
  puts "  User ID: #{user_id}"
  puts "  Created: #{created_at.timestamp}"            # Extract Time from TimeUUID
  puts "  TimeUUID: #{created_at}"
  puts
end

# Generate TimeUUIDs for specific timestamps
timestamps = [
  Time.new(2023, 1, 1, 0, 0, 0),    # New Year
  Time.new(2023, 6, 15, 12, 30, 45), # Mid year
  Time.new(2023, 12, 31, 23, 59, 59) # Year end
]

timestamps.each_with_index do |timestamp, index|
  timeuuid = CassandraC::Types::TimeUuid.from_time(timestamp)
  
  statement = prepared.bind
  statement.bind_uuid_by_index(0, CassandraC::Types::Uuid.generate)
  statement.bind_timeuuid_by_index(1, timeuuid)
  statement.bind_uuid_by_index(2, user_uuid)
  statement.bind_text_by_index(3, "scheduled_event_#{index}")
  statement.bind_timeuuid_by_index(4, timeuuid)
  session.execute(statement)
  
  puts "Scheduled event #{index}: #{timestamp} -> #{timeuuid}"
  puts "  Extracted time: #{timeuuid.timestamp}"
  puts "  Time match: #{(timestamp.to_f - timeuuid.timestamp.to_f).abs < 0.01}"
  puts
end

# UUID comparison and validation
uuid_str = CassandraC::Types::Uuid.generate.to_s
uuid1 = uuid_str.to_cassandra_uuid
uuid2 = uuid_str.upcase.to_cassandra_uuid  # Same UUID, different case
uuid3 = CassandraC::Types::Uuid.generate

puts "UUID Comparisons:"
puts "  uuid1 == uuid2: #{uuid1 == uuid2}"        # true (case insensitive)
puts "  uuid1 == uuid1.to_s: #{uuid1 == uuid1.to_s}"  # true (can compare with strings)
puts "  uuid1 == uuid3: #{uuid1 == uuid3}"        # false (different UUIDs)

# TimeUUID chronological ordering
past_time = Time.new(2020, 1, 1)
current_time = Time.now
future_time = Time.new(2030, 1, 1)

past_timeuuid = CassandraC::Types::TimeUuid.from_time(past_time)
current_timeuuid = CassandraC::Types::TimeUuid.from_time(current_time)
future_timeuuid = CassandraC::Types::TimeUuid.from_time(future_time)

puts "\nTimeUUID Chronological Order:"
puts "  Past: #{past_timeuuid} (#{past_time})"
puts "  Current: #{current_timeuuid} (#{current_time})"
puts "  Future: #{future_timeuuid} (#{future_time})"
puts "  Past < Current: #{past_timeuuid < current_timeuuid}"
puts "  Current < Future: #{current_timeuuid < future_timeuuid}"

# Handle null values
prepared_null = session.prepare("INSERT INTO events (id, event_id, user_id, event_name) VALUES (?, ?, ?, ?)")
statement = prepared_null.bind
statement.bind_uuid_by_index(0, CassandraC::Types::Uuid.generate)
statement.bind_timeuuid_by_index(1, nil)    # Null TimeUUID
statement.bind_uuid_by_index(2, nil)        # Null UUID
statement.bind_text_by_index(3, "null_test")
session.execute(statement)

# Query with null values
result = session.query("SELECT event_id, user_id FROM events WHERE event_name = 'null_test'")
row = result.to_a.first
puts "\nNull handling:"
puts "  Event ID: #{row[0].inspect}"  # nil
puts "  User ID: #{row[1].inspect}"   # nil

# TimeUUID utilities and edge cases
puts "\nTimeUUID Utilities:"

# Generate multiple TimeUUIDs in sequence
sequence_timeuuids = []
5.times do |i|
  # Small delay to ensure different timestamps
  sleep(0.001)
  timeuuid = CassandraC::Types::TimeUuid.generate
  sequence_timeuuids << timeuuid
  puts "  #{i}: #{timeuuid} -> #{timeuuid.timestamp.strftime('%H:%M:%S.%L')}"
end

# Verify chronological order
puts "  Chronologically ordered: #{sequence_timeuuids == sequence_timeuuids.sort}"

# TimeUUID version validation
puts "\nTimeUUID Version Validation:"
valid_timeuuid = CassandraC::Types::TimeUuid.generate
puts "  Generated TimeUUID: #{valid_timeuuid}"
puts "  Version (should be 1): #{valid_timeuuid.to_s[14]}"
puts "  Variant (should be 8-b): #{valid_timeuuid.to_s[19]}"

# Try to create TimeUUID from non-version-1 UUID (will fail)
begin
  invalid_uuid = SecureRandom.uuid  # SecureRandom.uuid generates version 4 UUIDs
  CassandraC::Types::TimeUuid.new(invalid_uuid)
rescue ArgumentError => e
  puts "  Expected error for version 4 UUID: #{e.message}"
end

# UUID format validation
puts "\nUUID Format Validation:"
# Generate a sample UUID for testing formats
sample_uuid = CassandraC::Types::Uuid.generate.to_s
valid_formats = [
  sample_uuid,
  sample_uuid.upcase,  # Mixed case
  "00000000-0000-0000-0000-000000000000"   # All zeros (special case)
]

invalid_formats = [
  sample_uuid[0..-2],   # Missing digit
  sample_uuid + "x",    # Extra character
  sample_uuid.gsub("-", ""),  # No hyphens
  "not-a-uuid-at-all"
]

valid_formats.each do |format|
  begin
    uuid = CassandraC::Types::Uuid.new(format)
    puts "  Valid: #{format} -> #{uuid}"
  rescue ArgumentError => e
    puts "  Unexpected error: #{format} -> #{e.message}"
  end
end

invalid_formats.each do |format|
  begin
    uuid = CassandraC::Types::Uuid.new(format)
    puts "  Unexpected success: #{format} -> #{uuid}"
  rescue ArgumentError => e
    puts "  Expected error: #{format} -> #{e.message}"
  end
end

# Performance considerations for UUID generation
puts "\nUUID Generation Performance:"
start_time = Time.now

# Generate many UUIDs
1000.times { CassandraC::Types::Uuid.generate }
uuid_time = Time.now - start_time

start_time = Time.now

# Generate many TimeUUIDs
1000.times { CassandraC::Types::TimeUuid.generate }
timeuuid_time = Time.now - start_time

puts "  1000 UUIDs: #{(uuid_time * 1000).round(2)}ms"
puts "  1000 TimeUUIDs: #{(timeuuid_time * 1000).round(2)}ms"
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