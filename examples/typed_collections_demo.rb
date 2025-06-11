#!/usr/bin/env ruby
# frozen_string_literal: true

# Type-Hinted Collections Demo
#
# This example demonstrates the new type-hinted collection binding feature
# which allows you to specify exact Cassandra types for collection elements,
# avoiding Ruby object allocation overhead and ensuring proper type conversion.

require "bundler/setup"
require "cassandra_c"
require "set"

# Connect to Cassandra
cluster = CassandraC::Native::Cluster.new
cluster.contact_points = "127.0.0.1"
cluster.port = 9042

session = CassandraC::Native::Session.new
session.connect(cluster)

# Create keyspace and tables for demo
session.query("CREATE KEYSPACE IF NOT EXISTS demo WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")

session.query(<<~SQL)
  CREATE TABLE IF NOT EXISTS demo.typed_collections (
    id text PRIMARY KEY,
    tinyint_list list<tinyint>,
    bigint_set set<bigint>,
    text_to_int_map map<text, int>
  )
SQL

puts "=== Type-Hinted Collection Binding Demo ==="
puts

# Example 1: Type-hinted List binding
puts "1. List with tinyint type hint"
puts "   Ruby data: [1, 2, 3, 127, -128]"

statement = CassandraC::Native::Statement.new(
  "INSERT INTO demo.typed_collections (id, tinyint_list) VALUES (?, ?)", 2
)
statement.bind_by_index(0, "list_demo")
statement.bind_list_by_index(1, [1, 2, 3, 127, -128], :tinyint)

session.execute(statement)

# Retrieve and display
result = session.query("SELECT tinyint_list FROM demo.typed_collections WHERE id = 'list_demo'")
retrieved_list = result.to_a.first[0]
puts "   Retrieved: #{retrieved_list.map(&:to_i)}"
puts "   Type conversion handled in C - no extra Ruby objects allocated!"
puts

# Example 2: Type-hinted Set binding
puts "2. Set with bigint type hint"
big_numbers = Set.new([1_000_000_000_000, 2_000_000_000_000, 9_223_372_036_854_775_807])
puts "   Ruby data: #{big_numbers.to_a}"

statement = CassandraC::Native::Statement.new(
  "INSERT INTO demo.typed_collections (id, bigint_set) VALUES (?, ?)", 2
)
statement.bind_by_index(0, "set_demo")
statement.bind_set_by_index(1, big_numbers, :bigint)

session.execute(statement)

# Retrieve and display
result = session.query("SELECT bigint_set FROM demo.typed_collections WHERE id = 'set_demo'")
retrieved_set = result.to_a.first[0]
puts "   Retrieved: #{Set.new(retrieved_set.map(&:to_i)).to_a}"
puts "   Large integers handled efficiently with type hint!"
puts

# Example 3: Type-hinted Map binding
puts "3. Map with text keys and int values"
scores = {"alice" => 100, "bob" => 95, "charlie" => 87}
puts "   Ruby data: #{scores}"

statement = CassandraC::Native::Statement.new(
  "INSERT INTO demo.typed_collections (id, text_to_int_map) VALUES (?, ?)", 2
)
statement.bind_by_index(0, "map_demo")
statement.bind_map_by_index(1, scores, :text, :int)

session.execute(statement)

# Retrieve and display
result = session.query("SELECT text_to_int_map FROM demo.typed_collections WHERE id = 'map_demo'")
retrieved_map = result.to_a.first[0]
puts "   Retrieved: #{retrieved_map.transform_values(&:to_i)}"
puts "   Both key and value types can be specified!"
puts

# Example 4: Binding by name with type hints
puts "4. Named parameter binding with type hints"
statement = CassandraC::Native::Statement.new(
  "INSERT INTO demo.typed_collections (id, tinyint_list) VALUES (:id, :numbers)", 2
)
statement.bind_by_name("id", "named_demo")
statement.bind_list_by_name("numbers", [42, -42, 100], :tinyint)

session.execute(statement)

result = session.query("SELECT tinyint_list FROM demo.typed_collections WHERE id = 'named_demo'")
retrieved_list = result.to_a.first[0]
puts "   Retrieved: #{retrieved_list.map(&:to_i)}"
puts "   Named parameters work with type hints too!"
puts

# Example 5: Backward compatibility
puts "5. Backward compatibility - works without type hints"
puts "   (Note: For precise type control, especially with numeric collections,"
puts "    type hints are recommended to avoid type mismatches)"

# Use the general bind_by_index method which uses the existing logic
statement = CassandraC::Native::Statement.new(
  "INSERT INTO demo.typed_collections (id, text_to_int_map) VALUES (?, ?)", 2
)
statement.bind_by_index(0, "compat_demo")
statement.bind_by_index(1, {"test" => 123})  # Uses existing logic

session.execute(statement)

result = session.query("SELECT text_to_int_map FROM demo.typed_collections WHERE id = 'compat_demo'")
retrieved_map = result.to_a.first[0]
puts "   Retrieved: #{retrieved_map.transform_values(&:to_i)}"
puts "   Existing bind_by_index code continues to work!"
puts

# Performance comparison note
puts "=== Performance Benefits ==="
puts "• Type conversion happens entirely in C"
puts "• No intermediate Ruby objects allocated for collection elements"
puts "• Direct binding to Cassandra driver without Ruby type introspection"
puts "• Backward compatible - existing code unaffected"
puts

# Available type hints
puts "=== Available Type Hints ==="
puts "Numeric: :tinyint, :smallint, :int, :bigint, :varint, :float, :double"
puts "String:  :text, :varchar, :ascii"
puts "Other:   :boolean, :blob, :uuid, :timeuuid, :inet"
puts

# Clean up
session.query("DROP KEYSPACE demo")
session.close

puts "Demo completed successfully!"
