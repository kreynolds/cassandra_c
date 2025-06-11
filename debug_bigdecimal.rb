require_relative "test/test_helper"
require "bigdecimal"

# Set up session
cluster = CassandraC::Native::Cluster.new.tap { |c|
  c.contact_points = "127.0.0.1"
  c.port = 9042
}
session = CassandraC::Native::Session.new
session.connect(cluster)
session.execute("USE cassandra_c_test")

# Test the exact same BigDecimal value both ways
test_decimal = BigDecimal("123.456")
puts "Testing BigDecimal: #{test_decimal.inspect}"
puts "BigDecimal to_s: '#{test_decimal}'"
puts "BigDecimal to_s('F'): '#{test_decimal.to_s("F")}'"

# Test explicit binding
puts "\n=== EXPLICIT BINDING ==="
statement1 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, decimal_val) VALUES (?, ?)", 2)
statement1.bind_by_index(0, 8001, :int)
statement1.bind_decimal_by_index(1, test_decimal)
session.execute(statement1)

result1 = session.query("SELECT decimal_val FROM cassandra_c_test.decimal_types WHERE id = 8001")
explicit_result = result1.to_a.first[0]
puts "Explicit result: #{explicit_result.inspect} (#{explicit_result.class})"

# Test automatic inference
puts "\n=== AUTOMATIC INFERENCE ==="
statement2 = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, decimal_val) VALUES (?, ?)", 2)
statement2.bind_by_index(0, 8002, :int)
statement2.bind_by_index(1, test_decimal) # No type hint - should auto-infer
session.execute(statement2)

result2 = session.query("SELECT decimal_val FROM cassandra_c_test.decimal_types WHERE id = 8002")
auto_result = result2.to_a.first[0]
puts "Automatic result: #{auto_result.inspect} (#{auto_result.class})"

puts "\n=== COMPARISON ==="
puts "Explicit == Automatic: #{explicit_result == auto_result}"
puts "Both are BigDecimal: #{explicit_result.is_a?(BigDecimal) && auto_result.is_a?(BigDecimal)}"
