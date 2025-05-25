# frozen_string_literal: true

require "test_helper"

class TestIntegerTypes < Minitest::Test
  def self.startup
    cluster = CassandraC::Native::Cluster.new.tap { |cluster|
      cluster.contact_points = "127.0.0.1"
      cluster.port = 9042
    }
    session = CassandraC::Native::Session.new
    session.connect(cluster)
    session.query("CREATE KEYSPACE IF NOT EXISTS cassandra_c_test WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}")
    session.close
  end

  def setup
    @cluster = CassandraC::Native::Cluster.new.tap { |cluster|
      cluster.contact_points = "127.0.0.1"
      cluster.port = 9042
    }
    self.class.startup unless defined?(@@keyspace_created)
    @@keyspace_created = true

    # Create test tables
    unless defined?(@@tables_created)
      session = CassandraC::Native::Session.new
      session.connect(@cluster)
      session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.integer_types (id int PRIMARY KEY, tiny_val tinyint, small_val smallint, int_val int, big_val bigint, var_val varint)")
      session.close
      @@tables_created = true
    end

    @session = CassandraC::Native::Session.new
    @session.connect(@cluster)
  end

  def teardown
    @session&.close
  end

  def test_integer_type_creation
    # Test that we can create typed integers
    tiny = 42.to_cassandra_tinyint
    small = 1000.to_cassandra_smallint
    int = 50000.to_cassandra_int
    big = 9223372036854775807.to_cassandra_bigint
    var = 123456789012345678901234567890.to_cassandra_varint

    assert_instance_of CassandraC::Types::TinyInt, tiny
    assert_instance_of CassandraC::Types::SmallInt, small
    assert_instance_of CassandraC::Types::Int, int
    assert_instance_of CassandraC::Types::BigInt, big
    assert_instance_of CassandraC::Types::VarInt, var

    # Test to_i returns regular Integer
    assert_instance_of Integer, tiny.to_i
    assert_instance_of Integer, small.to_i
    assert_instance_of Integer, int.to_i
    assert_instance_of Integer, big.to_i
    assert_instance_of Integer, var.to_i
  end

  def test_integer_overflow_handling
    # Test that values wrap correctly for fixed-width types
    tiny_overflow = 128.to_cassandra_tinyint  # Should wrap to -128
    assert_equal(-128, tiny_overflow.to_i)

    small_overflow = 32768.to_cassandra_smallint  # Should wrap to -32768
    assert_equal(-32768, small_overflow.to_i)

    int_overflow = 2147483648.to_cassandra_int  # Should wrap to -2147483648
    assert_equal(-2147483648, int_overflow.to_i)
  end

  def test_prepared_statement_with_typed_integers
    # Test binding typed integers to prepared statements
    prepared = @session.prepare("INSERT INTO cassandra_c_test.integer_types (id, tiny_val, small_val, int_val, big_val, var_val) VALUES (?, ?, ?, ?, ?, ?)")

    tiny = 42.to_cassandra_tinyint
    small = 1000.to_cassandra_smallint
    int = 50000.to_cassandra_int
    big = 9223372036854775807.to_cassandra_bigint
    var = 123456789012345678901234567890.to_cassandra_varint

    statement = prepared.bind([1.to_cassandra_int, tiny, small, int, big, var])
    @session.execute(statement)

    # Verify the data was inserted correctly
    result = @session.query("SELECT * FROM cassandra_c_test.integer_types WHERE id = 1")
    rows = result.to_a
    assert_equal(1, rows.length)

    row = rows.first
    # Based on debug output, actual column order from Cassandra is:
    # [0] = Int(1) - id
    # [1] = BigInt(9223372036854775807) - big_val
    # [2] = Int(50000) - int_val
    # [3] = SmallInt(1000) - small_val
    # [4] = TinyInt(42) - tiny_val
    # [5] = VarInt(0) - var_val (has issue with large numbers)

    assert_instance_of CassandraC::Types::Int, row[0]      # id
    assert_instance_of CassandraC::Types::BigInt, row[1]   # big_val
    assert_instance_of CassandraC::Types::Int, row[2]      # int_val
    assert_instance_of CassandraC::Types::SmallInt, row[3] # small_val
    assert_instance_of CassandraC::Types::TinyInt, row[4]  # tiny_val
    assert_instance_of CassandraC::Types::VarInt, row[5]   # var_val

    assert_equal(1, row[0].to_i)                           # id
    assert_equal(9223372036854775807, row[1].to_i)         # big_val
    assert_equal(50000, row[2].to_i)                       # int_val
    assert_equal(1000, row[3].to_i)                        # small_val
    assert_equal(42, row[4].to_i)                          # tiny_val
    assert_equal(123456789012345678901234567890, row[5].to_i) # var_val
  end

  def test_arithmetic_operations
    # Test that arithmetic operations work and return typed integers
    tiny1 = 10.to_cassandra_tinyint
    tiny2 = 20.to_cassandra_tinyint

    result = tiny1 + tiny2
    assert_instance_of CassandraC::Types::TinyInt, result
    assert_equal(30, result.to_i)

    result = tiny2 - tiny1
    assert_instance_of CassandraC::Types::TinyInt, result
    assert_equal(10, result.to_i)

    result = tiny1 * 3
    assert_instance_of CassandraC::Types::TinyInt, result
    assert_equal(30, result.to_i)
  end
end
