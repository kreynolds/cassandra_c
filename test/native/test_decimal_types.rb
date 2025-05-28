# frozen_string_literal: true

require "test_helper"

class TestDecimalTypes < Minitest::Test
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
      session.query("CREATE TABLE IF NOT EXISTS cassandra_c_test.decimal_types (id int PRIMARY KEY, float_val float, double_val double)")
      session.close
      @@tables_created = true
    end

    @session = CassandraC::Native::Session.new
    @session.connect(@cluster)
  end

  def teardown
    @session&.close
  end

  def test_decimal_type_creation
    # Test that we can create typed floats and doubles
    float_val = 3.14.to_cassandra_float
    double_val = 2.71828.to_cassandra_double

    assert_instance_of CassandraC::Types::Float, float_val
    assert_instance_of CassandraC::Types::Double, double_val

    # Test to_f returns regular Float
    assert_instance_of Float, float_val.to_f
    assert_instance_of Float, double_val.to_f

    # Test values are preserved
    assert_in_delta 3.14, float_val.to_f, 0.001
    assert_in_delta 2.71828, double_val.to_f, 0.00001
  end

  def test_decimal_type_creation_from_integers
    # Test creation from integers
    int_float = 42.to_cassandra_float
    int_double = 123.to_cassandra_double

    assert_instance_of CassandraC::Types::Float, int_float
    assert_instance_of CassandraC::Types::Double, int_double

    assert_equal 42.0, int_float.to_f
    assert_equal 123.0, int_double.to_f
  end

  def test_prepared_statement_with_typed_decimals
    # Test binding typed decimals to prepared statements
    prepared = @session.prepare("INSERT INTO cassandra_c_test.decimal_types (id, float_val, double_val) VALUES (?, ?, ?)")

    float_val = 3.14159.to_cassandra_float
    double_val = 2.718281828459045.to_cassandra_double

    statement = prepared.bind([1.to_cassandra_int, float_val, double_val])
    @session.execute(statement)

    # Verify the data was inserted correctly
    result = @session.query("SELECT * FROM cassandra_c_test.decimal_types WHERE id = 1")
    rows = result.to_a
    assert_equal(1, rows.length)

    row = rows.first
    # Based on similar pattern to integer types, column order from Cassandra may be:
    # [0] = Int(1) - id
    # [1] = Double(...) - double_val
    # [2] = Float(...) - float_val

    assert_instance_of CassandraC::Types::Int, row[0]      # id
    assert_instance_of CassandraC::Types::Double, row[1]   # double_val
    assert_instance_of CassandraC::Types::Float, row[2]    # float_val

    assert_equal(1, row[0].to_i)                           # id
    assert_in_delta(2.718281828459045, row[1].to_f, 0.0000000000001) # double_val (high precision)
    assert_in_delta(3.14159, row[2].to_f, 0.00001)        # float_val (lower precision)
  end

  def test_type_specific_binding_methods
    # Test using type-specific binding methods
    prepared = @session.prepare("INSERT INTO cassandra_c_test.decimal_types (id, float_val, double_val) VALUES (?, ?, ?)")

    statement = prepared.bind
    statement.bind_by_index(0, 2.to_cassandra_int)
    statement.bind_float_by_index(1, 1.23.to_cassandra_float)
    statement.bind_double_by_index(2, 4.56789.to_cassandra_double)

    @session.execute(statement)

    # Verify the data was inserted correctly
    result = @session.query("SELECT * FROM cassandra_c_test.decimal_types WHERE id = 2")
    rows = result.to_a
    assert_equal(1, rows.length)

    row = rows.first
    assert_instance_of CassandraC::Types::Int, row[0]      # id
    assert_instance_of CassandraC::Types::Double, row[1]   # double_val
    assert_instance_of CassandraC::Types::Float, row[2]    # float_val

    assert_equal(2, row[0].to_i)                           # id
    assert_in_delta(4.56789, row[1].to_f, 0.00001)        # double_val
    assert_in_delta(1.23, row[2].to_f, 0.001)             # float_val
  end

  def test_binding_with_raw_numeric_values
    # Test binding raw Ruby numeric values (should still work)
    prepared = @session.prepare("INSERT INTO cassandra_c_test.decimal_types (id, float_val, double_val) VALUES (?, ?, ?)")

    statement = prepared.bind
    statement.bind_by_index(0, 3)  # Raw integer
    statement.bind_float_by_index(1, 7.89)  # Raw float
    statement.bind_double_by_index(2, 12.34567890123)  # Raw float

    @session.execute(statement)

    # Verify the data was inserted correctly
    result = @session.query("SELECT * FROM cassandra_c_test.decimal_types WHERE id = 3")
    rows = result.to_a
    assert_equal(1, rows.length)

    row = rows.first
    assert_instance_of CassandraC::Types::Int, row[0]      # id
    assert_instance_of CassandraC::Types::Double, row[1]   # double_val
    assert_instance_of CassandraC::Types::Float, row[2]    # float_val

    assert_equal(3, row[0].to_i)                           # id
    assert_in_delta(12.34567890123, row[1].to_f, 0.00000000001) # double_val
    assert_in_delta(7.89, row[2].to_f, 0.001)             # float_val
  end

  def test_arithmetic_operations
    # Test that arithmetic operations work and return typed floats/doubles
    float1 = 1.5.to_cassandra_float
    float2 = 2.5.to_cassandra_float

    result = float1 + float2
    assert_instance_of CassandraC::Types::Float, result
    assert_in_delta(4.0, result.to_f, 0.001)

    result = float2 - float1
    assert_instance_of CassandraC::Types::Float, result
    assert_in_delta(1.0, result.to_f, 0.001)

    result = float1 * 3
    assert_instance_of CassandraC::Types::Float, result
    assert_in_delta(4.5, result.to_f, 0.001)

    # Test with doubles
    double1 = 1.5.to_cassandra_double
    double2 = 2.5.to_cassandra_double

    result = double1 + double2
    assert_instance_of CassandraC::Types::Double, result
    assert_in_delta(4.0, result.to_f, 0.000001)
  end

  def test_float_precision_differences
    # Test that we can create both float and double types
    # The actual precision differences are handled at the Cassandra level
    # Here we just verify both types can be created and used

    precise_value = 1.23456789012345

    float_val = precise_value.to_cassandra_float
    double_val = precise_value.to_cassandra_double

    # Verify types are correct
    assert_instance_of CassandraC::Types::Float, float_val
    assert_instance_of CassandraC::Types::Double, double_val

    # Both should preserve the value reasonably well for testing
    assert_in_delta precise_value, float_val.to_f, 0.0001
    assert_in_delta precise_value, double_val.to_f, 0.0001
  end

  def test_comparison_operations
    # Test comparison operations work correctly
    float1 = 1.5.to_cassandra_float
    float2 = 2.5.to_cassandra_float
    float3 = 1.5.to_cassandra_float

    assert float2 > float1
    assert float1 < float2
    assert float1 == float3
    assert float1 >= float3
    assert float2 >= float1

    # Test comparison with regular Ruby numbers
    assert_in_delta 1.5, float1.to_f, 0.001
    assert float2 > 1.5
  end

  def test_marker_methods
    # Test marker methods for type identification
    float_val = 1.5.to_cassandra_float
    double_val = 2.5.to_cassandra_double

    assert float_val.cassandra_typed_float?
    assert double_val.cassandra_typed_double?

    # Test that regular floats don't have these methods
    regular_float = 1.5
    refute regular_float.respond_to?(:cassandra_typed_float?)
    refute regular_float.respond_to?(:cassandra_typed_double?)
  end
end
