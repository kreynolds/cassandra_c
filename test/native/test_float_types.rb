# frozen_string_literal: true

require "test_helper"

class TestFloatTypes < Minitest::Test
  def test_float_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, float_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 50, :int)
    statement.bind_by_index(1, 3.14159, :float)
    session.execute(statement)

    result = session.query("SELECT float_val FROM cassandra_c_test.decimal_types WHERE id = 50")
    assert_in_delta 3.14159, result.to_a.first[0], 0.001
  end

  def test_float_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, float_val) VALUES (:id, :float_val)", 2)

    statement.bind_by_name("id", 51, :int)
    statement.bind_by_name("float_val", 1.23456, :float)
    session.execute(statement)

    result = session.query("SELECT float_val FROM cassandra_c_test.decimal_types WHERE id = 51")
    assert_in_delta 1.23456, result.to_a.first[0], 0.001
  end

  def test_float_special_values
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, float_val) VALUES (?, ?)", 2)

    # Test positive infinity
    statement.bind_by_index(0, 52, :int)
    statement.bind_by_index(1, Float::INFINITY, :float)
    session.execute(statement)

    # Test negative infinity
    statement.bind_by_index(0, 53, :int)
    statement.bind_by_index(1, -Float::INFINITY, :float)
    session.execute(statement)

    # Test positive infinity
    result = session.query("SELECT float_val FROM cassandra_c_test.decimal_types WHERE id = 52")
    assert result.to_a.first[0].infinite?
    assert result.to_a.first[0] > 0

    # Test negative infinity
    result = session.query("SELECT float_val FROM cassandra_c_test.decimal_types WHERE id = 53")
    assert result.to_a.first[0].infinite?
    assert result.to_a.first[0] < 0
  end

  def test_float_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, float_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 54, :int)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT float_val FROM cassandra_c_test.decimal_types WHERE id = 54")
    assert_nil result.to_a.first[0]
  end

  def test_float_precision_limits
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, float_val) VALUES (?, ?)", 2)

    # Test 32-bit float precision
    precise_value = 3.4028235e+38  # Close to max float value
    statement.bind_by_index(0, 55, :int)
    statement.bind_by_index(1, precise_value, :float)
    session.execute(statement)

    result = session.query("SELECT float_val FROM cassandra_c_test.decimal_types WHERE id = 55")
    retrieved = result.to_a.first[0]
    assert_instance_of Float, retrieved
    assert_in_delta precise_value, retrieved, precise_value * 0.001
  end
end
