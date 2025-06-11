# frozen_string_literal: true

require "test_helper"

class TestDoubleTypes < Minitest::Test
  def test_double_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, double_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 60, :int)
    statement.bind_by_index(1, 2.718281828459045, :double)
    session.execute(statement)

    result = session.query("SELECT double_val FROM cassandra_c_test.decimal_types WHERE id = 60")
    assert_in_delta 2.718281828459045, result.to_a.first[0], 0.000001
  end

  def test_double_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, double_val) VALUES (:id, :double_val)", 2)

    statement.bind_by_name("id", 61, :int)
    statement.bind_by_name("double_val", 9.876543210987654, :double)
    session.execute(statement)

    result = session.query("SELECT double_val FROM cassandra_c_test.decimal_types WHERE id = 61")
    assert_in_delta 9.876543210987654, result.to_a.first[0], 0.000001
  end

  def test_double_automatic_inference
    # Test float automatic inference
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, double_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 62, :int)  # Explicit hint for ID
    statement.bind_by_index(1, 3.14159)   # Should infer as double
    session.execute(statement)

    result = session.query("SELECT double_val FROM cassandra_c_test.decimal_types WHERE id = 62")
    assert_instance_of Float, result.to_a.first[0]
    assert_in_delta 3.14159, result.to_a.first[0], 0.00001
  end

  def test_double_special_values
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, double_val) VALUES (?, ?)", 2)

    # Test positive infinity
    statement.bind_by_index(0, 63, :int)
    statement.bind_by_index(1, Float::INFINITY, :double)
    session.execute(statement)

    # Test negative infinity
    statement.bind_by_index(0, 64, :int)
    statement.bind_by_index(1, -Float::INFINITY, :double)
    session.execute(statement)

    # Test positive infinity
    result = session.query("SELECT double_val FROM cassandra_c_test.decimal_types WHERE id = 63")
    assert result.to_a.first[0].infinite?
    assert result.to_a.first[0] > 0

    # Test negative infinity
    result = session.query("SELECT double_val FROM cassandra_c_test.decimal_types WHERE id = 64")
    assert result.to_a.first[0].infinite?
    assert result.to_a.first[0] < 0
  end

  def test_double_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, double_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 65, :int)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT double_val FROM cassandra_c_test.decimal_types WHERE id = 65")
    assert_nil result.to_a.first[0]
  end

  def test_double_high_precision
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, double_val) VALUES (?, ?)", 2)

    # Test 64-bit double precision
    precise_value = 1.7976931348623157e+308  # Close to max double value
    statement.bind_by_index(0, 66, :int)
    statement.bind_by_index(1, precise_value, :double)
    session.execute(statement)

    result = session.query("SELECT double_val FROM cassandra_c_test.decimal_types WHERE id = 66")
    retrieved = result.to_a.first[0]
    assert_instance_of Float, retrieved
    assert_in_delta precise_value, retrieved, precise_value * 0.001
  end
end
