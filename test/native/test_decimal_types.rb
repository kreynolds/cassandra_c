# frozen_string_literal: true

require "test_helper"

class TestDecimalTypes < Minitest::Test
  def test_decimal_binding_with_type_hints
    require "bigdecimal"

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, decimal_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 70, :int)
    statement.bind_by_index(1, BigDecimal("123.456"), :decimal)
    session.execute(statement)

    result = session.query("SELECT decimal_val FROM cassandra_c_test.decimal_types WHERE id = 70")
    assert_equal BigDecimal("123.456"), result.to_a.first[0]
  end

  def test_decimal_binding_by_name
    require "bigdecimal"

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, decimal_val) VALUES (:id, :decimal_val)", 2)

    statement.bind_by_name("id", 71, :int)
    statement.bind_by_name("decimal_val", BigDecimal("999.123"), :decimal)
    session.execute(statement)

    result = session.query("SELECT decimal_val FROM cassandra_c_test.decimal_types WHERE id = 71")
    assert_equal BigDecimal("999.123"), result.to_a.first[0]
  end

  def test_decimal_automatic_inference
    require "bigdecimal"

    # Test BigDecimal automatic inference
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, decimal_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 72, :int)  # Explicit hint for ID
    statement.bind_by_index(1, BigDecimal("456.789"))  # Should infer as decimal
    session.execute(statement)

    result = session.query("SELECT decimal_val FROM cassandra_c_test.decimal_types WHERE id = 72")
    assert_instance_of BigDecimal, result.to_a.first[0]
    assert_equal BigDecimal("456.789"), result.to_a.first[0]
  end

  def test_decimal_with_string_conversion
    require "bigdecimal"

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, decimal_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 73, :int)
    statement.bind_decimal_by_index(1, "999.123")  # String automatically converted
    session.execute(statement)

    result = session.query("SELECT decimal_val FROM cassandra_c_test.decimal_types WHERE id = 73")
    assert_instance_of BigDecimal, result.to_a.first[0]
    assert_equal BigDecimal("999.123"), result.to_a.first[0]
  end

  def test_decimal_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, decimal_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 74, :int)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT decimal_val FROM cassandra_c_test.decimal_types WHERE id = 74")
    assert_nil result.to_a.first[0]
  end

  def test_decimal_high_precision
    require "bigdecimal"

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.decimal_types (id, decimal_val) VALUES (?, ?)", 2)

    # Test high precision decimal
    precise_value = BigDecimal("123456789.123456789123456789")
    statement.bind_by_index(0, 75, :int)
    statement.bind_by_index(1, precise_value, :decimal)
    session.execute(statement)

    result = session.query("SELECT decimal_val FROM cassandra_c_test.decimal_types WHERE id = 75")
    retrieved = result.to_a.first[0]
    assert_instance_of BigDecimal, retrieved
    assert_equal precise_value, retrieved
  end
end
