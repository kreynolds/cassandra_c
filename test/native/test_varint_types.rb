# frozen_string_literal: true

require "test_helper"

class TestVarintTypes < Minitest::Test
  def test_varint_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, var_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 40, :int)
    statement.bind_by_index(1, 123456789012345678901234567890, :varint)
    session.execute(statement)

    result = session.query("SELECT var_val FROM cassandra_c_test.integer_types WHERE id = 40")
    assert_equal 123456789012345678901234567890, result.to_a.first[0]
  end

  def test_varint_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, var_val) VALUES (:id, :var)", 2)

    statement.bind_by_name("id", 41, :int)
    statement.bind_by_name("var", -123456789012345678901234567890, :varint)
    session.execute(statement)

    result = session.query("SELECT var_val FROM cassandra_c_test.integer_types WHERE id = 41")
    assert_equal(-123456789012345678901234567890, result.to_a.first[0])
  end

  def test_varint_very_large_numbers
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, var_val) VALUES (?, ?)", 2)

    # Test extremely large positive number
    huge_positive = 12345678901234567890123456789012345678901234567890
    statement.bind_by_index(0, 42, :int)
    statement.bind_by_index(1, huge_positive, :varint)
    session.execute(statement)

    # Test extremely large negative number
    huge_negative = -12345678901234567890123456789012345678901234567890
    statement.bind_by_index(0, 43, :int)
    statement.bind_by_index(1, huge_negative, :varint)
    session.execute(statement)

    # Test huge positive number
    result = session.query("SELECT var_val FROM cassandra_c_test.integer_types WHERE id = 42")
    assert_equal huge_positive, result.to_a.first[0]

    # Test huge negative number
    result = session.query("SELECT var_val FROM cassandra_c_test.integer_types WHERE id = 43")
    assert_equal huge_negative, result.to_a.first[0]
  end

  def test_varint_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, var_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 44, :int)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT var_val FROM cassandra_c_test.integer_types WHERE id = 44")
    assert_nil result.to_a.first[0]
  end

  def test_varint_arithmetic_operations
    huge_number = 12345678901234567890123456789012345678901234567890
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, var_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 45, :int)
    statement.bind_by_index(1, huge_number, :varint)
    session.execute(statement)

    result = session.query("SELECT var_val FROM cassandra_c_test.integer_types WHERE id = 45")
    retrieved = result.to_a.first[0]

    # Test that we can perform basic arithmetic
    assert huge_number > (huge_number - 1)
    assert retrieved == huge_number
    assert_instance_of Integer, retrieved
  end
end
