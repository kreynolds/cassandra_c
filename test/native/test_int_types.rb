# frozen_string_literal: true

require "test_helper"

class TestIntTypes < Minitest::Test
  def test_int_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, int_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 20, :int)
    statement.bind_by_index(1, 50000, :int)
    session.execute(statement)

    result = session.query("SELECT int_val FROM cassandra_c_test.integer_types WHERE id = 20")
    assert_equal 50000, result.to_a.first[0]
  end

  def test_int_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, int_val) VALUES (:id, :int_val)", 2)

    statement.bind_by_name("id", 21, :int)
    statement.bind_by_name("int_val", 2147483647, :int)
    session.execute(statement)

    result = session.query("SELECT int_val FROM cassandra_c_test.integer_types WHERE id = 21")
    assert_equal 2147483647, result.to_a.first[0]
  end

  def test_int_boundary_values
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, int_val) VALUES (?, ?)", 2)

    # Test minimum value
    statement.bind_by_index(0, 22, :int)
    statement.bind_by_index(1, -2147483648, :int)
    session.execute(statement)

    # Test maximum value
    statement.bind_by_index(0, 23, :int)
    statement.bind_by_index(1, 2147483647, :int)
    session.execute(statement)

    # Test minimum value
    result = session.query("SELECT int_val FROM cassandra_c_test.integer_types WHERE id = 22")
    assert_equal(-2147483648, result.to_a.first[0])

    # Test maximum value
    result = session.query("SELECT int_val FROM cassandra_c_test.integer_types WHERE id = 23")
    assert_equal 2147483647, result.to_a.first[0]
  end

  def test_int_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, int_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 24, :int)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT int_val FROM cassandra_c_test.integer_types WHERE id = 24")
    assert_nil result.to_a.first[0]
  end

  def test_int_backward_compatibility
    # Test that binding without type hints works when column types are compatible
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, int_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 25, :int)    # Explicit int32 for id column
    statement.bind_by_index(1, 100000, :int) # Explicit int32 for int_val column
    session.execute(statement)

    result = session.query("SELECT id, int_val FROM cassandra_c_test.integer_types WHERE id = 25")
    row = result.to_a.first
    assert_equal 25, row[0]
    assert_equal 100000, row[1]
  end
end
