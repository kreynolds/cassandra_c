# frozen_string_literal: true

require "test_helper"

class TestSmallintTypes < Minitest::Test
  def test_smallint_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, small_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 10, :int)
    statement.bind_by_index(1, 1000, :smallint)
    session.execute(statement)

    result = session.query("SELECT small_val FROM cassandra_c_test.integer_types WHERE id = 10")
    assert_equal 1000, result.to_a.first[0]
  end

  def test_smallint_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, small_val) VALUES (:id, :small)", 2)

    statement.bind_by_name("id", 11, :int)
    statement.bind_by_name("small", 32767, :smallint)
    session.execute(statement)

    result = session.query("SELECT small_val FROM cassandra_c_test.integer_types WHERE id = 11")
    assert_equal 32767, result.to_a.first[0]
  end

  def test_smallint_boundary_values
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, small_val) VALUES (?, ?)", 2)

    # Test minimum value
    statement.bind_by_index(0, 12, :int)
    statement.bind_by_index(1, -32768, :smallint)
    session.execute(statement)

    # Test maximum value
    statement.bind_by_index(0, 13, :int)
    statement.bind_by_index(1, 32767, :smallint)
    session.execute(statement)

    # Test minimum value
    result = session.query("SELECT small_val FROM cassandra_c_test.integer_types WHERE id = 12")
    assert_equal(-32768, result.to_a.first[0])

    # Test maximum value
    result = session.query("SELECT small_val FROM cassandra_c_test.integer_types WHERE id = 13")
    assert_equal 32767, result.to_a.first[0]
  end

  def test_smallint_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, small_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 14, :int)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT small_val FROM cassandra_c_test.integer_types WHERE id = 14")
    assert_nil result.to_a.first[0]
  end
end
