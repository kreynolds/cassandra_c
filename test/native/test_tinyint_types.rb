# frozen_string_literal: true

require "test_helper"

class TestTinyintTypes < Minitest::Test
  def test_tinyint_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, tiny_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 1, :int)
    statement.bind_by_index(1, 42, :tinyint)
    session.execute(statement)

    result = session.query("SELECT tiny_val FROM cassandra_c_test.integer_types WHERE id = 1")
    assert_equal 42, result.to_a.first[0]
  end

  def test_tinyint_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, tiny_val) VALUES (:id, :tiny)", 2)

    statement.bind_by_name("id", 2, :int)
    statement.bind_by_name("tiny", 127, :tinyint)
    session.execute(statement)

    result = session.query("SELECT tiny_val FROM cassandra_c_test.integer_types WHERE id = 2")
    assert_equal 127, result.to_a.first[0]
  end

  def test_tinyint_boundary_values
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, tiny_val) VALUES (?, ?)", 2)

    # Test minimum value
    statement.bind_by_index(0, 3, :int)
    statement.bind_by_index(1, -128, :tinyint)
    session.execute(statement)

    # Test maximum value
    statement.bind_by_index(0, 4, :int)
    statement.bind_by_index(1, 127, :tinyint)
    session.execute(statement)

    # Test minimum value
    result = session.query("SELECT tiny_val FROM cassandra_c_test.integer_types WHERE id = 3")
    assert_equal(-128, result.to_a.first[0])

    # Test maximum value
    result = session.query("SELECT tiny_val FROM cassandra_c_test.integer_types WHERE id = 4")
    assert_equal 127, result.to_a.first[0]
  end

  def test_tinyint_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, tiny_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 5, :int)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT tiny_val FROM cassandra_c_test.integer_types WHERE id = 5")
    assert_nil result.to_a.first[0]
  end
end
