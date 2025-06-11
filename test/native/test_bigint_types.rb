# frozen_string_literal: true

require "test_helper"

class TestBigintTypes < Minitest::Test
  def test_bigint_binding_with_type_hints
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, big_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 30, :int)
    statement.bind_by_index(1, 9223372036854775807, :bigint)
    session.execute(statement)

    result = session.query("SELECT big_val FROM cassandra_c_test.integer_types WHERE id = 30")
    assert_equal 9223372036854775807, result.to_a.first[0]
  end

  def test_bigint_binding_by_name
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, big_val) VALUES (:id, :big)", 2)

    statement.bind_by_name("id", 31, :int)
    statement.bind_by_name("big", -9223372036854775808, :bigint)
    session.execute(statement)

    result = session.query("SELECT big_val FROM cassandra_c_test.integer_types WHERE id = 31")
    assert_equal(-9223372036854775808, result.to_a.first[0])
  end

  def test_bigint_boundary_values
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, big_val) VALUES (?, ?)", 2)

    # Test minimum value
    statement.bind_by_index(0, 32, :int)
    statement.bind_by_index(1, -9223372036854775808, :bigint)
    session.execute(statement)

    # Test maximum value
    statement.bind_by_index(0, 33, :int)
    statement.bind_by_index(1, 9223372036854775807, :bigint)
    session.execute(statement)

    # Test minimum value
    result = session.query("SELECT big_val FROM cassandra_c_test.integer_types WHERE id = 32")
    assert_equal(-9223372036854775808, result.to_a.first[0])

    # Test maximum value
    result = session.query("SELECT big_val FROM cassandra_c_test.integer_types WHERE id = 33")
    assert_equal 9223372036854775807, result.to_a.first[0]
  end

  def test_bigint_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, big_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 34, :int)
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT big_val FROM cassandra_c_test.integer_types WHERE id = 34")
    assert_nil result.to_a.first[0]
  end

  def test_bigint_automatic_inference
    # Test that integers default to bigint with automatic inference
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.integer_types (id, big_val) VALUES (?, ?)", 2)

    statement.bind_by_index(0, 35, :int)    # Explicit hint for ID
    statement.bind_by_index(1, 42)          # Should automatically infer as bigint
    session.execute(statement)

    result = session.query("SELECT big_val FROM cassandra_c_test.integer_types WHERE id = 35")
    assert_equal 42, result.to_a.first[0]
  end
end
