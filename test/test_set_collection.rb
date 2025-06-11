# frozen_string_literal: true

require "test_helper"
require "set"

class TestSetCollection < Minitest::Test
  def setup
    session.query("TRUNCATE cassandra_c_test.set_types")
  end

  def test_set_database_round_trip
    # Test string sets
    string_set = Set.new(["hello", "world", "test"])
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "string_test")
    statement.bind_by_index(1, string_set)
    session.execute(statement)

    result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'string_test'")
    assert_equal Set.new(["hello", "world", "test"]), result.to_a.first[0]

    # Test integer sets
    int_set = Set.new([1, 2, 3, 42, 100])
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, int_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "int_test")
    statement.bind_by_index(1, int_set)
    session.execute(statement)

    result = session.query("SELECT int_set FROM cassandra_c_test.set_types WHERE id = 'int_test'")
    retrieved_set = result.to_a.first[0]
    assert_equal Set.new([1, 2, 3, 42, 100]), Set.new(retrieved_set.map(&:to_i))
  end

  def test_set_edge_cases_and_features
    # Test empty set (may be stored as NULL)
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "empty_test")
    statement.bind_by_index(1, Set.new)
    session.execute(statement)

    result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'empty_test'")
    retrieved = result.to_a.first[0]
    assert(retrieved.nil? || (retrieved.instance_of?(Set) && retrieved.empty?))

    # Test binding by name
    string_set = Set.new(["param", "binding", "test"])
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (:id, :string_set)", 2)
    statement.bind_by_name("id", "name_test")
    statement.bind_by_name("string_set", string_set)
    session.execute(statement)

    result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'name_test'")
    assert_equal Set.new(["param", "binding", "test"]), result.to_a.first[0]

    # Test null handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "null_test")
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'null_test'")
    assert_nil result.to_a.first[0]

    # Test uniqueness - sets automatically handle duplicates
    set_with_duplicates = Set.new(["hello", "world", "hello", "test", "world"])
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "unique_test")
    statement.bind_by_index(1, set_with_duplicates)
    session.execute(statement)

    result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'unique_test'")
    retrieved_set = result.to_a.first[0]
    assert_equal Set.new(["hello", "world", "test"]), retrieved_set
    assert_equal 3, retrieved_set.size

    # Test array to set conversion
    array_data = ["hello", "world", "test", "hello"]
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "array_test")
    statement.bind_by_index(1, array_data)
    session.execute(statement)

    result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'array_test'")
    assert_equal Set.new(["hello", "world", "test"]), result.to_a.first[0]
  end
end
