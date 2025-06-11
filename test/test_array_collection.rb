# frozen_string_literal: true

require "test_helper"

class TestArrayCollection < Minitest::Test
  def setup
    # Clean up test table before each test
    session.query("TRUNCATE cassandra_c_test.list_types")
  end

  def test_array_database_round_trip
    # Test string arrays
    string_array = ["hello", "world", "test"]
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, string_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "string_test")
    statement.bind_by_index(1, string_array)
    session.execute(statement)

    result = session.query("SELECT string_list FROM cassandra_c_test.list_types WHERE id = 'string_test'")
    assert_equal ["hello", "world", "test"], result.to_a.first[0]

    # Test integer arrays
    int_array = [1, 2, 3, 42, 100]
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, int_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "int_test")
    statement.bind_by_index(1, int_array)
    session.execute(statement)

    result = session.query("SELECT int_list FROM cassandra_c_test.list_types WHERE id = 'int_test'")
    assert_equal [1, 2, 3, 42, 100], result.to_a.first[0]

    # Test mixed string arrays
    mixed_array = ["string", "42", "3.14", "true", "false"]
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, mixed_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "mixed_test")
    statement.bind_by_index(1, mixed_array)
    session.execute(statement)

    result = session.query("SELECT mixed_list FROM cassandra_c_test.list_types WHERE id = 'mixed_test'")
    assert_equal ["string", "42", "3.14", "true", "false"], result.to_a.first[0]
  end

  def test_array_edge_cases
    # Test empty array (may be stored as NULL)
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, string_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "empty_test")
    statement.bind_by_index(1, [])
    session.execute(statement)

    result = session.query("SELECT string_list FROM cassandra_c_test.list_types WHERE id = 'empty_test'")
    retrieved = result.to_a.first[0]
    assert(retrieved.nil? || retrieved == [])

    # Test binding by name
    string_array = ["param", "binding", "test"]
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, string_list) VALUES (:id, :list)", 2)
    statement.bind_by_name("id", "name_test")
    statement.bind_by_name("list", string_array)
    session.execute(statement)

    result = session.query("SELECT string_list FROM cassandra_c_test.list_types WHERE id = 'name_test'")
    assert_equal ["param", "binding", "test"], result.to_a.first[0]

    # Test null handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, string_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "null_test")
    statement.bind_by_index(1, nil)
    session.execute(statement)

    result = session.query("SELECT string_list FROM cassandra_c_test.list_types WHERE id = 'null_test'")
    assert_nil result.to_a.first[0]
  end
end
