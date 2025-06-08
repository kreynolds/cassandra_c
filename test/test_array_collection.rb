# frozen_string_literal: true

require "test_helper"

class TestArrayCollection < Minitest::Test
  def setup
    # Clean up test table before each test
    session.query("TRUNCATE cassandra_c_test.list_types")
  end

  def test_array_database_round_trip_strings
    # Insert a list of strings
    string_array = ["hello", "world", "test"]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, string_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "string_test")
    statement.bind_by_index(1, string_array)

    result = session.execute(statement)
    assert_instance_of CassandraC::Native::Result, result

    # Retrieve and verify
    query_result = session.query("SELECT string_list FROM cassandra_c_test.list_types WHERE id = 'string_test'")
    row = query_result.to_a.first
    retrieved_array = row[0]

    assert_instance_of Array, retrieved_array
    assert_equal ["hello", "world", "test"], retrieved_array
  end

  def test_array_database_round_trip_integers
    # Insert a list of integers
    int_array = [1, 2, 3, 42, 100]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, int_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "int_test")
    statement.bind_by_index(1, int_array)

    result = session.execute(statement)
    assert_instance_of CassandraC::Native::Result, result

    # Retrieve and verify
    query_result = session.query("SELECT int_list FROM cassandra_c_test.list_types WHERE id = 'int_test'")
    row = query_result.to_a.first
    retrieved_array = row[0]

    assert_instance_of Array, retrieved_array
    assert_equal [1, 2, 3, 42, 100], retrieved_array
  end

  def test_array_database_round_trip_strings_as_mixed
    # Insert a list with values converted to strings (for mixed_list column)
    # Note: Cassandra collections are strongly typed, so we convert everything to strings
    mixed_array = ["string", "42", "3.14", "true", "false"]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, mixed_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "mixed_test")
    statement.bind_by_index(1, mixed_array)

    result = session.execute(statement)
    assert_instance_of CassandraC::Native::Result, result

    # Retrieve and verify
    query_result = session.query("SELECT mixed_list FROM cassandra_c_test.list_types WHERE id = 'mixed_test'")
    row = query_result.to_a.first
    retrieved_array = row[0]

    assert_instance_of Array, retrieved_array
    assert_equal ["string", "42", "3.14", "true", "false"], retrieved_array
  end

  def test_array_database_empty_array
    # Insert an empty array
    empty_array = []

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, string_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "empty_test")
    statement.bind_by_index(1, empty_array)

    result = session.execute(statement)
    assert_instance_of CassandraC::Native::Result, result

    # Retrieve and verify - Cassandra may store empty collections as NULL
    query_result = session.query("SELECT string_list FROM cassandra_c_test.list_types WHERE id = 'empty_test'")
    row = query_result.to_a.first
    retrieved_array = row[0]

    # Empty arrays might be stored as NULL in Cassandra, which is acceptable
    if retrieved_array.nil?
      # This is acceptable behavior - empty collections can be NULL in Cassandra
      assert_nil retrieved_array
    else
      # If it's not NULL, it should be an empty Array
      assert_instance_of Array, retrieved_array
      assert_equal [], retrieved_array
      assert retrieved_array.empty?
    end
  end

  def test_array_binding_by_name
    # Test binding by parameter name
    string_array = ["param", "binding", "test"]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, string_list) VALUES (:id, :list)", 2)
    statement.bind_by_name("id", "name_test")
    statement.bind_by_name("list", string_array)

    result = session.execute(statement)
    assert_instance_of CassandraC::Native::Result, result

    # Retrieve and verify
    query_result = session.query("SELECT string_list FROM cassandra_c_test.list_types WHERE id = 'name_test'")
    row = query_result.to_a.first
    retrieved_array = row[0]

    assert_instance_of Array, retrieved_array
    assert_equal ["param", "binding", "test"], retrieved_array
  end

  def test_array_null_handling
    # Test null array insertion
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.list_types (id, string_list) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "null_test")
    statement.bind_by_index(1, nil)

    result = session.execute(statement)
    assert_instance_of CassandraC::Native::Result, result

    # Retrieve and verify
    query_result = session.query("SELECT string_list FROM cassandra_c_test.list_types WHERE id = 'null_test'")
    row = query_result.to_a.first
    retrieved_array = row[0]

    assert_nil retrieved_array
  end
end
