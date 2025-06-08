# frozen_string_literal: true

require "test_helper"
require "set"

class TestSetCollection < Minitest::Test
  def setup
    session.query("TRUNCATE cassandra_c_test.set_types")
  end

  def test_set_database_round_trip_strings
    string_set = Set.new(["hello", "world", "test"])

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "string_test")
    statement.bind_by_index(1, string_set)

    session.execute(statement)

    query_result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'string_test'")
    retrieved_set = query_result.to_a.first[0]

    assert_instance_of Set, retrieved_set
    assert_equal Set.new(["hello", "world", "test"]), retrieved_set
  end

  def test_set_database_round_trip_integers
    int_set = Set.new([1, 2, 3, 42, 100])

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, int_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "int_test")
    statement.bind_by_index(1, int_set)

    session.execute(statement)

    query_result = session.query("SELECT int_set FROM cassandra_c_test.set_types WHERE id = 'int_test'")
    retrieved_set = query_result.to_a.first[0]

    assert_instance_of Set, retrieved_set
    plain_int_set = Set.new(retrieved_set.map(&:to_i))
    assert_equal Set.new([1, 2, 3, 42, 100]), plain_int_set
  end

  def test_set_database_empty_set
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "empty_test")
    statement.bind_by_index(1, Set.new)

    session.execute(statement)

    query_result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'empty_test'")
    retrieved_set = query_result.to_a.first[0]

    assert(retrieved_set.nil? || (retrieved_set.instance_of?(Set) && retrieved_set.empty?))
  end

  def test_set_binding_by_name
    string_set = Set.new(["param", "binding", "test"])

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (:id, :string_set)", 2)
    statement.bind_by_name("id", "name_test")
    statement.bind_by_name("string_set", string_set)

    session.execute(statement)

    query_result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'name_test'")
    retrieved_set = query_result.to_a.first[0]

    assert_equal Set.new(["param", "binding", "test"]), retrieved_set
  end

  def test_set_null_handling
    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "null_test")
    statement.bind_by_index(1, nil)

    session.execute(statement)

    query_result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'null_test'")
    retrieved_set = query_result.to_a.first[0]

    assert_nil retrieved_set
  end

  def test_set_uniqueness
    set_with_duplicates = Set.new(["hello", "world", "hello", "test", "world"])

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "unique_test")
    statement.bind_by_index(1, set_with_duplicates)

    session.execute(statement)

    query_result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'unique_test'")
    retrieved_set = query_result.to_a.first[0]

    assert_equal Set.new(["hello", "world", "test"]), retrieved_set
    assert_equal 3, retrieved_set.size
  end

  def test_array_to_set_conversion
    array_data = ["hello", "world", "test", "hello"]

    statement = CassandraC::Native::Statement.new("INSERT INTO cassandra_c_test.set_types (id, string_set) VALUES (?, ?)", 2)
    statement.bind_by_index(0, "array_test")
    statement.bind_by_index(1, array_data)

    session.execute(statement)

    query_result = session.query("SELECT string_set FROM cassandra_c_test.set_types WHERE id = 'array_test'")
    retrieved_set = query_result.to_a.first[0]

    assert_instance_of Set, retrieved_set
    assert_equal Set.new(["hello", "world", "test"]), retrieved_set
  end
end
