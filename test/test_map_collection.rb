# frozen_string_literal: true

require_relative "test_helper"

class TestMapCollection < Minitest::Test
  def test_map_database_round_trip
    # Test string-to-string map
    string_map = {"key1" => "value1", "key2" => "value2", "key3" => "value3"}
    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_string_map")
    statement.bind_by_index(1, string_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_string_map'")
    assert_equal string_map, result.to_a.first[0]

    # Test string-to-integer map
    int_map = {"count1" => 100, "count2" => 200, "count3" => 300}
    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, int_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_int_map")
    statement.bind_by_index(1, int_map)
    session.execute(statement)

    result = session.query("SELECT int_map FROM cassandra_c_test.map_types WHERE id = 'test_int_map'")
    assert_equal int_map, result.to_a.first[0]
  end

  def test_map_edge_cases
    # Test binding by name
    test_map = {"name" => "Alice", "role" => "admin", "status" => "active"}
    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (:id, :map_data)")
    statement = prepared.bind
    statement.bind_by_name("id", "test_named_binding")
    statement.bind_by_name("map_data", test_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_named_binding'")
    assert_equal test_map, result.to_a.first[0]

    # Test empty map (may be stored as NULL)
    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_empty_map")
    statement.bind_by_index(1, {})
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_empty_map'")
    retrieved = result.to_a.first[0]
    assert(retrieved.nil? || retrieved == {})

    # Test null handling
    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map, int_map) VALUES (?, ?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_null_map")
    statement.bind_by_index(1, nil)
    statement.bind_by_index(2, {"test" => 42})
    session.execute(statement)

    result = session.query("SELECT string_map, int_map FROM cassandra_c_test.map_types WHERE id = 'test_null_map'")
    null_map, non_null_map = result.to_a.first
    assert_nil null_map
    assert_equal({"test" => 42}, non_null_map)
  end

  def test_map_comprehensive_features
    # Test multiple map types in same row
    string_map = {"name" => "Alice", "role" => "admin"}
    int_map = {"score" => 95, "level" => 5}
    mixed_map = {"setting" => "enabled", "config" => "production"}

    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map, int_map, mixed_map) VALUES (?, ?, ?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_multiple_maps")
    statement.bind_by_index(1, string_map)
    statement.bind_by_index(2, int_map)
    statement.bind_by_index(3, mixed_map)
    session.execute(statement)

    result = session.query("SELECT string_map, int_map, mixed_map FROM cassandra_c_test.map_types WHERE id = 'test_multiple_maps'")
    retrieved_string_map, retrieved_int_map, retrieved_mixed_map = result.to_a.first
    assert_equal string_map, retrieved_string_map
    assert_equal int_map, retrieved_int_map
    assert_equal mixed_map, retrieved_mixed_map

    # Test Hash operations and overwrite behavior
    test_map = {"user_id" => "12345", "session_token" => "abc123"}
    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_operations")
    statement.bind_by_index(1, test_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_operations'")
    retrieved_map = result.to_a.first[0]
    assert_equal 2, retrieved_map.size
    assert retrieved_map.key?("user_id")
    assert_equal "12345", retrieved_map["user_id"]

    # Test with special characters and Unicode
    special_map = {
      "unicode_🔑" => "value_🎯",
      "spaces in key" => "spaces in value"
    }
    statement = prepared.bind
    statement.bind_by_index(0, "test_special")
    statement.bind_by_index(1, special_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_special'")
    retrieved = result.to_a.first[0]
    assert_equal "value_🎯", retrieved["unicode_🔑"]
    assert_equal "spaces in value", retrieved["spaces in key"]
  end
end
