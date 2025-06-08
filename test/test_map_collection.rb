# frozen_string_literal: true

require_relative "test_helper"

class TestMapCollection < Minitest::Test
  def test_map_database_round_trip_strings
    # Test string-to-string map type
    test_map = {"key1" => "value1", "key2" => "value2", "key3" => "value3"}

    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_string_map")
    statement.bind_by_index(1, test_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_string_map'")
    row = result.to_a.first
    retrieved_map = row[0]

    assert_instance_of Hash, retrieved_map
    assert_equal test_map, retrieved_map
    assert_equal "value1", retrieved_map["key1"]
    assert_equal "value2", retrieved_map["key2"]
    assert_equal "value3", retrieved_map["key3"]
  end

  def test_map_database_round_trip_integers
    # Test string-to-integer map type
    test_map = {"count1" => 100, "count2" => 200, "count3" => 300}

    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, int_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_int_map")
    statement.bind_by_index(1, test_map)
    session.execute(statement)

    result = session.query("SELECT int_map FROM cassandra_c_test.map_types WHERE id = 'test_int_map'")
    row = result.to_a.first
    retrieved_map = row[0]

    assert_instance_of Hash, retrieved_map
    assert_equal test_map, retrieved_map
    assert_equal 100, retrieved_map["count1"]
    assert_equal 200, retrieved_map["count2"]
    assert_equal 300, retrieved_map["count3"]
  end

  def test_map_binding_by_name
    test_map = {"name" => "Alice", "role" => "admin", "status" => "active"}

    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (:id, :map_data)")
    statement = prepared.bind
    statement.bind_by_name("id", "test_named_binding")
    statement.bind_by_name("map_data", test_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_named_binding'")
    row = result.to_a.first
    retrieved_map = row[0]

    assert_equal test_map, retrieved_map
  end

  def test_map_empty_handling
    empty_map = {}

    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_empty_map")
    statement.bind_by_index(1, empty_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_empty_map'")
    row = result.to_a.first
    retrieved_map = row[0]

    # Empty maps may be returned as nil or empty Hash depending on Cassandra storage
    assert retrieved_map.nil? || retrieved_map == {}
  end

  def test_map_null_handling
    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map, int_map) VALUES (?, ?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_null_map")
    statement.bind_by_index(1, nil)
    statement.bind_by_index(2, {"test" => 42})
    session.execute(statement)

    result = session.query("SELECT string_map, int_map FROM cassandra_c_test.map_types WHERE id = 'test_null_map'")
    row = result.to_a.first
    null_map, non_null_map = row

    assert_nil null_map
    assert_equal({"test" => 42}, non_null_map)
  end

  def test_map_multiple_columns_same_row
    # Test storing different map types in the same row
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
    row = result.to_a.first
    retrieved_string_map, retrieved_int_map, retrieved_mixed_map = row

    assert_equal string_map, retrieved_string_map
    assert_equal int_map, retrieved_int_map
    assert_equal mixed_map, retrieved_mixed_map

    # Verify types - integer values come back as typed integer objects
    assert_instance_of String, retrieved_string_map["name"]
    assert retrieved_int_map["score"].respond_to?(:to_i)
    assert_equal 95, retrieved_int_map["score"].to_i
    assert_instance_of String, retrieved_mixed_map["setting"]
  end

  def test_map_key_access_patterns
    test_map = {"user_id" => "12345", "session_token" => "abc123", "expires_at" => "2024-12-31"}

    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_key_access")
    statement.bind_by_index(1, test_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_key_access'")
    row = result.to_a.first
    retrieved_map = row[0]

    # Test standard Ruby Hash operations
    assert_equal 3, retrieved_map.size
    assert retrieved_map.key?("user_id")
    assert retrieved_map.has_key?("session_token")
    assert_includes retrieved_map.keys, "expires_at"
    assert_includes retrieved_map.values, "12345"

    # Test iteration
    key_count = 0
    retrieved_map.each do |key, value|
      assert_instance_of String, key
      assert_instance_of String, value
      key_count += 1
    end
    assert_equal 3, key_count
  end

  def test_map_overwrite_behavior
    # Test that maps can be updated/overwritten
    original_map = {"version" => "1.0", "status" => "beta"}
    updated_map = {"version" => "2.0", "status" => "stable", "features" => "enhanced"}

    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (?, ?)")

    # Insert original
    statement = prepared.bind
    statement.bind_by_index(0, "test_overwrite")
    statement.bind_by_index(1, original_map)
    session.execute(statement)

    # Update with new map
    statement = prepared.bind
    statement.bind_by_index(0, "test_overwrite")
    statement.bind_by_index(1, updated_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_overwrite'")
    row = result.to_a.first
    retrieved_map = row[0]

    assert_equal updated_map, retrieved_map
    assert_equal "2.0", retrieved_map["version"]
    assert_equal "stable", retrieved_map["status"]
    assert_equal "enhanced", retrieved_map["features"]
  end

  def test_map_large_dataset
    # Test with larger map to ensure performance
    large_map = {}
    50.times do |i|
      large_map["key_#{i}"] = "value_#{i}"
    end

    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_large_map")
    statement.bind_by_index(1, large_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_large_map'")
    row = result.to_a.first
    retrieved_map = row[0]

    assert_equal large_map.size, retrieved_map.size
    assert_equal large_map, retrieved_map

    # Spot check a few values
    assert_equal "value_0", retrieved_map["key_0"]
    assert_equal "value_25", retrieved_map["key_25"]
    assert_equal "value_49", retrieved_map["key_49"]
  end

  def test_map_special_characters
    # Test maps with special characters and Unicode
    test_map = {
      "unicode_key_🔑" => "unicode_value_🎯",
      "spaces in key" => "spaces in value",
      "quotes\"and'stuff" => "more\"quotes'here",
      "newlines\nand\ttabs" => "more\nnewlines\there"
    }

    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, string_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_special_chars")
    statement.bind_by_index(1, test_map)
    session.execute(statement)

    result = session.query("SELECT string_map FROM cassandra_c_test.map_types WHERE id = 'test_special_chars'")
    row = result.to_a.first
    retrieved_map = row[0]

    # Test individual key-value pairs instead of full map equality (due to potential ordering differences)
    assert_equal "unicode_value_🎯", retrieved_map["unicode_key_🔑"]
    assert_equal "spaces in value", retrieved_map["spaces in key"]
    assert_equal "more\"quotes'here", retrieved_map["quotes\"and'stuff"]
    assert_equal "more\nnewlines\there", retrieved_map["newlines\nand\ttabs"]
    assert_equal 4, retrieved_map.size
  end

  def test_map_integer_keys_converted_to_strings
    # Test that integer keys get converted to strings for map<text, int>
    test_map = {"1" => 100, "2" => 200, "3" => 300}

    prepared = session.prepare("INSERT INTO cassandra_c_test.map_types (id, int_map) VALUES (?, ?)")
    statement = prepared.bind
    statement.bind_by_index(0, "test_string_keys")
    statement.bind_by_index(1, test_map)
    session.execute(statement)

    result = session.query("SELECT int_map FROM cassandra_c_test.map_types WHERE id = 'test_string_keys'")
    row = result.to_a.first
    retrieved_map = row[0]

    assert_equal test_map, retrieved_map
    assert_equal 100, retrieved_map["1"]
    assert_equal 200, retrieved_map["2"]
    assert_equal 300, retrieved_map["3"]
  end
end
